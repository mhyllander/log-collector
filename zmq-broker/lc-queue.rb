#!/usr/bin/env ruby

require 'rubygems'
require 'ffi-rzmq'
require 'orderedhash'
require 'optparse'
require_relative '../log-collector/lc/logger'

PPP_READY = "\x01" # Signals worker is ready
PPP_PING  = "\x02" # Signals queue ping
PPP_PONG  = "\x03" # Signals worker pong

$context = nil
$frontend = nil
$backend = nil

$workers = nil
$requests = nil
$processing = nil
$responses = nil

class Worker
  attr_reader :identity, :expiry, :ping_at, :request_queue

  def initialize(identity)
    @identity = identity
    @request_queue = []
    renew_expiry
  end

  def renew_expiry
    now = Time.now
    @ping_at = now + $options[:ping_interval]
    @expiry = now + $options[:ping_interval] * $options[:ping_liveness]
  end
end

class WorkerQueue
  attr_reader :queue

  def initialize
    @queue = OrderedHash.new
  end

  def ready(workerid)
    if worker = @queue[workerid]
      worker.renew_expiry
      $logger.debug "renew expiry worker=#{workerid}"
    else
      worker = @queue[workerid] = Worker.new(workerid)
      $logger.debug "add to queue worker=#{workerid}"
    end
    worker
  end

  # look for and kill expired workers
  def purge
    t = Time.now
    expired = []
    @queue.each do |identity,worker|
      expired << identity if t > worker.expiry
    end
    expired.each do |identity|
      @queue.delete identity
    end
  end

  # remove the request from any worker queue(s)
  def purge_request(key)
    @queue.each do |identity,worker|
      worker.request_queue.delete key
    end
  end

  def next
    # find first available worker
    identity, worker = self.available_worker
    return nil if identity.nil?
    # move worker last in queue
    @queue.delete identity
    @queue[identity] = worker
    worker.renew_expiry
    worker
  end

  def available_worker
    @queue.detect {|identity,worker| worker.request_queue.length < $options[:enqueue]}
  end
end

def expire_processed_requests
  now = Time.now
  # removed expired requests from processing
  expire = []
  $processing.each {|k,t| expire << k if t < now}
  expire.each do |k|
    $processing.delete k
    $workers.purge_request k
  end
  # removed expired requests from responses
  expire = []
  $responses.each {|k,r| expire << k if r[1] < now}
  expire.each {|k| $responses.delete k}
end

def ping_workers
  # Send pings to idle workers if it's time
  $workers.queue.each do |workerid,worker|
    if Time.now > worker.ping_at
      $logger.debug "send ping to worker=#{workerid}"
      $backend.send_strings [workerid, PPP_PING]
    end
  end
end

def run
  $context = ZMQ::Context.new
  $frontend = $context.socket ZMQ::ROUTER
  $frontend.setsockopt ZMQ::LINGER, 60000 # wait for messages to clients to be delivered
  $backend = $context.socket ZMQ::ROUTER
  $backend.setsockopt ZMQ::LINGER, 0   # discard messages to workers

  $logger.info "frontend bind to #{$options[:frontend]}"
  $frontend.bind $options[:frontend]
  $logger.info "backend bind to #{$options[:backend]}"
  $backend.bind $options[:backend]

  poller = ZMQ::Poller.new
  poller.register_readable $backend
  poller.register_readable $frontend

  $workers = WorkerQueue.new
  $requests = OrderedHash.new
  $processing = {}
  $responses = {}

  loop do
    if (rc = poller.poll($options[:ping_interval]*1000)) > 0
      poller.readables.each do |readable|

        if readable === $backend
          $logger.debug { "backend recv" }

          while (rcb = $backend.recv_strings(msgs = [], ZMQ::DONTWAIT)) != -1
            #$logger.debug { "backend recv_strings rc=#{rcb}" }
            if msgs.length>0
              workerid = msgs[0]
              $logger.debug { "recv worker=#{workerid} len=#{msgs.length} msgs=#{msgs}" }

              # Add this worker to the list of available workers
              worker = $workers.ready(workerid)

              if msgs.length==2
                # [workerid, msg]
                $logger.error "Error: Invalid message from worker: #{msgs}" unless [PPP_READY, PPP_PONG].include?(msgs[1])
                $logger.debug "recv worker pong" if msgs[1]==PPP_PONG
                $logger.debug "recv worker ready" if msgs[1]==PPP_READY
              elsif msgs.length==5
                # [workerid, clientid, '', serial, response]
                # send reply back to client
                $logger.info "<-- response worker=#{workerid} to client=#{msgs[1]}, serial=#{msgs[3]}"
                $frontend.send_strings msgs[1..-1]
                # cache the response for a while in case the client re-sends the request
                key = "#{msgs[1]}/#{msgs[3]}"
                $responses[key] = [ msgs, Time.now + $options[:response_time] ]
                # remove request from worker queue
                worker.request_queue.delete key
                $logger.debug { "worker #{worker.identity}, queue=#{worker.request_queue}" }
              else
                $logger.error "Error: Invalid message from worker: #{msgs}"
              end
            end
          end
          #$logger.debug { "backend recv_strings rc=#{rcb}" }

        elsif readable === $frontend
          $logger.debug { "frontend recv" }

          # Read the request from the client and forward it to the LRU worker
          while (rcf = $frontend.recv_strings(msgs = [], ZMQ::DONTWAIT)) != -1
            #$logger.debug { "frontend recv_strings rc=#{rcf}" }
            if msgs.length==4
              # [ clientid, '', serial, request ]
              $logger.debug { "recv msg client=#{msgs[0]} serial=#{msgs[2]} len=#{msgs.length}" }
              key = "#{msgs[0]}/#{msgs[2]}"
              if r = $responses[key]
                # This request has already been processed by a worker. Apparently the client has not
                # seen the result, since it is re-sending the request. Now we can simply resend the
                # reply to the client.
                resp = r[0]
                $logger.info "<-- re-send cached response worker=#{resp[0]} to client=#{resp[1]}, serial=#{resp[3]}"
                $frontend.send_strings resp[1..-1]
              elsif $processing.include? key
                # This request has already been sent to a worker for processing. Just continue waiting for the worker's reponse.
                $logger.info { "already processing request #{key}" }
              else
                # Enqueue the request for processing. The request might already be in the queue, if so
                # it will just retain its position.
                $logger.debug { "enqueue request #{key}" }
                $requests[key] = msgs
              end
            else
              $logger.error "Error: Invalid message from client: #{msgs}"
            end
          end
          #$logger.debug { "frontend recv_strings rc=#{rcf}" }

        end # if readable ===

      end # poller.readables.each
    end # if poller.poll

    # We get here when no incoming messages were found

    $logger.debug { "poller rc=#{rc}" }
    break if rc == -1

    # Send pings to idle workers if it's time
    ping_workers

    # Purge old workers from the list
    $workers.purge

    # Expire old requests from processing and responses
    expire_processed_requests

    # send enqueued request if worker available
    while $requests.size>0 && $workers.available_worker
      worker = $workers.next
      key, msgs = $requests.shift
      $logger.info "--> request client=#{msgs[0]} to worker=#{worker.identity}, serial=#{msgs[2]}"
      $backend.send_strings [worker.identity] + msgs
      $processing[key] = Time.now + $options[:processing_time]
      # save request in worker queue
      worker.request_queue << key
      $logger.debug { "worker #{worker.identity}, queue=#{worker.request_queue}" }
    end

  end # loop

  $logger.info "terminating"
  $backend.close
  $frontend.close
  $context.terminate
end

$options = {
  frontend: 'tcp://*:5559',
  backend: 'tcp://*:5560',
  processing_time: 120,
  response_time: 240,
  ping_interval: 5,
  ping_liveness: 3,
  enqueue: 1,
  syslog: false,
  loglevel: 'WARN'
}

parser = OptionParser.new do |opts|
  opts.banner = "Usage: #{opts.program_name} [options]"

  opts.on("-f", "--frontend ZMQADDR", "The frontend address to bind to (default=#{$options[:frontend]}).") do |v|
    $options[:frontend] = v
  end
  opts.on("-b", "--backend ZMQADDR", "The backend address to bind to (default=#{$options[:backend]}).") do |v|
    $options[:backend] = v
  end
  opts.on("-p", "--processing_purge NUMBER", Integer, "The processing time in seconds (default=#{$options[:processing_time]}).") do |v|
    $options[:processing_time] = v
  end
  opts.on("-r", "--response_purge NUMBER", Integer, "The response cache time in seconds (default=#{$options[:response_time]}).") do |v|
    $options[:response_time] = v
  end
  opts.on("-i", "--pinginterval NUMBER", Integer, "The ping interval in seconds (default=#{$options[:ping_interval]}).") do |v|
    $options[:ping_interval] = v
  end
  opts.on("-v", "--pingliveness NUMBER", Integer, "The ping liveness (number of unanswered pings before failing) (default=#{$options[:ping_liveness]}).") do |v|
    $options[:ping_liveness] = v
  end
  opts.on("-l", "--[no-]syslog", "Log to syslog (default=#{$options[:syslog]}).") do |v|
    $options[:syslog] = v
  end
  opts.on("-L", "--loglevel LOGLEVEL", "Log level (default=#{$options[:loglevel]}).") do |v|
    $options[:loglevel] = v.upcase
  end
  opts.on("-e", "--enqueue LENGTH", Integer, "Per worker queue length (default=#{$options[:enqueue]}).") do |v|
    $options[:enqueue] = v
  end

  opts.on_tail("-h", "--help", "Show this message") do
    puts opts
    exit
  end

end
parser.parse!

$logger = LogCollector::Logger.new $options[:syslog], parser.program_name, ($DEBUG ? 'DEBUG' : $options[:loglevel])
$logger.debug("Debugging #{$logger.id}...")

run
