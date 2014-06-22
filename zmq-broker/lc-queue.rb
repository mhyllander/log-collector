#!/usr/bin/env ruby

require 'rubygems'
require 'ffi-rzmq'
require 'orderedhash'
require 'optparse'
require_relative '../log-collector/lc/logger'

PPP_READY = "\x01" # Signals worker is ready
PPP_PING  = "\x02" # Signals queue ping
PPP_PONG  = "\x03" # Signals worker pong

KEEP_IN_PROCESSING = 180 # seconds to keep requests in processing, purge after this long

class Worker
  attr_reader :identity
  attr_reader :expiry
  attr_reader :ping_at

  def initialize(identity)
    @identity = identity
    renew_expiry
  end

  def renew_expiry
    @ping_at = Time.now + $options[:ping_interval]
    @expiry = Time.now + $options[:ping_interval] * $options[:ping_liveness]
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
      @queue[workerid] = Worker.new(workerid)
      $logger.debug "add to queue worker=#{workerid}"
    end
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

  def next
    identity, _ = @queue.shift
    identity
  end

  def available
    @queue.length
  end
end

def run
  context = ZMQ::Context.new
  frontend = context.socket ZMQ::ROUTER
  backend = context.socket ZMQ::ROUTER

  $logger.info "frontend bind to #{$options[:frontend]}"
  frontend.bind $options[:frontend]
  $logger.info "backend bind to #{$options[:backend]}"
  backend.bind $options[:backend]

  poll_workers = ZMQ::Poller.new
  poll_workers.register_readable backend

  poll_both = ZMQ::Poller.new
  poll_both.register_readable backend
  poll_both.register_readable frontend

  workers = WorkerQueue.new
  requests = OrderedHash.new
  processing = Hash.new

  loop do
    poller = workers.available > 0 ? poll_both : poll_workers
    while poller.poll($options[:ping_interval]*1000) > 0
      poller.readables.each do |readable|

        if readable === backend

          backend.recv_strings msgs = []
          if msgs.length>0
            workerid = msgs[0]
            $logger.debug { "got msg worker=#{workerid} len=#{msgs.length} msgs=#{msgs}" }

            # Add this worker to the list of available workers
            workers.ready(workerid)

            if msgs.length==2
              # [workerid, msg]
              $logger.error "Error: Invalid message from worker: #{msgs}" unless [PPP_READY, PPP_PONG].include?(msgs[1])
              $logger.debug "recv worker pong" if msgs[1]==PPP_PONG
              $logger.debug "recv worker ready" if msgs[1]==PPP_READY
            elsif msgs.length==5
              # [workerid, clientid, '', serial, reply]
              # send reply back to client
              $logger.info "<-- response worker=#{workerid} to client=#{msgs[1]}, serial=#{msgs[3]}"
              frontend.send_strings msgs[1..-1]
            else
              $logger.error "Error: Invalid message from worker: #{msgs}"
            end
          end

        elsif readable === frontend

          # Read the request from the client and forward it to the LRU worker
          frontend.recv_strings msgs = []
          if msgs.length==4
            # [ clientid, '', serial, request ]
            $logger.debug { "got msg client=#{msgs[0]} serial=#{msgs[2]} len=#{msgs.length}" }
            key = "#{msgs[0]}/#{msgs[2]}"
            unless processing.include? key
              $logger.debug { "enqueue request #{key}" }
              requests[key] = msgs
            else
              $logger.debug { "ignore request #{key}, already processing" }
            end
          else
            $logger.error "Error: Invalid message from client: #{msgs}"
          end

        end

        # send enqueued request if worker available
        if requests.size>0 && workers.available>0
          workerid = workers.next
          key, msgs = requests.shift
          $logger.info "--> request client=#{msgs[0]} to worker=#{workerid}, serial=#{msgs[2]}"
          backend.send_strings [workerid] + msgs
          processing[key] = Time.now + KEEP_IN_PROCESSING
        end

      end # poller.readables.each
    end # while poller.poll

    # Send pings to idle workers if it's time
    workers.queue.each do |workerid,worker|
      if Time.now > worker.ping_at
        $logger.debug "send ping to worker=#{workerid}"
        backend.send_strings [workerid, PPP_PING]
      end
    end

    workers.purge

    # removed expired requests from processing
    now = Time.now
    expire = []
    processing.each {|k,t| expire << k if t < now}
    expire.each {|k| processing.delete k}
  end

  frontend.close
  backend.close
  context.terminate
end

$options = {
  frontend: 'tcp://*:5559',
  backend: 'tcp://*:5560',
  ping_interval: 1,
  ping_liveness: 3,
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

  opts.on_tail("-h", "--help", "Show this message") do
    puts opts
    exit
  end

end
parser.parse!

$logger = LogCollector::Logger.new $options[:syslog], parser.program_name, ($DEBUG ? 'DEBUG' : $options[:loglevel])
$logger.debug("Debugging #{$logger.id}...")

run
