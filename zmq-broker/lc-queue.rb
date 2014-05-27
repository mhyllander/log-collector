#!/usr/bin/env ruby

require 'rubygems'
require 'ffi-rzmq'
require 'orderedhash'
require 'optparse'
require 'logger'

PPP_READY = "\x01" # Signals worker is ready
PPP_PING  = "\x02" # Signals queue ping
PPP_PONG  = "\x03" # Signals worker pong

$logger = Logger.new(STDERR)
$logger.level = ($DEBUG and Logger::DEBUG or Logger::WARN)
$logger.debug("Debugging lc-queue...")

class Worker
  attr_reader :identity
  attr_reader :expiry
  attr_reader :ping_at

  def initialize(identity)
    @identity = identity
    @ping_at = Time.now + $options[:ping_interval]
    @expiry = Time.now + $options[:ping_interval] * $options[:ping_liveness]
  end
end

class WorkerQueue
  attr_reader :queue

  def initialize
    @queue = OrderedHash.new
  end

  def ready(worker)
    @queue.delete(worker.identity)
    @queue[worker.identity] = worker
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
    identity, _ = @queue.pop
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
  start_at = 0.0

  loop do
    poller = workers.available > 0 ? poll_both : poll_workers
    while poller.poll($options[:ping_interval]*1000) > 0
      poller.readables.each do |readable|

        if readable === backend

          backend.recv_strings msgs = []
          if msgs.length>0
            workerid = msgs[0]
            $logger.debug "got msg worker=#{workerid} len=#{msgs.length} msgs=#{msgs}"

            # Add this worker to the list of available workers
            workers.ready(Worker.new(workerid))

            if msgs.length==2
              $logger.error "Error: Invalid message from worker: #{msgs}" unless [PPP_READY, PPP_PONG].include?(msgs[1])
              $logger.debug "recv worker pong" if msgs[1]==PPP_PONG
              $logger.debug "recv worker ready" if msgs[1]==PPP_READY
            elsif msgs.length>=4
              # send reply back to client
              # ['', clientid, '', reply]
              frontend.send_strings msgs[2..-1]
              $logger.info "logstash handling time: #{Time.now.to_f - start_at}"
            else
              $logger.error "Error: Invalid message from worker: #{msgs}"
            end
          end

        elsif readable === frontend

          # Read the request from the client and forward it to the LRU worker
          frontend.recv_strings msgs = []
          $logger.debug "got msg client=#{msgs[0]} len=#{msgs.length} msgs=#{msgs[0..-2]}"
          if msgs.length>=2 && workers.available>0
            start_at = Time.now.to_f
            backend.send_strings [workers.next, ''] + msgs
          end

        end

      end # poller.readables.each
    end # while poller.poll

    # Send pings to idle workers if it's time
    workers.queue.each do |identity,worker|
      if Time.now > worker.ping_at
        $logger.debug "send ping to #{identity}"
        backend.send_strings [identity, '', PPP_PING]
      end
    end

    workers.purge
  end

  frontend.close
  backend.close
  context.terminate
end

$options = {
  frontend: 'tcp://*:5559',
  backend: 'tcp://*:5560',
  ping_interval: 1,
  ping_liveness: 3
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
  opts.on("-l", "--pingliveness NUMBER", Integer, "The ping liveness (number of unanswered pings before failing) (default=#{$options[:ping_liveness]}).") do |v|
    $options[:ping_liveness] = v
  end

  opts.on_tail("-h", "--help", "Show this message") do
    puts opts
    exit
  end

end
parser.parse!

run