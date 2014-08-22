#!/usr/bin/env ruby

require 'rubygems'
require 'ffi-rzmq'
require 'optparse'
require 'zlib'
require 'json'
require_relative '../log-collector/lc/logger'

PPP_READY = "\x01" # Signals worker is ready
PPP_PING  = "\x02" # Signals queue ping
PPP_PONG  = "\x03" # Signals worker pong

INTERVAL_INIT = 1
INTERVAL_MAX  = 32

# Helper function that returns a new configured socket
# connected to the Paranoid Pirate queue
def worker_socket(context, identity, poller)
  worker = context.socket ZMQ::DEALER
  worker.setsockopt ZMQ::IDENTITY, identity
  worker.setsockopt ZMQ::LINGER, 0
  poller.register_readable worker
  $logger.info "worker connect to #{$options[:queue]}"
  worker.connect $options[:queue]
  worker.send_string PPP_READY
  worker
end

def run
  context = ZMQ::Context.new
  poller = ZMQ::Poller.new

  liveness = $options[:ping_liveness]
  interval = INTERVAL_INIT

  workerid = $options[:identity] || ("W:%04X-%04X" % [(rand()*0x10000).to_i, (rand()*0x10000).to_i])
  worker = worker_socket context, workerid, poller
  last_sent = Time.now

  loop do

    while (rc = poller.poll($options[:ping_interval]*1000)) > 0
      poller.readables.each do |readable|
        if readable==worker

          # Get message
          # - 3-part envelope + content -> request
          # - 1-part PING -> ping
          worker.recv_strings msgs = []
          if msgs.length==1
            $logger.debug "recv msg len=#{msgs.length} msgs=#{msgs}"
            if msgs[0]==PPP_PING && (Time.now-last_sent) > $options[:ping_interval]
              $logger.debug "recv queue ping, send pong"
              worker.send_string PPP_PONG
              last_sent = Time.now
            end
          elsif msgs.length==4
            # msgs[0]: client id
            # msgs[1]: empty delimiter
            # msgs[2]: serial
            # msgs[3]: request
            clientid = msgs[0]
            serial = msgs[2]
            request = msgs[3]
            $logger.debug "recv msg client=#{clientid} serial=#{serial} len=#{msgs.length}"
            sleep 2*rand() # simulate doing dome work
            json = Zlib::Inflate.inflate(request)
            data = JSON.parse(json)
            $logger.debug "send ACK serial=#{serial} n=#{data['n']}"
            worker.send_strings [clientid, '', serial, ['ACK',serial,data['n']].to_json]
            last_sent = Time.now
          else
            $logger.error "Invalid message: #{msgs}"
          end

          liveness = $options[:ping_liveness]
          interval = INTERVAL_INIT

        end # if readable==worker

      end # poller.readables.each
    end # while poller.poll

    $logger.debug { "poller rc=#{rc}" }
    break if rc == -1

    liveness -= 1
    if liveness==0
      $logger.debug "Queue failure (no pings or requests)"
      $logger.debug "Reconnecting in #{interval}s"

      poller.deregister_readable worker
      worker.close

      sleep interval
      interval *= 2 if interval < INTERVAL_MAX

      worker = worker_socket context, workerid, poller
      last_sent = Time.now
      liveness = $options[:ping_liveness]
    end

  end # loop

  $logger.info "terminating"
  worker.close
  context.terminate
end

$options = {
  queue: 'tcp://127.0.0.1:5560',
  identity: nil,
  ping_interval: 5,
  ping_liveness: 3,
  syslog: false,
  loglevel: 'WARN'
}

parser = OptionParser.new do |opts|
  opts.banner = "Usage: #{opts.program_name} [options]"

  opts.on("-q", "--queue ZMQADDR", "The queue address to bind to (default=#{$options[:queue]}).") do |v|
    $options[:queue] = v
  end
  opts.on("-I", "--identity IDENTITY", "The worker identity.") do |v|
    $options[:identity] = v
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
