#!/usr/bin/env ruby

libdir = File.expand_path(File.dirname(__FILE__))
$LOAD_PATH.unshift(libdir) unless $LOAD_PATH.include?(libdir)

require 'rubygems'
require 'ffi-rzmq'
require 'optparse'
require 'json'
require 'socket'
require 'zlib'
require 'thread'
require 'jruby-notify'
require 'pathname'

require 'java'
java_import 'java.lang.OutOfMemoryError'

require 'lc/error_utils.rb'
require 'lc/logger'
require 'lc/config'
require 'lc/logevent'
require 'lc/request'
require 'lc/monitor'
require 'lc/collector'
require 'lc/spooler'
require 'lc/sender'
require 'lc/state'
require 'lc/buftok'

include LogCollector::ErrorUtils

Thread.current[:name] = 'main'

options = {
  configfile: 'log-collector.conf',
  identity: nil,
  syslog: false,
  loglevel: 'WARN'
}

parser = OptionParser.new do |opts|
  opts.banner = "Usage: #{opts.program_name} [options]"

  opts.on("-f", "--config CONFIGFILE", "The configuration file to use.") do |v|
    options[:configfile] = v
  end
  opts.on("-I", "--identity IDENTITY", "The client identity.") do |v|
    options[:identity] = v
  end
  opts.on("-l", "--[no-]syslog", "Log to syslog (default=#{options[:syslog]}).") do |v|
    options[:syslog] = v
  end
  opts.on("-L", "--loglevel LOGLEVEL", "Log level (default=#{options[:loglevel]}).") do |v|
    options[:loglevel] = v.upcase
  end

  opts.on_tail("-h", "--help", "Show this message") do
    puts opts
    exit
  end

end
parser.parse!

$logger = LogCollector::Logger.new options[:syslog], parser.program_name, ($DEBUG ? 'DEBUG' : options[:loglevel])
$logger.debug("Debugging #{$logger.process_id}...")

config = LogCollector::Config.new(options[:configfile])
event_queue = SizedQueue.new(config.queue_size)
request_queue = SizedQueue.new(1)
state_mgr = LogCollector::State.new(config)
@sender = LogCollector::Sender.new(config,options[:identity],request_queue,state_mgr)
@spooler = LogCollector::Spooler.new(config,event_queue,request_queue)
@monitor = LogCollector::Monitor.new(config,event_queue,state_mgr)

@shutdown_mutex = Mutex.new

def shutdown
  @shutdown_mutex.synchronize {
    begin
      @monitor.terminate if @monitor
    rescue Exception => e
      on_exception e, false
    end
    @monitor = nil
    begin
      @spooler.terminate if @spooler
    rescue Exception => e
      on_exception e, false
    end
    @spooler = nil
    begin
      @sender.terminate if @sender
    rescue Exception => e
      on_exception e, false
    end
    @sender = nil
  }
end

Signal.trap("HUP") do
  Thread.current[:name] = "SIGHUP-#{Thread.current.object_id}"
  $logger.info "caught HUP signal, ignoring"
end
Signal.trap("TERM") do
  Thread.current[:name] = "SIGTERM-#{Thread.current.object_id}"
  $logger.info "caught TERM signal, shutting down"
  shutdown
end
Signal.trap("INT") do
  Thread.current[:name] = "SIGINT-#{Thread.current.object_id}"
  $logger.info "caught INT signal, shutting down"
  shutdown
end

# just hang in here and let the threads do the work
@sender.send_thread.join
