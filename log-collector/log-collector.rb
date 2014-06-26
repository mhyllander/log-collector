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

require 'lc/error_utils.rb'
require 'lc/logger'
require 'lc/config'
require 'lc/logevent'
require 'lc/collector'
require 'lc/spooler'
require 'lc/state'
require 'lc/buftok'

include LogCollector::ErrorUtils

Thread.current['name'] = 'main'

options = {
  configfile: 'log-collector.conf',
  syslog: false,
  loglevel: 'WARN'
}

parser = OptionParser.new do |opts|
  opts.banner = "Usage: #{opts.program_name} [options]"

  opts.on("-f", "--config CONFIGFILE", "The configuration file to use.") do |v|
    options[:configfile] = v
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
$logger.debug("Debugging #{$logger.id}...")

config = LogCollector::Config.new(options[:configfile])

spool_queue = SizedQueue.new(config.queue_size)

state_mgr = LogCollector::State.new(config)
spooler = LogCollector::Spooler.new(config,spool_queue,state_mgr)

collectors = []
config.files.each do |path,fc|
  collectors << LogCollector::Collector.new(path,fc,spool_queue)
end

Signal.trap("HUP") do
  $logger.info "caught HUP signal, ignoring"
end
Signal.trap("TERM") do
  $logger.info "caught TERM signal, notifying spooler"
  spooler.terminate
end
Signal.trap("QUIT") do
  $logger.info "caught QUIT signal, notifying spooler"
  spooler.terminate
end
Signal.trap("ABRT") do
  $logger.info "caught ABRT signal, notifying spooler"
  spooler.terminate
end
Signal.trap("INT") do
  $logger.info "caught INT signal, notifying spooler"
  spooler.terminate
end

# just hang in here and let the threads do the work
spooler.spool_thread.join
