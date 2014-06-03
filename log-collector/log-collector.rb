#!/usr/bin/env ruby

libdir = File.expand_path(File.dirname(__FILE__))
$LOAD_PATH.unshift(libdir) unless $LOAD_PATH.include?(libdir)

require 'rubygems'
require 'eventmachine'
require 'eventmachine-tail'
require 'ffi-rzmq'
require 'optparse'
require 'json'
require 'socket'
require 'zlib'

require 'lc/logger'
require 'lc/config'
require 'lc/logevent'
require 'lc/limited_queue'
require 'lc/collector'
require 'lc/spooler'
require 'lc/state'

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

spool_queue = EM::LimitedQueue.new
spool_queue.high_water_mark = config.queue_high
spool_queue.low_water_mark = config.queue_low
state_queue = EM::Queue.new
collectors = []
spooler = nil
state = nil
EM.run do
  config.files.each do |path,fc|
    if File.file? path
      collectors << LogCollector::Collector.new(path,fc,spool_queue)
    else
      LogCollector::Watcher.new(path).callback {collectors << LogCollector::Collector.new(path,fc,spool_queue)}
    end
  end
  spooler = LogCollector::Spooler.new(config,spool_queue,state_queue)
  state = LogCollector::State.new(config,state_queue)
end
