#!/usr/bin/env ruby

libdir = File.expand_path(File.dirname(__FILE__))
$LOAD_PATH.unshift(libdir) unless $LOAD_PATH.include?(libdir)

require 'rubygems'
require 'eventmachine'
require 'eventmachine-tail'
#require 'em-zeromq'
require 'ffi-rzmq'
require 'optparse'
require 'json'
require 'logger'
require 'socket'
require 'zlib'

require 'lc/config'
require 'lc/logevent'
require 'lc/limited_queue'
require 'lc/collector'
require 'lc/spooler'
require 'lc/state'

$logger = Logger.new(STDERR)
$logger.level = ($DEBUG and Logger::DEBUG or Logger::WARN)
$logger.debug("Debugging log-collector...")

$options = {
  configfile: 'log-collector.conf',
  statefile: 'log-collector.state'
}

parser = OptionParser.new do |opts|
  opts.banner = "Usage: #{opts.program_name} [options]"

  opts.on("-f", "--config CONFIGFILE", "The configuration file to use.") do |v|
    $options[:configfile] = v
  end
  opts.on("-f", "--state STATEFILE", "The state file to use.") do |v|
    $options[:statefile] = v
  end

  opts.on_tail("-h", "--help", "Show this message") do
    puts opts
    exit
  end

end
parser.parse!

$config = LogCollector::Config.new($options[:configfile],$options[:statefile])

def main(args)
  spool_queue = EM::LimitedQueue.new
  spool_queue.high_water_mark = $config.queue_high
  spool_queue.low_water_mark = $config.queue_low
  state_queue = EM::Queue.new
  collectors = []
  spooler = nil
  state = nil
  EM.run do
    $config.files.each do |path,fc|
      if File.file? path
        collectors << LogCollector::Collector.new(path,fc,spool_queue)
      else
        LogCollector::Watcher.new(path).callback {collectors << LogCollector::Collector.new(path,fc,spool_queue)}
      end
    end
    spooler = LogCollector::Spooler.new($config.hostname,$config.servers,spool_queue,state_queue)
    state = LogCollector::State.new($config.state,$options[:statefile],state_queue)
  end
end # def main

exit(main(ARGV))
