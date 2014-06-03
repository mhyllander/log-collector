module LogCollector

  class Logger
    LEVELS        = [ 'FATAL', 'ERROR', 'WARN', 'INFO', 'DEBUG' ]
    LEVEL_FATAL   = 0
    LEVEL_ERROR   = 1
    LEVEL_WARNING = 2
    LEVEL_INFO    = 3
    LEVEL_DEBUG   = 4

    attr_reader :id

    def initialize(syslog,program,level)
      @program = program
      @id = "#{program}[#{Process.pid}]"
      @level = LEVELS.index(level)
      @level = LEVELS.index('WARN') if @level.nil?
      if syslog
        require 'syslog'
        Syslog.open(program, Syslog::LOG_PID, Syslog::LOG_DAEMON)
        @prio = [ Syslog::LOG_EMERG, Syslog::LOG_ERR, Syslog::LOG_WARNING, Syslog::LOG_INFO, Syslog::LOG_DEBUG ]
        @logproc = lambda {|prio, msg| syslog prio, msg}
      else
        require 'date'
        @file = STDERR
        @logproc = lambda {|prio, msg| filelog prio, msg}
      end
    end

    def fatal(msg = nil)
      msg = yield if block_given?
      @logproc.call LEVEL_FATAL, msg
    end

    def error(msg = nil)
      return if @level < LEVEL_ERROR
      msg = yield if block_given?
      @logproc.call LEVEL_ERROR, msg
    end

    def warning(msg = nil)
      return if @level < LEVEL_WARNING
      msg = yield if block_given?
      @logproc.call LEVEL_WARNING, msg
    end

    def info(msg = nil)
      return if @level < LEVEL_INFO
      msg = yield if block_given?
      @logproc.call LEVEL_INFO, msg
    end

    def debug(msg = nil)
      return if @level < LEVEL_DEBUG
      msg = yield if block_given?
      @logproc.call LEVEL_DEBUG, msg
    end

    private

    def syslog(prio,msg)
      Syslog.log @prio[prio], "[#{LEVELS[prio]}] #{msg}"
    end

    def filelog(prio,msg)
      @file.puts "#{Time.now.to_datetime.iso8601} #{@id}: [#{LEVELS[prio]}] #{msg}"
    end

  end

end # module LogCollector
