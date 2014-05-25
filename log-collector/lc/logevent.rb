module LogCollector

  class LogEvent
    attr_reader :path
    attr_reader :line
    attr_reader :stat
    attr_reader :pos
    attr_reader :fields

    def initialize(path,line,stat,pos,fields)
      @path = path
      @line = line
      @stat = stat
      @pos = pos
      @fields = fields
    end

    def append(ev)
      @line += "\n" + ev.line
      @stat = ev.stat
      @pos = ev.pos
    end

    def dev
      @stat.dev
    end

    def inode
      @stat.ino
    end

    def to_s
      "LogEvent[#{path}(#{@stat.dev}/#{@stat.ino}): pos=#{@pos} log=#{@line}]"
    end
  end

end
