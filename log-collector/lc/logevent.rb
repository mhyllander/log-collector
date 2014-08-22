module LogCollector

  class LogEvent
    attr_reader :path
    attr_reader :line
    attr_reader :stat
    attr_reader :pos
    attr_reader :fields
    attr_reader :timestamp

    def initialize(path,line,stat,pos,fields)
      @path = path
      @line = line
      @stat = stat
      @pos = pos
      @fields = fields

      @timestamp = Time.now
    end

    def append(line,stat,pos)
      @line << "\n" << line
      @stat = stat
      @pos = pos
    end

    def dev
      @stat[:dev]
    end

    def ino
      @stat[:ino]
    end

    def to_s
      %Q(LogEvent[#{path}(#{dev}/#{ino}): pos=#{@pos} log="#{@line[0..20]}"...])
    end
  end

end
