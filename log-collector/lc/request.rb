module LogCollector

  class Request
    def initialize(hostname)
      @hostname = hostname
      @buffer = []
    end

    def <<(ev)
      @buffer << ev
    end

    def [](i)
      @buffer[i]
    end

    def last_event
      @buffer.last
    end

    def length
      @buffer.length
    end

    def empty?
      @buffer.empty?
    end

    def to_s
      %Q(Request[events=#{@buffer.length}])
    end

    def formatted_msg(starting_at = 0)
      {
        'serial' => Time.now.to_f.to_s,
        'host' => @hostname,
        'n' => @buffer.length-starting_at,
        'events' => formatted_events(starting_at)
      }
    end

    private
    def formatted_events(starting_at)
      @buffer[starting_at..-1].collect do |ev|
        {
          'ts' => ev.timestamp.to_f,
          'file' => ev.path,
          'msg' => ev.line,
          'flds' => ev.fields
        }
      end
    end

  end

end
