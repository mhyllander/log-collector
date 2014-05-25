module LogCollector

  class Collector < EventMachine::FileTail
    Default_Delimiter = "\n"

    def initialize(path,fileconfig,spool_queue)
      @fileconfig = fileconfig

      super(path,@fileconfig['startpos'])

      @delimiter = @fileconfig['delimiter'] || Default_Delimiter
      @delimiter_length = @delimiter.unpack('C*').length

      @buffer = BufferedTokenizer.new(@delimiter)

      # if doing multiline processing
      if fileconfig['multiline_re']
        # set up a Multiline processor to read line_queue and forward to spool_queue
        require 'lc/multiline'
        @line_queue = EM::LimitedQueue.new
        @line_queue.low_water_mark = spool_queue.low_water_mark
        @line_queue.high_water_mark = spool_queue.high_water_mark
        @multiline = Multiline.new(@fileconfig,@line_queue,spool_queue)
      else
        # send lines directly to spool_queue
        @line_queue = spool_queue
      end
    end

    def receive_data(data,fstat,fpos)
      pos = fpos
      @buffer.extract(data).each do |line|
        # Save the position after the current line in the event. When the event has been acked by logstash,
        # this is the position to save in the state file, so we know where to resume from if a restart occurs.
        pos += line.unpack('C*').length + @delimiter_length
        ev = LogEvent.new(path,line,fstat,pos,@fileconfig['fields'])
        $logger.debug "#{path}: enqueue ev=#{ev}"
        @line_queue.push ev

        if @line_queue.full?
          # suspend reading from file
          unless self.suspended?
            $logger.debug "#{path}: suspend filetail"
            self.suspend
            @line_queue.callback do
              $logger.debug "#{path}: resume filetail"
              self.resume
            end
          end
        end
      end
    end

  end # class Collector
  
end # module LogCollector
