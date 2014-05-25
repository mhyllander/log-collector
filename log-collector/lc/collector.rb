module LogCollector

  class Collector < EventMachine::FileTail
    Default_Delimiter = "\n"

    def initialize(path,fileconfig,spool_queue)
      @fileconfig = fileconfig

      super(path,@fileconfig['startpos'])

      @delimiter = @fileconfig['delimiter'] || Default_Delimiter
      @delimiter_length = @delimiter.unpack('C*').length

      @buffer = BufferedTokenizer.new(@delimiter)
      @internal_queue = []

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
        if @line_queue.full?
          # save temporarily to internal queue
          @internal_queue << ev
          # suspend reading from file and schedule monitoring of the line queue
          unless self.suspended?
            $logger.debug "#{path}: suspend filetail"
            self.suspend
            schedule_line_queue_monitor
          end
        else
          @line_queue.push ev
        end
      end
    end

    def schedule_line_queue_monitor
      EM.next_tick do
        if @line_queue.full?
          $logger.debug "#{path}: line_queue full"
          # reschedule until line_queue is not full
          schedule_line_queue_monitor
        else
          $logger.debug "#{path}: line_queue not full"
          # flush the internal queue and resume reading from file
          @internal_queue.each {|ev| @line_queue.push ev}
          @internal_queue.clear
          $logger.debug "#{path}: resume filetail"
          self.resume
        end
      end
    end

  end # class Collector
  
end # module LogCollector
