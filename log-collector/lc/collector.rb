module LogCollector

  class Collector < EventMachine::FileTail
    Default_Delimiter = "\n"

    def initialize(path,fileconfig,spool_queue)
      @fileconfig = fileconfig
      @delimiter = @fileconfig['delimiter'] || Default_Delimiter
      @delimiter_length = @delimiter.bytesize
      @buffer = BufferedTokenizer.new(@delimiter)

      # if doing multiline processing
      if fileconfig['multiline_re']
        # set up a Multiline processor to read line_queue and forward to spool_queue
        require 'lc/multiline'
        @line_queue = EM::LimitedQueue.new
        @line_queue.high_water_mark = spool_queue.high_water_mark
        @line_queue.low_water_mark = spool_queue.low_water_mark
        @multiline = Multiline.new(@fileconfig,@line_queue,spool_queue)
      else
        # send lines directly to spool_queue
        @line_queue = spool_queue
      end

      # intitialize the filetail, this will also set the current position
      super(path,fileconfig['startpos'])
    end

    # This method is called whenever the file position is altered, e.g.
    # when opening the file the first time, and when the file is rotated or
    # truncated.
    def newpos
      # take care of any unfinished line in the buffer before starting at the new position
      enqueue_line @buffer.flush unless @buffer.empty?
      # set the current line position
      @linepos = self.position
      $logger.debug "#{path}: new pos=#{@linepos}"
    end

    def receive_data(data)
      @buffer.extract(data).each do |line|
        enqueue_line line
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
    
    def enqueue_line(line)
      # Save the position after the current line in the event. When the
      # event has been acked by logstash, this is the position to save in
      # the state file, so we know where to resume from if a restart
      # occurs.
      @linepos += line.bytesize + @delimiter_length
      ev = LogEvent.new(path,line,@file.stat,@linepos,@fileconfig['fields'])
      $logger.debug "#{path}: enqueue ev=#{ev}"
      @line_queue.push ev
    end

  end # class Collector
  
  class Watcher < EventMachine::FileGlobWatch
    include EM::Deferrable

    def initialize(pathglob, interval=5)
      super(pathglob, interval)
    end
  
    def file_found(path)
      $logger.debug "watcher: found #{path}"
      unless @glob==path
        $logger.debug "watcher: ignoring #{path}"
        return
      end
      set_deferred_status :succeeded
      stop
    end

    def file_deleted(path)
    end

  end # class Watcher

end # module LogCollector
