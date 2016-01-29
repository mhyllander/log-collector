module LogCollector

  class Collector
    include ErrorUtils

    FORCE_ENCODING = !! (defined? Encoding)

    attr_reader :path,:stat,:position

    def initialize(path,fileconfig,event_queue,monitor)
      @path = path
      @fileconfig = fileconfig
      @event_queue = event_queue
      @monitor = monitor

      @notification_queue = Queue.new
      @last_notification = nil

      @deadtime = fileconfig['deadtime']
      @delimiter = fileconfig['delimiter']
      @chunksize = fileconfig['chunksize']
      @select_re = Regexp.new(fileconfig['select_re']) if fileconfig['select_re']
      @exclude_re = Regexp.new(fileconfig['exclude_re']) if fileconfig['exclude_re']

      @delimiter_length = @delimiter.bytesize
      @buffer = BufferedTokenizer.new(@delimiter)

      @file = nil
      @position = 0
      @stat = {}
      @collector_id = @path

      @multiline = false
      @multiline_re = nil
      @multiline_invert = nil
      @multiline_wait = nil
      @multiline_ev = nil
      @multiline_flush_thread = nil

      # if doing multiline processing
      if fileconfig['multiline_re']
        @multiline = true
        @multiline_re = Regexp.new(fileconfig['multiline_re'])
        @multiline_invert = fileconfig['multiline_invert']
        @multiline_wait = fileconfig['multiline_wait']

        @multiline_flush_thread = Thread.new do
          Thread.current['name'] = 'collector/flush'
          Thread.current['started'] = Time.now.strftime "%Y%m%dT%H%M%S.%L"
          loop do
            begin
              multiline_schedule_flush_held_ev
            rescue OutOfMemoryError
              abort "Collector: exiting because of java.lang.OutOfMemoryError"
            rescue Exception => e
              on_exception e, false
            end
          end
        end
      end

      @collector_thread = Thread.new do
        Thread.current[:name] = 'collector'
        Thread.current[:started] = Time.now.strftime "%Y%m%dT%H%M%S.%L"
        Thread.current.priority = 2
        begin
          # resume reading the file from startpos
          resume_file fileconfig['startpos']
          # monitoring the file for changes
          monitor_file
        rescue OutOfMemoryError
          abort "Collector: exiting because of java.lang.OutOfMemoryError"
        rescue Exception => e
          on_exception e, false
        end
      end
    end

    def terminate
      $logger.info "#{@collector_id}: terminating"
      @multiline_flush_thread.terminate if @multiline_flush_thread
      @collector_thread.terminate
    end

    def resume_file(startpos)
      if File.exists?(@path)
        open
        if (startpos == -1)
          $logger.info "#{@collector_id}: opened, start reading from end"
          @position = @file.sysseek(0, IO::SEEK_END)
          flush
        else
          $logger.info "#{@collector_id}: opened, start reading from pos #{startpos}"
          if startpos != @position
            @position = @file.sysseek(startpos, IO::SEEK_SET)
            flush
          end
          read_to_eof
        end
      end
    end

    # notify is used by Monitor to enqueue notifications
    def notify(notification)
      # avoid queuing multiple :modified events in a row
      @notification_queue << notification if @notification_queue.empty? || notification!=:modified || @last_notification!=:modified
      @last_notification = notification
    end

    # notifications:
    # file created:  :created + :modified,
    # file deleted:  :deleted
    # file modified: :modified
    # file renamed:  :renamed
    # file replaced: :replaced
    # check file:    :check
    def monitor_file
      loop do
        notification = @notification_queue.pop
        begin
          $logger.debug %Q[#{@collector_id}: notification="#{notification}"]
          case notification
          when :modified
            if @file.size < @position
              # file has shrunk and is probably truncated
              $logger.info "#{@collector_id}: truncated, start reading from beginning"
              @position = @file.sysseek(0, IO::SEEK_SET)
              flush
            end
            read_to_eof
          when :renamed, :deleted, :replaced
            # File has either been rotated, or deleted/replaced. Note that the writing application
            # may continue to write for a while to the old file before it is restarted or notified
            # about the rotation. This collector will continue reading from the old file
            # periodically until no data has been written during the last @deadtime interval. The
            # collector will then close the file and terminate. A new collector will (eventually) be
            # started to read the new file.
            $logger.info %Q[#{@collector_id}: "#{notification}", read data until dead]
            # Continue reading from the file as long as there is new data, until @deatime has passed
            # and no new data exists.
            read_to_eof
            begin
              sleep @deadtime
              $logger.info "#{@collector_id}: check for new data"
            end while read_to_eof
            @file.close
            $logger.info "#{@collector_id}: pronounced dead, collector terminating"
            @monitor.forget_old_collector self
            Thread.current.exit
          when :created
            $logger.info %Q[#{@collector_id}: "#{notification}", (re-)open from beginning]
            # (re-)open the file
            open
            # next event :modified follows immediately
          when :check
            fstat = File.stat(@path) rescue nil
            if fstat && (fstat.dev!=@stat['dev'] || fstat.ino!=@stat['ino'])
              $logger.info "#{@collector_id}: file appears to be new, re-open and start from beginning"
              # finish current file
              read_to_eof
              # re-open the file and start from beginning
              open
              read_to_eof
            end
          when :flush
            multiline_flush
          end # case notification
        rescue OutOfMemoryError
          raise
        rescue Exception=>e
          on_exception e, false
        end
      end # loop
    end

    def open
      @file.close if @file && !@file.closed?
      @position = 0
      @stat = {}
      @collector_id = @path
      return unless File.exists?(@path)
      begin
        $logger.debug "#{@collector_id}: opening file"
        @file = File.open(@path, "r")
      rescue Errno::ENOENT => e
        $logger.warning "#{@collector_id}: file not found"
        @file = nil
        on_exception e
      rescue OutOfMemoryError
        raise
      end
      fstat = @file.stat
      @stat = {'dev' => fstat.dev, 'ino' => fstat.ino}
      @collector_id = "#{@path}(#{@stat['dev']}/#{@stat['ino']})"
      $logger.debug "#{@collector_id}: stat=#{@stat}"
      flush
    end

    def flush
      $logger.debug { "#{@collector_id}: flush (buffer #{@buffer.empty? ? 'is empty' : 'has data'})" }
      # take care of any unfinished line in the buffer before starting at the new position
      remaining = @buffer.flush.chomp
      enqueue_line remaining unless remaining.empty?
      # set the current line position
      @linepos = @position
      $logger.info "#{@collector_id}: flush, pos=#{@linepos}"
    end

    def read_to_eof
      # loop until we reach EOF
      read_data = false
      loop do
        data = nil
        $logger.debug "#{@collector_id}: reading..."
        begin
          data = @file.sysread(@chunksize)
          read_data = true
        rescue EOFError, IOError
          $logger.debug "#{@collector_id}: eof"
          return read_data
        rescue OutOfMemoryError
          raise
        rescue Exception => e
          $logger.error("#{@collector_id}: error reading")
          on_exception e
        end

        data.force_encoding(@file.external_encoding) if FORCE_ENCODING

        # Won't get here if sysread throws EOF
        @position = @file.pos

        receive_data(data)
      end
    end

    def receive_data(data)
      @buffer.extract(data).each do |line|
        next if @select_re && !@select_re.match(line) || @exclude_re && @exclude_re.match(line)
        enqueue_line line
      end
    end

    def enqueue_line(line)
      # Save the position after the current line in the event. When the
      # event has been acked by logstash, this is the position to save in
      # the state file, so we know where to resume from if a restart
      # occurs.
      @linepos += line.bytesize + @delimiter_length

      if @multiline
        multiline_reset_flush
        multiline_process line
      else
        ev = new_event line
        $logger.debug { "#{@collector_id}: enqueue ev=#{ev}" }
        @event_queue.push ev
      end
    end
    
    def new_event(line)
      LogEvent.new(@path,line,@stat,@linepos,@fileconfig['fields'])
    end

    def multiline_process(line)
      # Lines the match @multiline_re are continuation lines that will be appended to the previous line.
      # If @multiline_invert is true, then lines that don't match @multiline_re are continuation lines.
      # The "!= @multiline_invert" clause is a clever/sneaky way of inverting the result of the match.
      if !@multiline_re.match(line).nil? != @multiline_invert
        if @multiline_ev.nil?
          @multiline_ev = new_event line
          $logger.debug { "multiline(no previous): @multiline_ev=#{@multiline_ev}" }
        else
          @multiline_ev.append line,@stat,@linepos
          $logger.debug { "multiline(append): @multiline_ev=#{@multiline_ev}" }
        end
      else
        # not a continuation line, so send any held event
        $logger.debug { "multiline(enqueue): @multiline_ev=#{@multiline_ev}" } if @multiline_ev
        @event_queue.push(@multiline_ev) if @multiline_ev
        @multiline_ev = new_event line
      end
    end

    def multiline_flush
      if @multiline_ev
        $logger.debug { "multiline(flush): @multiline_ev=#{@multiline_ev}" }
        @event_queue.push(@multiline_ev)
        @multiline_ev = nil
      end
    end

    def multiline_schedule_flush_held_ev
      loop do
        # loop until a full @multiline_wait has been slept
        slept = sleep(@multiline_wait) until slept==@multiline_wait
        # flush any held event
        $logger.debug "schedule flush multiline_ev"
        @notification_queue.push :flush
      end
    end

    # restart the flush timer
    def multiline_reset_flush
      @multiline_flush_thread.wakeup
    end
  end # class Collector

end # module LogCollector
