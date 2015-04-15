module LogCollector

  class Collector
    include ErrorUtils

    FORCE_ENCODING = !! (defined? Encoding)

    def initialize(path,fileconfig,event_queue)
      @path = path
      @fileconfig = fileconfig
      @event_queue = event_queue

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
        Thread.current['name'] = 'collector'
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
      $logger.info "terminating collector for #{@path}"
      @multiline_flush_thread.terminate if @multiline_flush_thread
      @collector_thread.terminate
    end

    def resume_file(startpos)
      if File.exists?(@path)
        open
        if (startpos == -1)
          $logger.info "#{@path}: opened, start reading from end"
          @position = @file.sysseek(0, IO::SEEK_END)
          reset
        else
          $logger.info "#{@path}: opened, start reading from pos #{startpos}"
          @position = @file.sysseek(startpos, IO::SEEK_SET)
          reset
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
        change = @notification_queue.pop
        begin
          $logger.debug "#{@path}: detected '#{change}'"
          case change
          when :modified
            if @file.size < @position
              # file has shrunk and is probably truncated
              $logger.info "#{@path}: truncated, start reading from beginning"
              @position = @file.sysseek(0, IO::SEEK_SET)
              reset
            end
            read_to_eof
          when :renamed, :deleted, :replaced
            # File has either been rotated, or deleted/replaced. Note that the writing application
            # may continue to write for a short while to the old file before it is restarted or
            # notified about the rotation. This currently solved by waiting for @deadtime to pass
            # before reading one final time and then closing the file. A new collector will be
            # started on the new file. This collector will finish off teh old file and then
            # terminate.
            $logger.info "#{@path}(#{@stat[:dev]}/#{@stat[:ino]}): #{change}, read data until dead"
            @stat[:active] = false
            # Continue reading from the file as long as their is new data, until @deatime has passed
            # and no new data exists.
            read_to_eof
            begin
              sleep @deadtime
              $logger.info "#{@path}(#{@stat[:dev]}/#{@stat[:ino]}): read data, check if dead"
            end while read_to_eof
            @file.close
            $logger.info "#{@path}(#{@stat[:dev]}/#{@stat[:ino]}): pronounced dead, collector terminating"
            Thread.current.exit
          when :created
            $logger.info "#{@path}: created, (re-)open from beginning"
            # (re-)open the file
            open
            # next event :modified follows immediately
          when :check
            fstat = File.stat(@path) rescue nil
            if fstat && (fstat.dev!=@stat[:dev] || fstat.ino!=@stat[:ino])
              $logger.info "#{@path}: file appears to be new, re-open and start from beginning"
              # finish current file
              read_to_eof
              # re-open the file and start from beginning
              open
              read_to_eof
            end
          when :flush
            multiline_flush
          end # case change
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
      return unless File.exists?(@path)
      @stat[:active] = true
      begin
        $logger.debug "#{@path}: opening file"
        @file = File.open(@path, "r")
      rescue Errno::ENOENT => e
        $logger.warning "#{@path}: file not found"
        @file = nil
        on_exception e
      rescue OutOfMemoryError
        raise
      end
      fstat = @file.stat
      @stat[:dev], @stat[:ino] = fstat.dev, fstat.ino
      $logger.debug "#{@path}: stat=#{@stat}"
      reset
    end

    def reset
      $logger.debug { "#{@path}: reset (buffer #{@buffer.empty? ? 'is empty' : 'has data'})" }
      # take care of any unfinished line in the buffer before starting at the new position
      remaining = @buffer.flush.chomp
      enqueue_line remaining unless remaining.empty?
      # set the current line position
      @linepos = @position
      $logger.info "#{@path}: reset, pos=#{@linepos}"
    end

    def read_to_eof
      # loop until we reach EOF
      read_data = false
      loop do
        data = nil
        $logger.debug "#{@path}: reading..."
        begin
          data = @file.sysread(@chunksize)
          read_data = true
        rescue EOFError, IOError
          $logger.debug "#{@path}: eof"
          return read_data
        rescue OutOfMemoryError
          raise
        rescue Exception => e
          $logger.error("#{@path}: error reading")
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
        $logger.debug { "#{@path}: enqueue ev=#{ev}" }
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
