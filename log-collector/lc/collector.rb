module LogCollector

  class Collector
    include ErrorUtils

    FORCE_ENCODING = !! (defined? Encoding)

    def initialize(path,fileconfig,event_queue)
      @path = path
      @fileconfig = fileconfig

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

      # if doing multiline processing
      if fileconfig['multiline_re']
        # set up a Multiline processor to read line_queue and forward to event_queue
        require 'lc/multiline'
        @line_queue = SizedQueue.new(event_queue.max)
        @multiline = Multiline.new(@fileconfig,@line_queue,event_queue)
      else
        # send lines directly to event_queue
        @line_queue = event_queue
      end

      # intitialize the filetail, this will also set the current position
      @input_thread = Thread.new do
        Thread.current['name'] = 'collector'
        Thread.current.priority = 10
        begin
          # resume reading the file from startpos
          resume_file fileconfig['startpos']
          # monitoring the file for changes
          monitor_file
        rescue Exception => e
          on_exception e
        end
      end
    end

    def terminate
      $logger.info "terminating collector for #{@path}"
      @input_thread.terminate
    end

    def cancel_monitors
      @monitor.stop if @monitor
      @monitor = nil
      @symlink_monitors.each {|m| m.stop}
      @symlink_monitors.clear
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
            # Open the file if it was closed. This can happen if the file was deleted, then closed
            # after the dead time had passed, and then was written to again.
            # TODO(mhy): DELETED FILE: uncomment this if implementing a dead time for deleted files.
            #open if @file.nil? || @file.closed?
            
            if @file.size < @position
              # file has shrunk and is probably truncated
              $logger.info "#{@path}: truncated, start reading from beginning"
              @position = @file.sysseek(0, IO::SEEK_SET)
              reset
            end
            read_to_eof
          when :renamed
            # File has presumably been rotated. Note that the writing application may continue to
            # write for a short while to the old file before it is restarted or notified about the
            # rotation. This currently solved by waiting for @deadtime to pass before reading one
            # final time and then closing the file. After that a :created event is expected for when
            # the log file is re-created. This a design decision to avoid having to monitor both old
            # and new files simultaneously. With this design the old file will be finalized and
            # closed before opening the new file.
            $logger.info "#{@path}: renamed, finishing current before continuing with new file"
            start_read = Time.now
            read_to_eof
            read_time = Time.now - start_read
            # If final reading was very quick (i.e. we were already at eof), wait for the rest of
            # @deadtime and check one more time for any more data before closing and waiting for the
            # creation of a new file.
            if read_time < @deadtime
              sleep @deadtime-read_time
              read_to_eof
            end
            @file.close
          when :deleted
            # We don't really do anything about this. The file can still be open for writing even
            # though it has been deleted, and we will receive :modified events if so. If the file is
            # re-created we will receive a :created event, and we will then close the current file
            # and open the new one.
            # TODO(mhy): DELETED FILE: may want to implement a "dead time" interval to close and
            # release the deleted file.
            $logger.info "#{@path}: deleted, file kept open in case more is written to it"
          when :created
            $logger.info "#{@path}: created, (re-)open from beginning"
            # (re-)open the file
            open
            # next event :modified follows immediately
          when :replaced
            # Some other file has replaced the one we were monitoring. Finish the current file,
            # then open the new one.
            $logger.info "#{@path}: replaced, read to eof and re-open from beginning"
            # finish current file
            read_to_eof
            # re-open the file and start from beginning
            open
            read_to_eof
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
          end # case change
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
      begin
        $logger.debug "#{@path}: opening file"
        @file = File.open(@path, "r")
      rescue Errno::ENOENT => e
        $logger.warning "#{@path}: file not found"
        @file = nil
        on_exception e
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
      loop do
        data = nil
        $logger.debug "#{@path}: reading..."
        begin
          data = @file.sysread(@chunksize)
        rescue EOFError, IOError
          $logger.debug "#{@path}: eof"
          return
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
      ev = LogEvent.new(@path,line,@stat,@linepos,@fileconfig['fields'])
      $logger.debug { "#{@path}: enqueue ev=#{ev}" }
      @line_queue.push ev
    end

  end # class Collector

end # module LogCollector
