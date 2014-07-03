module LogCollector

  class Collector
    include ErrorUtils

    FORCE_ENCODING = !! (defined? Encoding)

    def initialize(path,fileconfig,event_queue)
      @path = path
      @fileconfig = fileconfig
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
      @monitor = nil
      @symlink_monitors = []

      @pass_start = event_queue.max / 4
      @pass_countdown = @pass_start

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
        Thread.current['name'] = 'collector/init'
        Thread.current.priority = 10
        begin
          process_file(fileconfig['startpos'])
        rescue Exception => e
          on_exception e
        end
        $logger.debug "#{@path}: input thread terminating, monitor thread taking over"
      end
    end

    def terminate
      $logger.info "terminating collector for #{@path}"
      @input_thread.terminate
      cancel_monitors
    end

    def cancel_monitors
      @monitor.stop if @monitor
      @monitor = nil
      @symlink_monitors.each {|m| m.stop}
      @symlink_monitors.clear
    end

    def process_file(startpos)
      if File.exists?(@path)
        open
        if (startpos == -1)
          $logger.info "#{@path} opened, start reading from end"
          @position = @file.sysseek(0, IO::SEEK_END)
          reset
        else
          $logger.info "#{@path} opened, start reading from pos #{startpos}"
          @position = @file.sysseek(startpos, IO::SEEK_SET)
          reset
          read_to_eof
          # If we were far behind when starting to read the file, the file could have been rotated
          # while we were catching up. Therefore we need to loop here and read the new file until we
          # really catch up.
          # Check if it's a different file, or if it has shrunk (been truncated).
          fstat = File.stat(@path) rescue nil
          while fstat && (fstat.dev!=@stat[:dev] || fstat.ino!=@stat[:ino]) || @file && @file.size<@position
            $logger.info "#{@path}: file appears to have been rotated while catching up"
            open
            read_to_eof
            fstat = File.stat(@path) rescue nil
          end
        end
      end
      # start monitoring the file
      monitor_file
    end

    # inotify events:
    # file created:  created + modified
    # file deleted:  deleted
    # file modified: modified
    # file renamed:  renamed
    def monitor_file
      pn = Pathname.new(@path)
      dir, base = pn.split.map {|p| p.to_s}
      @monitor = JRubyNotify::Notify.new
      @monitor.watch(dir, JRubyNotify::FILE_ANY, false) do |change, path, file, newfile|
        Thread.current['name'] = 'collector/monitor'
        Thread.current.priority = 10
        begin
          $logger.debug "#{@path}: detected '#{change}' #{path}/#{file} (#{newfile})"
          if file==base
            case change
            when :modified
              # Open the file if it was closed. This can happen if the file was deleted, then closed
              # after the dead time had passed, and then was written to again.
              # TODO(mhy): DELETED FILE: uncomment this if implementing a dead time for deleted files.
              #open if @file.nil? || @file.closed?

              if @file.size < @position
                # file has shrunk and is probably truncated
                $logger.info "#{@path}: #{file} truncated, start reading from beginning"
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
              $logger.info "#{@path}: #{file} renamed, finishing #{newfile} before continuing with new file"
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
              $logger.info "#{@path}: #{file} deleted, file kept open in case more is written to it"
            when :created
              $logger.info "#{@path}: created, start reading from beginning"
              # (re-)open the file
              open
              # next event :modified follows immediately
            end # case change
          elsif change==:renamed && newfile==base
            # Some other file has replaced the one we were monitoring. Finish the current file,
            # then open the new one.
            read_to_eof
            open
          end # if file==base
        rescue Exception=>e
          on_exception e, false
        end
      end # @monitor.watch
      @monitor.run
      $logger.info %Q[#{@path}: watching "#{dir}" for notifications about "#{base}"]

      # check if any parents are symlinks, and if so monitor the symlinks
      pn = pn.parent
      until pn.root?
        @symlink_monitors << monitor_symlink(pn) if pn.symlink?
        pn = pn.parent
      end # until pn.root?
    end

    def monitor_symlink(pn)
      dir, base = pn.split.map {|p| p.to_s}
      $logger.info %Q[#{@path}: watching "#{dir}" for notifications about "#{base}" symlink]
      mon = JRubyNotify::Notify.new
      mon.watch(dir, JRubyNotify::FILE_ANY, false) do |change, path, file, newfile|
        begin
          $logger.debug "#{@path}: detected '#{change}' #{path}/#{file} (#{newfile})"
          case change
          when :created
            if file==base
              # base was re-created, either as a symlink or a directory
              check_if_monitor_restart dir, base
            end
          when :renamed
            # symlink base was renamed, or another symlink/dir was renamed to base
            if file==base       # like a delete
              # if a symlink was deleted we just keep on reading the open file
              # and monitor for the re-creation of the symlink/directory
            elsif newfile==base # like a create
              # base was re-created, either as a symlink or a directory
              check_if_monitor_restart dir, base
            end
          when :deleted
            # a symlink was deleted
            if file==base
              # if a symlink was deleted we just keep on reading the open file
              # and monitor for the re-creation of the symlink/directory
            end
          end # case change
        rescue Exception=>e
          on_exception e, false
        end
      end # mon.watch
      mon.run
      mon
    end

    def check_if_monitor_restart(dir,base)
      fstat = File.stat(@path) rescue nil
      if fstat && (fstat.dev!=@stat[:dev] || fstat.ino!=@stat[:ino])
        $logger.info "#{@path}: file appears to be different when #{dir}/#{base} changed"
        cancel_monitors
        read_to_eof
        process_file 0
      end
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

      # after a number of queued events, pass control to another thread to avoid starvation
      @pass_countdown -= 1
      if @pass_countdown <= 0
        @pass_countdown = @pass_start
        Thread.pass
      end
    end

  end # class Collector

end # module LogCollector
