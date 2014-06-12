module LogCollector

  class Collector
    Default_Delimiter = "\n"

    # Maximum size to read at a time from a single file.
    CHUNKSIZE = 128*1024

    FORCE_ENCODING = !! (defined? Encoding)

    attr_reader :input_thread

    def initialize(path,fileconfig,spool_queue)
      @path = path
      @fileconfig = fileconfig
      @delimiter = @fileconfig['delimiter'] || Default_Delimiter
      @delimiter_length = @delimiter.bytesize
      @buffer = BufferedTokenizer.new(@delimiter)

      @file = nil
      @position = 0
      @stat = {}
      @monitor = nil

      # if doing multiline processing
      if fileconfig['multiline_re']
        # set up a Multiline processor to read line_queue and forward to spool_queue
        require 'lc/multiline'
        @line_queue = SizedQueue.new(spool_queue.max)
        @multiline = Multiline.new(@fileconfig,@line_queue,spool_queue)
      else
        # send lines directly to spool_queue
        @line_queue = spool_queue
      end

      # intitialize the filetail, this will also set the current position
      @input_thread = Thread.new do
        begin
          Thread.current['name'] = 'collector'
          Thread.current.priority = 10
          handle_file(fileconfig['startpos'])
        rescue Exception => e
          $logger.error "exception raised: #{e}"
        end
      end
    end

    def handle_file(startpos)
      if File.exists?(@path)
        open
        if (startpos == -1)
          $logger.info "file #{@path} opened, start reading from end"
          @position = @file.sysseek(0, IO::SEEK_END)
          reset
        else
          $logger.info "file #{@path} opened, start reading from pos #{startpos}"
          @position = @file.sysseek(startpos, IO::SEEK_SET)
          reset
          read_to_eof
        end
        # start monitoring the file
        monitor_file
      else
        # watch for the creation of the log file
        notify = JRubyNotify::Notify.new
        dir, base = Pathname.new(@path).split
        notify.watch(dir.to_s, JRubyNotify::FILE_CREATED, false) do |change, paths|
          $logger.debug "detected '#{change}': paths=#{paths}"
          if paths[1]==base.to_s
            $logger.info "#{@path}: created, start reading from beginning"
            open
            read_to_eof
            # stop and release this notifier
            notify.stop
            notify = nil
            # start monitoring the file
            monitor_file
          end
        end
        notify.run
        $logger.warning "#{@path}: not found, watching #{dir} for creation of #{base}"
      end
    end

    def monitor_file
      $logger.debug "start monitoring file #{@path}"
      @monitor = JRubyNotify::Notify.new
      @monitor.watch(@path, (JRubyNotify::FILE_DELETED | JRubyNotify::FILE_MODIFIED | JRubyNotify::FILE_RENAMED), false) do |change, paths|
        # TODO(mhy): can we get new notifications while processing the previous one?

        $logger.debug { "#{@path}: detected '#{change}'" }
        case change
        when :modified
          read_to_eof
        when :deleted, :renamed # file is presumably rotated
          # check that we have read to EOF before re-opening
          unless @file.eof?
            $logger.warning "#{@path}: not at EOF when file was rotated"
            read_to_eof
          end
          open # re-open the file
          read_to_eof
        end
      end
      @monitor.run
      $logger.debug "#{@path}: waiting for file modified notification"
    end

    def open
      @file.close if @file && !@file.closed?
      @position = 0
      return unless File.exists?(@path)
      begin
        $logger.debug "#{@path}: opening file"
        @file = File.open(@path, "r")
      rescue Errno::ENOENT => e
        $logger.warning "#{@path}: file not found: #{e}"
        on_exception(e)
      end
      @stat = {dev: @file.stat.dev, inode: @file.stat.ino}
      reset
    end

    def reset
      $logger.debug "#{@path}: reset (buffer.empty?=#{@buffer.empty?})"
      # take care of any unfinished line in the buffer before starting at the new position
      enqueue_line @buffer.flush unless @buffer.empty?
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
          data = @file.sysread(CHUNKSIZE)
        rescue EOFError, IOError
          return
        rescue Exception => e
          $logger.error("Error reading: '#{@path}' (#{e})")
          on_exception(e)
        end

        data.force_encoding(@file.external_encoding) if FORCE_ENCODING

        # Won't get here if sysread throws EOF
        @position = @file.pos

        receive_data(data)
      end
    end

    def receive_data(data)
      @buffer.extract(data).each do |line|
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

    def on_exception(exception)
      $logger.error("Exception raised. Using default handler in #{self.class.name}")
      raise exception
    end
  end # class Collector

end # module LogCollector
