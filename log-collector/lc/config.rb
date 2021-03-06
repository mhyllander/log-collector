module LogCollector

  class Config
    Default_StateFile = 'log-collector.state'
    Default_FlushInterval = '30s'
    Default_FlushSize = 1000

    Default_QueueSize = 2000
    Default_MultilineWait = '5s'

    Default_SendErrorDelay = '1s'
    Default_RecvTries = 1
    Default_RecvTimeout = '60s'

    Default_Delimiter = "\n"
    Default_DeadTime = '60s'
    Default_ChunkSize = 128*1024

    attr_reader :state

    def initialize(config_file)
      json_config = File.read(config_file)
      @config = JSON.parse(json_config)

      @config['hostname'] ||= Socket.gethostname
      @config['state_file'] ||= Default_StateFile
      @config['queue_size'] ||= Default_QueueSize
      @config['flush_interval'] = duration_for(@config['flush_interval'] || Default_FlushInterval)
      @config['flush_size'] ||= Default_FlushSize

      @config['send_error_delay'] = duration_for(@config['send_error_delay'] || Default_SendErrorDelay)
      @config['recv_tries'] ||= Default_RecvTries
      @config['recv_timeout'] = duration_for(@config['recv_timeout'] || Default_RecvTimeout)

      @config['files'].each do |path,fc|
        fc['startpos'] ||= -1 # default is to start at end of file
        fc['multiline_invert'] ||= false
        fc['multiline_wait'] = duration_for(fc['multiline_wait'] || Default_MultilineWait)
        fc['delimiter'] ||= Default_Delimiter
        fc['deadtime'] = duration_for(fc['deadtime'] || Default_DeadTime)
        fc['chunksize'] ||= Default_ChunkSize
      end

      @state = {}
      if File.file? state_file
        json_state = File.read(state_file)
        @state = JSON.parse(json_state) rescue {}

        # remove states for files that are no longer being monitored
        @state.keys.each {|p| @state.delete(p) unless @config['files'].include?(p)}

        @config['files'].each do |path,fc|

          # does the file exist?
          if File.file? path

            # get current file info
            file_info = get_file_info(path)

            # have we been monitoring this file before?
            if saved_state = @state[path]

              # is it the same file as before?
              if file_info[:dev]==saved_state['dev'] && file_info[:ino]==saved_state['ino']
                # same file
                if file_info[:size] < saved_state['pos']
                  # file truncated, start at beginning
                  fc['startpos'] = 0
                  $logger.info "#{path}: truncated, starting at beginning"
                else
                  # resume at saved position
                  fc['startpos'] = saved_state['pos']
                  $logger.info "#{path}: resuming at saved position"
                end
              else
                # not same file, probably rotated, start at beginning
                fc['startpos'] = 0
                $logger.info "#{path}: not same file, probably rotated, starting at beginning"
              end

            else # ! saved_state
              # not monitored before, start at end
              $logger.info "#{path}: not monitored before, starting from end"
            end # saved_state

          else # !file exists
            # file doesn't exist (yet), so start at beginning
            fc['startpos'] = 0
            # delete any saved state for the file
            @state.delete path
            $logger.info "#{path}: nonexistant, deleting state"
          end

        end # file loop

      end # state_file exists

    end

    def servers
      @config['network']['servers']
    end

    def ssl
      @config['network']['ssl']
    end

    def hostname
      @config['hostname']
    end

    def files
      @config['files']
    end

    def state_file
      @config['state_file']
    end

    def flush_interval
      @config['flush_interval']
    end

    def flush_size
      @config['flush_size']
    end

    def send_error_delay
      @config['send_error_delay']
    end

    def recv_tries
      @config['recv_tries']
    end

    def recv_timeout
      @config['recv_timeout']
    end

    def queue_size
      @config['queue_size']
    end

    private
    def get_file_info(f)
      s = f.is_a?(String) ? File.stat(f) : f.stat
      {dev: s.dev, ino: s.ino, mtime: s.mtime, size: s.size}
    end

    private
    def duration_for(spec)
      return spec if spec.is_a?(Integer) || spec.is_a?(Float)
      ret = nil
      if((m0 = %r/^(\d+(?:\.\d+)?):(\d+(?:\.\d+)?):(\d+(?:\.\d+)?)$/.match(spec.to_s)))
        _, h, m, s, _ = m0.to_a
        h, m, s = Float(h), Float(m), Float(s)
        ret = (h * 60 * 60) + (m * 60) + (s)
      else
        pat = %r/(\d+(?:\.\d+)?)\s*([sSmMhHdDwWyY][^\d]*)?/
        begin
          "#{ spec }".scan(pat) do |m1|
            n = Float m1[0]
            unit = m1[1]
            if unit
              factor =
                case unit
                when %r/^m/i
                  case unit
                  when %r/^mo/i
                    30 * (60 * 60 * 24)
                  else
                    60
                  end
                when %r/^h/i
                  60 * 60
                when %r/^d/i
                  60 * 60 * 24
                when %r/^w/i
                  7 * (60 * 60 * 24)
                when %r/^y/i
                  365 * 7 * (60 * 60 * 24)
                else
                  1
                end
              n *= factor
            end
            ret ||= 0.0
            ret += n
          end
        rescue
          raise "bad time spec <#{ spec }>"
        end
      end
      ret
    end
  end # class Config

end # module
