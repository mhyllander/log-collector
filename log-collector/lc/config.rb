module LogCollector

  class Config
    Default_DeadTime = '24h'
    Default_QueueLowWaterMark = 1500
    Default_QueueHighWaterMark = 2000
    Default_MultilineWait = '2s'

    def initialize(configfile,statefile)
      json_config = File.read(configfile)
      @config = JSON.parse(json_config)

      @config['hostname'] ||= Socket.gethostname
      @config['queue_low'] ||= Default_QueueLowWaterMark
      @config['queue_high'] ||= Default_QueueHighWaterMark

      @config['files'].each do |path,fc|
        fc['startpos'] ||= -1 # default is to start at end of file
        fc['dead time'] = duration_for(fc['dead time'] || Default_DeadTime)
        fc['multiline_wait'] = duration_for(fc['multiline_wait'] || Default_MultilineWait)
      end

      if File.file? statefile
        json_state = File.read(statefile)
        @state = JSON.parse(json_state)

        @config['files'].each do |path,fc|

          # does the file exist?
          if File.file? path

            # get current file info
            file_info = get_file_info(path)

            # have we been watching this file before?
            if saved_state = @state[path]

              # is it the same file as before?
              if file_info[:dev]== saved_state['dev'] && file_info[:inode]==saved_state['inode']
                # same file
                if file_info[:size] < saved_state['pos']
                  # file truncated, start at beginning
                  fc['startpos'] = 0
                else
                  # resume at the saved position
                  fc['startpos'] = saved_state['pos']
                end
              else
                # not same file, probably rotated, start at beginning
                fc['startpos'] = 0
              end

            end # saved_state

          else # !file exists
            # file doesn't exist (yet), so start at beginning
            fc['startpos'] = 0
          end

        end # file loop

      end # statefile exists

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

    def queue_low
      @config['queue_low']
    end

    def queue_high
      @config['queue_high']
    end

    private
    def get_file_info(f)
      s= f.is_a?(String) ? File.stat(f) : f.stat
      {dev: s.dev, inode: s.ino, mtime: s.mtime, size: s.size}
    end

    private
    def duration_for(spec)
      ret = nil
      if((m = %r/^(\d+(?:\.\d+)?):(\d+(?:\.\d+)?):(\d+(?:\.\d+)?)$/.match(spec.to_s)))
        _, h, m, s, _ = m.to_a
        h, m, s = Float(h), Float(m), Float(s)
        ret = (h * 60 * 60) + (m * 60) + (s)
      else
        pat = %r/(\d+(?:\.\d+)?)\s*([sSmMhHdDwWyY][^\d]*)?/
        begin
          "#{ spec }".scan(pat) do |m|
            n = Float m[0]
            unit = m[1]
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
