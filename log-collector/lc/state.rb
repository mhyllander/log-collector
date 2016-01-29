module LogCollector

  class State
    include ErrorUtils

    def initialize(config)
      @state = config.state
      @state_file = config.state_file
      @state_file_new = @state_file + '.new'
      @active_files = {}

      @sync = Mutex.new
    end

    # called by a collector when a new log file is opened
    def active_file(path,fstat)
      $logger.info { "State: active file #{path} => #{fstat}" }

      @sync.synchronize { @active_files[path] = fstat }
    end

    # called by the sender when a request has been (partially) processed
    def update_state(state_update)
      $logger.debug { "State: updating state #{state_update}" }
      changed = false

      state_update.each do |file_id,state|
        path, _, _ = file_id.split(':')
        @sync.synchronize do
          fstat = @active_files[path]
          curstate = @state[path]
          $logger.debug { "State: #{path}     curstate=#{curstate}" }
          $logger.debug { "State: #{path} active fstat=#{fstat}" }
          $logger.debug { "State: #{path}     newstate=#{state}" }
          # Only update the state if this log event is from a currently active log file,
          # or if there is no current state for the path,
          # or if the current state happens to match this event
          if fstat && fstat['dev']==state['dev'] && fstat['ino']==state['ino'] ||
              curstate.nil? ||
              curstate['dev']==state['dev'] && curstate['ino']==state['ino']
            @state[path] = state
            changed = true
            $logger.debug { "State: updating state #{path} => #{state}" }
          end
        end
      end

      if changed
        begin
          json = @state.to_json
          File.open(@state_file_new, 'w') { |file| file.write(json) }
          File.rename @state_file_new, @state_file
          $logger.info "saved state=#{json}"
        rescue OutOfMemoryError
          raise
        rescue Exception=>e
          on_exception e
        end
      end
    end

  end # class State

end # module LogCollector
