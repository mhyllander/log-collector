module LogCollector

  class State

    def initialize(config,state_queue)
      @state = config.state
      @state_file = config.state_file
      @state_queue = state_queue

      process_state
    end

    def process_state
      state_processor = proc do |events|
        $logger.debug "process state updates: #{events}"

        events.each do |ev|
          @state[ev.path] ||= {}
          @state[ev.path]['dev'] = ev.dev
          @state[ev.path]['inode'] = ev.ino
          @state[ev.path]['pos'] = ev.pos
        end

        File.open(@state_file, 'w') { |file| file.write(@state.to_json) }
        $logger.info "saved state=#{@state}"

        @state_queue.pop(state_processor)
      end

      @state_queue.pop(state_processor)
    end

  end # class State

end # module LogCollector
