module LogCollector

  class State

    def initialize(config)
      @state = config.state
      @state_file = config.state_file
    end

    def update_state(state_update)
      $logger.debug { "updating state: #{state_update}" }

      state_update.each do |ev|
        @state[ev.path] ||= {}
        @state[ev.path]['dev'] = ev.dev
        @state[ev.path]['ino'] = ev.ino
        @state[ev.path]['pos'] = ev.pos
      end

      File.open(@state_file, 'w') { |file| file.write(@state.to_json) }
      $logger.info "saved state=#{@state}"
    end

  end # class State

end # module LogCollector
