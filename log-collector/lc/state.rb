module LogCollector

  class State
    include ErrorUtils

    def initialize(config)
      @state = config.state
      @state_file = config.state_file
      @state_file_new = @state_file + '.new'
    end

    def update_state(state_update)
      $logger.debug { "updating state: #{state_update}" }

      state_update.each do |path,state|
        @state[path] = state
      end

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

  end # class State

end # module LogCollector
