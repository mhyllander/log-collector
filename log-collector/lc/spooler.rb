module LogCollector

  class Spooler
    include ErrorUtils

    def initialize(config,event_queue,request_queue)
      @hostname = config.hostname
      @servers = config.servers
      @event_queue = event_queue
      @request_queue = request_queue

      @flush_interval = config.flush_interval
      @flush_size = config.flush_size
      @flush_thread = nil
      @delayed_flush = false

      @shutdown = false
      @request = Request.new(@hostname)

      schedule_process_events
      schedule_flush
    end

    def terminate
      $logger.info "terminating spooler"
      @shutdown = true
      @event_queue.clear
      @event_queue.push :exit
    end

    def schedule_process_events
      $logger.debug "schedule process events"
      @spool_thread = Thread.new do
        Thread.current['name'] = 'spooler'
        Thread.current.priority = 1
        loop do
          begin
            process_events
          rescue OutOfMemoryError
            abort "Spooler: exiting because of java.lang.OutOfMemoryError"
          rescue Exception => e
            on_exception e, false
          end
        end
      end
    end

    def process_events
      loop do
        $logger.debug "waiting on queue"
        ev = @event_queue.pop
        case ev
        when :flush
          # flush buffer when the special symbol :flush is received
          if @request_queue.empty?
            $logger.debug "flushing spool buffer"
            send_events
          else
            $logger.debug "delaying flush until the request queue is empty"
            @delayed_flush = true
          end
        when :exit
          # Exit thread when the special symbol :exit is received
          Thread.exit
        else
          process_event ev
        end
      end
    end

    def process_event(ev)
      new_state = @request.empty? ? {} : @request.last_event.accumulated_state.clone
      new_state[ev.path] = {dev: ev.dev, ino: ev.ino, pos: ev.pos}
      ev.accumulated_state = new_state
      @request << ev
      if @request.length >= @flush_size || @delayed_flush && @request_queue.empty?
        reset_flush
        send_events
      end
    end

    def schedule_flush
      $logger.debug "schedule flush timer"
      @flush_thread = Thread.new do
        Thread.current['name'] = 'spooler/flush'
        loop do
          begin
            # loop until a full @flush_interval has been slept
            slept = sleep(@flush_interval) until slept==@flush_interval
            # flush any buffered events
            $logger.debug "schedule flush spool buffer"
            @event_queue.push :flush
          rescue OutOfMemoryError
            abort "Spooler: exiting because of java.lang.OutOfMemoryError"
          rescue Exception => e
            on_exception e, false
          end
        end
      end # loop
    end

    # This allows us to restart the flush timer by calling wakeup on the
    # thread. The sleep starts over until it has slept a full
    # @flush_interval.
    def reset_flush
      @flush_thread.wakeup
    end

    def send_events
      @delayed_flush = false
      return if @request.empty?

      # the request to send
      req = @request

      # create new request before returning to processing the event_queue
      @request = Request.new(@hostname)

      # don't enqueue request if shutting down
      return if @shutdown

      $logger.debug { "enqueue request=#{req}" }
      @request_queue.push req
    end

  end # class Spooler

end # module LogCollector
