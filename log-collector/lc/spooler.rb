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

      @shutdown = false
      @buffer = []
      @state_to_save = {}

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
          $logger.debug "flushing spool buffer"
          send_events
        when :exit
          # Exit thread when the special symbol :exit is received
          Thread.exit
        else
          process_event ev
        end
      end
    end

    def process_event(ev)
      @buffer << ev
      @state_to_save["#{ev.path}//#{ev.dev}//#{ev.ino}"] = ev
      if @buffer.size >= @flush_size
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
      return if @buffer.empty?

      # format the message to send
      msg = formatted_msg
      serial = Time.now.to_f.to_s
      state_update = @state_to_save.values

      # empty the buffers before returning to processing the event_queue
      @buffer.clear
      @state_to_save.clear

      # don't enqueue request if shutting down
      return if @shutdown

      $logger.debug { "enqueue request serial=#{serial} (#{msg['n']} events)" }
      @request_queue.push [serial, msg, state_update]
    end

    def formatted_msg
      {
        'host' => @hostname,
        'n' => @buffer.length,
        'events' => formatted_events
      }
    end

    def formatted_events
      @buffer.collect do |ev|
        {
          'ts' => ev.timestamp.to_f,
          'file' => ev.path,
          'msg' => ev.line,
          'flds' => ev.fields
        }
      end
    end

  end # class Spooler

end # module LogCollector
