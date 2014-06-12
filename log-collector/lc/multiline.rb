module LogCollector

  class Multiline

    attr_reader :filter_thread

    def initialize(fileconfig,in_queue,out_queue)
      @fileconfig = fileconfig
      @in_queue = in_queue
      @out_queue = out_queue

      @multiline_re = fileconfig['multiline_re']
      @multiline_wait = fileconfig['multiline_wait']

      @held_ev = nil
      @ev_counter = 0
      @flush_mutex = Mutex.new
      @flush_thread = nil

      @filter_thread = Thread.new do
        Thread.current['name'] = 'filter'
        begin
          process_lines
        rescue Exception => e
          $logger.error "exception raised: #{e}"
        end
      end

      @flush_thread = Thread.new do
        begin
          Thread.current['name'] = 'filter_flush'
          schedule_flush_held_ev
        rescue Exception => e
          $logger.error "exception raised: #{e}"
        end
      end
    end

    def process_lines
      loop do
        $logger.debug "waiting on queue"
        ev = @in_queue.pop
        @ev_counter += 1
        @flush_mutex.synchronize do
          process_line ev
        end
      end
    end

    def process_line(ev)
      if ev.line =~ /#{@multiline_re}/
        if @held_ev.nil?
          @held_ev = ev
          $logger.debug "multiline(missing previous): @held_ev=#{@held_ev}"
        else
          @held_ev.append ev
          $logger.debug "multiline(append): @held_ev=#{@held_ev}"
        end
      else
        # not a continuation line, so send any held event
        $logger.debug("multiline(enqueue held): @held_ev=#{@held_ev}") if @held_ev
        @out_queue.push(@held_ev) if @held_ev
        @held_ev = ev
      end
    end

    def schedule_flush_held_ev
      loop do
        # save current counter before sleeping
        counter = @ev_counter
        sleep @multiline_wait
        # check if no new events were received while we slept
        if counter == @ev_counter && @held_ev
          @flush_mutex.synchronize do
            $logger.debug("multiline(flush held): @held_ev=#{@held_ev}")
            @out_queue.push(@held_ev)
            @held_ev = nil
          end
        end
      end
    end

  end # class Multiline
  
end # module LogCollector
