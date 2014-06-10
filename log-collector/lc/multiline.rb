module LogCollector

  class Multiline
    def initialize(fileconfig,in_queue,out_queue)
      @fileconfig = fileconfig
      @in_queue = in_queue
      @out_queue = out_queue

      @multiline_re = fileconfig['multiline_re']
      @multiline_wait = fileconfig['multiline_wait']

      @held_ev = nil
      @flush_timer = nil
      process_lines
    end

    def process_lines
      # Note that LimitedQueue.pop does not block when we are on the reactor thread and there is data to read.
      @in_queue.pop do |ev|
        process_line ev
        read_in_queue
        if @out_queue.full?
          # cancel flush and wait for out_queue to be pushable
          cancel_flush_held_ev
          @out_queue.callback do
            $logger.debug "multiline(out_queue not full)"
            # resume line_processing
            EM.next_tick { process_lines }
          end
        else
          # process next line
          EM.next_tick { process_lines }
        end
      end
    end

    def read_in_queue
      while @in_queue.size > 0 && !@out_queue.full?
        @in_queue.pop do |ev|
          process_line ev
        end
      end
    end

    def process_line(ev)
      if @multiline_re

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
        schedule_flush_held_ev
        
      else # not @multiline_re
        $logger.debug "multiline(disabled): ev=#{ev}"
        @out_queue.push ev
      end # not @multiline_re
    end

    def schedule_flush_held_ev
      cancel_flush_held_ev
      @flush_timer = EM::Timer.new(@multiline_wait) do
        $logger.debug("multiline(flush held): @held_ev=#{@held_ev}") if @held_ev
        @out_queue.push(@held_ev) if @held_ev
        @held_ev = nil
        @flush_timer = nil
      end
    end

    def cancel_flush_held_ev
      @flush_timer.cancel if @flush_timer
      @flush_timer = nil
    end

  end # class Multiline
  
end # module LogCollector
