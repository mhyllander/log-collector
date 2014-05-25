module LogCollector

  class Multiline
    def initialize(fileconfig,in_queue,out_queue)
      @fileconfig = fileconfig
      @in_queue = in_queue
      @out_queue = out_queue

      @multiline_re = fileconfig['multiline_re']
      @multiline_wait = fileconfig['multiline_wait']

      @internal_queue = []
      @held_ev = nil
      @flush_timer = nil
      schedule_process_next_line
    end

    def schedule_process_next_line
      @in_queue.pop do |ev|

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
        end

         if @out_queue.full?
          # cancel flush and wait for out_queue to not be full anymore
          cancel_flush_held_ev
          schedule_out_queue_monitor
        else
          # process the next line
          schedule_process_next_line
        end
      end
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

    def schedule_out_queue_monitor
      EM.next_tick do
        if @out_queue.full?
          $logger.debug "multiline(out_queue full)"
          # reschedule until out_queue is not full
          schedule_out_queue_monitor
        else
          $logger.debug "multiline(out_queue not full)"
          # flush the internal queue and resume processing
          @internal_queue.each {|ev| @out_queue.push ev}
          @internal_queue.clear
          schedule_process_next_line
          schedule_flush_held_ev
        end
      end
    end

  end # class Multiline
  
end # module LogCollector
