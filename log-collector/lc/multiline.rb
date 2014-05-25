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
      line_processor = proc do |ev|
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

        if @out_queue.full?
          # cancel flush and wait for out_queue to be pushable
          cancel_flush_held_ev
          @out_queue.callback do
            $logger.debug "multiline(out_queue not full)"
            # resume line_processing
            @in_queue.pop(line_processor)
          end
        else
          # process next line
          @in_queue.pop(line_processor)
        end
      end # proc

      @in_queue.pop(line_processor)
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
