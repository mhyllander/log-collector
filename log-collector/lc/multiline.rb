module LogCollector

  class Multiline
    include ErrorUtils

    def initialize(fileconfig,in_queue,out_queue)
      @fileconfig = fileconfig
      @in_queue = in_queue
      @out_queue = out_queue

      @multiline_re = Regexp.new(fileconfig['multiline_re'])
      @multiline_invert = fileconfig['multiline_invert']
      @multiline_wait = fileconfig['multiline_wait']

      @held_ev = nil

      @multiline_thread = Thread.new do
        Thread.current['name'] = 'multiline'
        loop do
          begin
            process_lines
          rescue Exception => e
            on_exception e, false
          end
        end
      end

      @flush_thread = Thread.new do
        Thread.current['name'] = 'multiline/flush'
        loop do
          begin
            schedule_flush_held_ev
          rescue Exception => e
            on_exception e, false
          end
        end
      end
    end

    def process_lines
      loop do
        $logger.debug "waiting on queue"
        ev = @in_queue.pop
        # flush the held ev when the special symbol :flush is received
        if ev==:flush
          if @held_ev
            $logger.debug { "multiline(flush held): @held_ev=#{@held_ev}" }
            @out_queue.push(@held_ev)
            @held_ev = nil
          end
        else
          reset_flush
          process_line ev
        end
      end
    end

    def process_line(ev)
      # Lines the match @multiline_re are continuation lines that will be appended to the previous line.
      # If @multiline_invert is true, then lines that don't match @multiline_re are continuation lines.
      # The "!= @multiline_invert" clause is a clever/sneaky way of inverting the result of the match.
      if !@multiline_re.match(ev.line).nil? != @multiline_invert
        if @held_ev.nil?
          @held_ev = ev
          $logger.debug { "multiline(missing previous): @held_ev=#{@held_ev}" }
        else
          @held_ev.append ev
          $logger.debug { "multiline(append): @held_ev=#{@held_ev}" }
        end
      else
        # not a continuation line, so send any held event
        $logger.debug { "multiline(enqueue held): @held_ev=#{@held_ev}" } if @held_ev
        @out_queue.push(@held_ev) if @held_ev
        @held_ev = ev
      end
    end

    def schedule_flush_held_ev
      loop do
        # loop until a full @multiline_wait has been slept
        slept = sleep(@multiline_wait) until slept==@multiline_wait
        # flush any held event
        @in_queue.push :flush
      end
    end

    # restart the flush timer
    def reset_flush
      @flush_thread.wakeup
    end

  end # class Multiline
  
end # module LogCollector
