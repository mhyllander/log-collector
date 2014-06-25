module LogCollector

  class Multiline

    attr_reader :filter_thread

    def initialize(fileconfig,in_queue,out_queue)
      @fileconfig = fileconfig
      @in_queue = in_queue
      @out_queue = out_queue

      @multiline_re = Regexp.new(fileconfig['multiline_re'])
      @multiline_invert = fileconfig['multiline_invert']
      @multiline_wait = fileconfig['multiline_wait']

      @held_ev = nil
      @ev_counter = 0
      @flush_mutex = Mutex.new
      @flush_thread = nil

      @filter_thread = Thread.new do
        Thread.current['name'] = 'filter'
        loop do
          begin
            process_lines
          rescue Exception => e
            on_exception e, false
          end
        end
      end

      @flush_thread = Thread.new do
        Thread.current['name'] = 'filter_flush'
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
        @ev_counter += 1
        @flush_mutex.synchronize do
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
        # save current counter before sleeping
        counter = @ev_counter
        sleep @multiline_wait
        # check if no new events were received while we slept
        if counter == @ev_counter && @held_ev
          @flush_mutex.synchronize do
            $logger.debug { "multiline(flush held): @held_ev=#{@held_ev}" }
            @out_queue.push(@held_ev)
            @held_ev = nil
          end
        end
      end
    end

    def on_exception(exception,reraise=true)
      begin
        $logger.error "Exception raised: #{exception.inspect}. Using default handler in #{self.class.name}. Backtrace: #{exception.backtrace}"
      rescue
      end
      raise exception if reraise
    end
  end # class Multiline
  
end # module LogCollector
