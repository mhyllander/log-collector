module EventMachine

  class LimitedQueue < Queue
    attr_accessor :low_water_mark
    attr_accessor :high_water_mark

    include EM::Deferrable

    def initialize
      super
      @low_water_mark = @high_water_mark = -1
      @full = false
    end

    # Pop items off the queue, running the block on the reactor thread. The pop
    # will not happen immediately, but at some point in the future, either in
    # the next tick, if the queue has data, or when the queue is populated.
    #
    # @return [NilClass] nil
    def pop(*a, &b)
      cb = EM::Callback(*a, &b)
      EM.schedule do
        if @items.empty?
          @popq << cb
        else
          cb.call @items.shift
          adjust_full_state
        end
      end
      nil # Always returns nil
    end

    # Push items onto the queue in the reactor thread. The items will not appear
    # in the queue immediately, but will be scheduled for addition during the
    # next reactor tick.
    def push(*items)
      EM.schedule do
        @items.push(*items)
        @popq.shift.call @items.shift until @items.empty? || @popq.empty?
        adjust_full_state
      end
    end
    alias :<< :push

    # Return true if the queue is currently full. It will become full when the size exceeds the
    # high water mark, and will remain full until size is below the low water mark.
    #
    # @return [Boolean]
    def full?
      @full
    end

    # check if full state has changed
    private
    def adjust_full_state
      return if @high_water_mark==-1
      @low_water_mark = @high_water_mark if @low_water_mark==-1
      if @full
        if size <= @low_water_mark
          @full = false
          self.succeed # notify callbacks that the queue can now be pushed to
        end
      else
        @full = true if size > @high_water_mark
      end
    end
  end # LimitedQueue
end # EventMachine
