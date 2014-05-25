module LogCollector

  class Spooler
    Default_FlushTime = 1
    Default_FlushSize = 1000

    def initialize(hostname,servers,spool_queue)
      @spool_queue = spool_queue
      @hostname = hostname

      @flush_time = Default_FlushTime
      @flush_size = Default_FlushSize
      @flush_timer = nil
      @buffer = []
      @sendbuf = nil

      # If using em-zeromq: 
      #@zmq_context = EM::ZeroMQ::Context.new(1)

      # If using ffi-rzmq directly:
      @zmq_context = ZMQ::Context.new(1)

      @spool_socket = @zmq_context.socket(ZMQ::REQ)
      servers.each do |addr|
        $logger.debug "bind to #{addr}"
        @spool_socket.connect addr
      end
      
      schedule_process_event
    end

    def schedule_process_event
      @spool_queue.pop do |ev|
        #$logger.debug "process event: #{ev}"
        @buffer << ev

        if @buffer.length >= @flush_size
          send_events
        else
          schedule_flush_buffer if @buffer.length==1
          schedule_process_event
        end
      end
    end

    def schedule_flush_buffer
      @flush_timer.cancel if @flush_timer
      $logger.debug "schedule flush timer"
      @flush_timer = EM::Timer.new(@flush_time) do
        $logger.debug "flush timer"
        send_events
      end
    end

    def schedule_wait_for_sendop
      EM.next_tick do
        if @sendbuf.nil?
          send_events
        else
          schedule_wait_for_sendop
        end
      end
    end

    def send_events
      $logger.debug "send_events: @buffer=#{!@buffer.empty?} @sendbuf=#{!@sendbuf.nil?}"
      # check if waiting for a reply to previous send
      unless @sendbuf.nil?
        # already sending a batch and waiting for the callback
        $logger.debug "wait for previous sendop to finish"
        schedule_wait_for_sendop
        return
      end
      # ready to send
      unless @buffer.empty?
        @sendbuf = @buffer
        @buffer = []

        sendop = proc do
          $logger.debug("send #{@sendbuf.length} events to logstash")
          msg = formatted_msg.to_json
          compress = Zlib::Deflate.deflate(msg)

          # If using em-zeromq: 
          #@spool_socket.send_msg(msg)
          #@spool_socket.on(:message) do |part|
          #  puts part.copy_out_string
          #  part.close
          #end

          # If using ffi-rzmq directly:
          @spool_socket.send_string(compress)
          @spool_socket.recv_string(rcvmsg = '')
          rcvmsg
        end
        sendcb = proc do |response|
          $logger.debug "got response: #{response}"
          @sendbuf = nil
        end

        EM.defer(sendop,sendcb)
      end

      # after scheduling sending of events, resume processing log events
      $logger.debug "continue processing events"
      schedule_process_event
    end

    def formatted_msg
      {
        'hostname' => @hostname,
        'n' => @sendbuf.length,
        'events' => formatted_events
      }
    end

    def formatted_events
      @sendbuf.collect do |ev|
        {
          'path' => ev.path,
          'msg' => ev.line,
          'fields' => ev.fields
        }
      end
    end

  end

end
