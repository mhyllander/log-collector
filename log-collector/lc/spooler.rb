module LogCollector

  class Spooler
    def initialize(config,spool_queue,state_queue)
      @hostname = config.hostname
      @servers = config.servers
      @spool_queue = spool_queue
      @state_queue = state_queue

      @flush_interval = config.flush_interval
      @flush_size = config.flush_size
      @flush_timer = nil
      @buffer = []
      @sendbuf = nil

      @delay = config.send_error_delay
      @retries = config.send_retries
      @timeout = config.recv_timeout

      @zmq_context = ZMQ::Context.new(1)
      @poller = ZMQ::Poller.new
      @clientid = "C:%04X-%04X" % [(rand()*0x10000).to_i, (rand()*0x10000).to_i]
      client_sock
      schedule_process_event

      at_exit do
        @socket.close
      end
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
      @flush_timer = EM::Timer.new(@flush_interval) do
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
          # collect the accumulated final state
          final_events = {}
          @sendbuf.each do |ev|
            final_events["#{ev.path}//#{ev.dev}//#{ev.inode}"] = ev
          end

          msg = formatted_msg
          serial = Time.now.to_f.to_s
          msg['serial'] = serial
          cmsg = Zlib::Deflate.deflate(msg.to_json)
          response = nil

          $logger.debug "send #{msg['n']} events to worker, serial=#{serial}"
          
          loop do
            begin
              rcvmsg = send cmsg
              if rcvmsg.length==1
                response = JSON.parse(rcvmsg[0])
                if response.length==3 && response[0]=='ACK'
                  break if response[1]==serial
                  $logger.debug "got ack for wrong serial: expecting #{serial} received #{response[1]}"
                else
                  $logger.debug "got unexpected message: #{rcvmsg}"
                end
              else
                $logger.debug "got unexpected message: #{rcvmsg}"
              end
                
            rescue Exception => e
              $logger.debug "send/receive exception: #{e.message} rcvmsg=#{rcvmsg}"
              sleep @delay
            end
          end

          # save state
          @state_queue.push final_events.values
          
          response
        end
        sendcb = proc do |response|
          $logger.debug "worker response: #{response}"
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
        'host' => @hostname,
        'n' => @sendbuf.length,
        'events' => formatted_events
      }
    end

    def formatted_events
      @sendbuf.collect do |ev|
        {
          'ts' => ev.timestamp.utc.strftime('%FT%T.%LZ'),
          'file' => ev.path,
          'msg' => ev.line,
          'flds' => ev.fields
        }
      end
    end

    def client_sock
      $logger.debug "create spool socket"
      @socket = @zmq_context.socket(ZMQ::REQ)
      @socket.setsockopt(ZMQ::LINGER, 0)
      @socket.setsockopt(ZMQ::IDENTITY, @clientid)
      @poller.register_readable @socket
      @servers.each do |addr|
        $logger.debug "bind to #{addr}"
        @socket.connect addr
      end
    end

    def send(message)
      @retries.times do |tries|
        raise("send: send failed") unless @socket.send_string(message)
        while @poller.poll(@timeout*1000) > 0
          @poller.readables.each do |readable|
            if readable==@socket
              @socket.recv_strings msgs=[]
              return msgs
            end
          end
        end
        @poller.deregister_readable @socket
        @socket.close
        client_sock
      end
      raise 'send: server down'
    end

  end # class Spooler

end # module LogCollector
