module LogCollector

  class Sender
    include ErrorUtils

    attr_reader :send_thread

    def initialize(config,clientid,request_queue)
      @hostname = config.hostname
      @servers = config.servers
      @clientid = clientid || ("C:%04X-%04X" % [(rand()*0x10000).to_i, (rand()*0x10000).to_i])
      @request_queue = request_queue

      @state_mgr = State.new(config)

      @send_thread = nil
      @shutdown = false

      @delay = config.send_error_delay
      @tries = config.recv_tries
      @timeout = config.recv_timeout

      @zmq_context = ZMQ::Context.new(1)
      @poller = ZMQ::Poller.new

      schedule_send_requests

      at_exit do
        @socket.close
        @zmq_context.terminate
      end
    end

    def terminate
      $logger.info "terminating sender"
      @shutdown = true
      @request_queue.clear
      @request_queue.push :exit
    end

    def schedule_send_requests
      $logger.debug "schedule send requests"
      @send_thread = Thread.new do
        Thread.current['name'] = 'sender'
        Thread.current.priority = 0
        loop do
          begin
            client_sock
            process_requests
          rescue OutOfMemoryError
            abort "Sender: exiting because of java.lang.OutOfMemoryError"
          rescue Exception => e
            on_exception e, false
          end
        end
      end
    end

    def process_requests
      loop do
        $logger.debug "waiting on queue"
        req = @request_queue.pop
        # Exit the thread when the special symbol :exit is received on the queue
        Thread.exit if req==:exit
        # process the request
        process_request req
      end
    end

    def process_request(req)
      first_unprocessed = 0
      while first_unprocessed < req.length
        processed = send_request req, first_unprocessed
        # update event pointer
        first_unprocessed += processed
        # save state if any events were processed
        @state_mgr.update_state req[first_unprocessed-1].accumulated_state if processed > 0
        # return if shutting down
        return if @shutdown
      end
    end

    def send_request(req,first_unprocessed)
      # don't send request if shutting down
      return 0 if @shutdown

      msg = req.formatted_msg first_unprocessed
      serial = msg['serial']
      data = Zlib::Deflate.deflate(msg.to_json)

      $logger.info { "sending request serial=#{serial} (#{msg['n']} events)" }
      send_time = Time.now.to_f
      response = nil
      loop do
        begin

          rcvmsg = send data, serial
          if rcvmsg.length==2
            # [ serial, response ]
            if rcvmsg[0]==serial
              response = JSON.parse(rcvmsg[1])
              # [ 'ACK', processed_events ]
              if response.length==2 && response[0]=='ACK'
                # exit the loop when ACK is received
                break
              else
                $logger.error "got unexpected response: #{response}"
              end
            else
              $logger.error "got msg for wrong serial: expecting #{serial} received #{rcvmsg[0]}"
            end
          else
            $logger.error "got unexpected message: #{rcvmsg}"
          end

        rescue OutOfMemoryError
          abort "Sender: exiting because of java.lang.OutOfMemoryError"
        rescue Exception => e
          $logger.error "send/receive exception: #{e.message} rcvmsg=#{rcvmsg}"
          sleep @delay
        end
      end
      
      $logger.info { "<-- response from worker: #{response} roundtrip_time=%.2fs" % [Time.now.to_f - send_time] }
      # return number of processed events so we can re-send unprocessed events
      response[1]
    end

    def client_sock
      $logger.debug "create spool socket"
      @socket = @zmq_context.socket(ZMQ::REQ)
      @socket.setsockopt(ZMQ::LINGER, 0)
      @socket.setsockopt(ZMQ::IDENTITY, @clientid)
      @poller.register_readable @socket
      @servers.each do |addr|
        $logger.debug { "bind to #{addr}" }
        @socket.connect addr
      end
    end

    def client_sock_reopen
      $logger.debug "close spool socket"
      @poller.deregister_readable @socket
      @socket.close
      client_sock
    end

    def send(message,serial)
      $logger.info { "--> request client #{@clientid} to worker, serial=#{serial}" }
      @tries.times do |try|
        unless @socket.send_strings [serial, message]
          client_sock_reopen
          raise 'send failed'
        end
        while @poller.poll(@timeout*1000) > 0
          @poller.readables.each do |readable|
            if readable==@socket
              @socket.recv_strings msgs=[]
              return msgs
            end
          end
        end
        client_sock_reopen
      end
      raise 'no response from worker'
    end

  end # class Sender

end # module LogCollector
