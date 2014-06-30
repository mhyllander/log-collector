module LogCollector

  class Sender
    include ErrorUtils

    attr_reader :send_thread

    def initialize(config,request_queue)
      @hostname = config.hostname
      @servers = config.servers
      @request_queue = request_queue

      @state_mgr = State.new(config)

      @send_thread = nil
      @shutdown = false

      @delay = config.send_error_delay
      @tries = config.recv_tries
      @timeout = config.recv_timeout

      @zmq_context = ZMQ::Context.new(1)
      @poller = ZMQ::Poller.new
      @clientid = "C:%04X-%04X" % [(rand()*0x10000).to_i, (rand()*0x10000).to_i]

      schedule_send_requests

      at_exit do
        @socket.close
        @zmq_context.terminate
      end
    end

    def terminate
      @shutdown = true
      @request_queue.clear
      @request_queue.push :exit
    end

    def schedule_send_requests
      $logger.debug "schedule send requests"
      @send_thread = Thread.new do
        Thread.current['name'] = 'sender'
        loop do
          begin
            client_sock
            process_requests
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
      serial, msg, state_update = req
      msg['serial'] = serial
      data = Zlib::Deflate.deflate(msg.to_json)

      # don't send request if shutting down
      return if @shutdown

      $logger.info { "sending request serial=#{serial} (#{msg['n']} events)" }
      response = nil
      loop do
        begin

          rcvmsg = send data, serial
          if rcvmsg.length==2
            # [ serial, response ]
            response = JSON.parse(rcvmsg[1])
            if response.length==3 && response[0]=='ACK'
              # exit the loop, save state and terminate the thread when ACK is received
              break if response[1]==serial
              $logger.error "got ACK for wrong serial: expecting #{serial} received #{response[1]}"
            else
              $logger.error "got unexpected message: #{rcvmsg}"
            end
          else
            $logger.error "got unexpected message: #{rcvmsg}"
          end

        rescue Exception => e
          $logger.error "send/receive exception: #{e.message} rcvmsg=#{rcvmsg}"
          sleep @delay
        end
      end
      
      $logger.info { "<-- response from worker: #{response}" }
      
      # save state
      @state_mgr.update_state state_update
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
