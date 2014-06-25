module LogCollector

  class Spooler
    attr_reader :spool_thread

    def initialize(config,spool_queue,state_mgr)
      @hostname = config.hostname
      @servers = config.servers
      @spool_queue = spool_queue
      @state_mgr = state_mgr

      @flush_interval = config.flush_interval
      @flush_size = config.flush_size
      @flush_mutex = Mutex.new
      @flush_thread = nil
      @send_thread = nil

      @buffer = []

      @delay = config.send_error_delay
      @tries = config.recv_tries
      @timeout = config.recv_timeout

      @zmq_context = ZMQ::Context.new(1)
      @poller = ZMQ::Poller.new
      @clientid = "C:%04X-%04X" % [(rand()*0x10000).to_i, (rand()*0x10000).to_i]

      @spool_thread = Thread.new do
        Thread.current['name'] = 'spooler'
        loop do
          begin
            client_sock
            process_events
          rescue Exception => e
            on_exception e, false
          end
        end
      end

      at_exit do
        @socket.close
      end
    end

    def process_events
      loop do
        $logger.debug "waiting on queue"
        ev = @spool_queue.pop
        @flush_mutex.synchronize do
          process_event ev
        end
      end
    end

    def process_event(ev)
      @buffer << ev
      if @buffer.size==1
        schedule_flush_buffer
      elsif @buffer.size >= @flush_size
        cancel_flush_buffer
        send_events
      end
    end

    def schedule_flush_buffer
      $logger.debug "schedule flush timer"
      # cancel scheduled flush
      cancel_flush_buffer
      # schedule new flush
      @flush_thread = Thread.new do
        begin
          Thread.current['name'] = 'spooler_flush'
          sleep @flush_interval
          @flush_mutex.synchronize do
            $logger.debug "flush spool buffer"
            send_events
          end
        rescue Exception => e
          on_exception e
        ensure
          @flush_mutex.synchronize { @flush_thread = nil }
        end
      end
    end

    def cancel_flush_buffer
      @flush_thread.kill if @flush_thread
    end

    def send_events
      # format the message to send
      msg = formatted_msg
      serial = Time.now.to_f.to_s
      msg['serial'] = serial
      cmsg = Zlib::Deflate.deflate(msg.to_json)

      # collect the state to save when the msg has been acked
      state_to_save = {}
      @buffer.each do |ev|
        state_to_save["#{ev.path}//#{ev.dev}//#{ev.ino}"] = ev
      end

      $logger.info { "send_events: ready to send #{msg['n']} events, serial=#{serial}" }

      # wait for running thread to finish
      @send_thread.join if @send_thread

      @send_thread = Thread.new(cmsg,serial,state_to_save.values) do |data,serial,state_update|
        begin
          Thread.current['name'] = 'spooler_send'
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
        rescue Exception => e
          on_exception e
        end
      end

      # empty the buffer before returning to processing the spool_queue
      @buffer = []
    end

    def formatted_msg
      {
        'host' => @hostname,
        'n' => @buffer.length,
        'events' => formatted_events
      }
    end

    def formatted_events
      @buffer.collect do |ev|
        {
          'ts' => ev.timestamp.to_f,
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

    def on_exception(exception,reraise=true)
      begin
        $logger.error "Exception raised: #{exception.inspect}. Using default handler in #{self.class.name}. Backtrace: #{exception.backtrace}"
      rescue
      end
      raise exception if reraise
    end
  end # class Spooler

end # module LogCollector
