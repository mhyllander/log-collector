# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require 'json'
require 'zlib'

# Read events from a queue over a 0MQ socket.
#
# You need to have the 0mq 4.0.x library installed to be able to use
# this input plugin.
#
# The default settings will connect to a queue at tcp://127.0.0.1:5560,
# waiting for client input.
#
class LogStash::Inputs::LogCollector < LogStash::Inputs::Base

  config_name "logcollector"
  milestone 2

  default :codec, "plain"

  # 0mq socket address to connect or bind
  # Please note that `inproc://` will not work with logstash
  # as each we use a context per thread.
  # By default, inputs bind/listen
  # and outputs connect
  config :address, :validate => :array, :default => ["tcp://127.0.0.1:5560"]

  # 0mq topology
  # The default logstash topologies work as follows:
  # * reqrep - inputs are rep, and can be either server or client mode (bind or connect)
  #
  # If the predefined topology flows don't work for you,
  # you can change the 'mode' setting
  config :topology, :validate => ["queue"], :default => "queue"

  # mode
  # server mode binds/listens
  # client mode connects
  config :mode, :validate => ["client"], :default => "client"

  # sender
  # overrides the sender to 
  # set the source of the event
  # default is "lc+topology://type/"
  #config :sender, :validate => :string

  # 0mq socket options
  # This exposes zmq_setsockopt
  # for advanced tuning
  # see http://api.zeromq.org/2-1:zmq-setsockopt for details
  #
  # This is where you would set values like:
  # ZMQ::HWM - high water mark
  # ZMQ::IDENTITY - named queues
  # ZMQ::SWAP_SIZE - space for disk overflow
  #
  # example: sockopt => ["ZMQ::HWM", 50, "ZMQ::IDENTITY", "my_named_queue"]
  config :sockopt, :validate => :hash

  # Ping interval in seconds
  config :ping_interval, :validate => :number, :default => 1

  # Ping liveness (3..5 is reasonable)
  config :ping_liveness, :validate => :number, :default => 3

  # SSL certificate to use.
  #config :ssl_certificate, :validate => :path, :required => false

  # SSL key to use.
  #config :ssl_key, :validate => :path, :required => false

  # SSL key passphrase to use.
  #config :ssl_key_passphrase, :validate => :password

  # LOGSTASH-400
  # see https://github.com/chuckremes/ffi-rzmq/blob/master/lib/ffi-rzmq/socket.rb#L93-117
  STRING_OPTS = %w{IDENTITY SUBSCRIBE UNSUBSCRIBE}

  PPP_READY = "\x01" # Signals worker is ready
  PPP_PING  = "\x02" # Signals queue ping
  PPP_PONG  = "\x03" # Signals worker pong

  # Back-off for connection errors
  INTERVAL_INIT = 1
  INTERVAL_MAX  = 32

  public
  def register
    require "ffi-rzmq"
    @context = ZMQ::Context.new

    @logger.info("Starting log-collector input listener", :address => "#{@address}", :mode => "#{@mode}")

    case @topology
    when "queue"
      @zmq_const = ZMQ::DEALER
    end # case socket_type

    @sockopt ||= {}
    @sockopt['ZMQ::IDENTITY'] ||= "L:%04X-%04X" % [(rand()*0x10000).to_i, (rand()*0x10000).to_i]

  end # def register

  def teardown
    error_check(@zsocket.close, "while closing the zmq socket")
  end # def teardown

  def context
    @context
  end

  def server?
    @mode == "server"
  end # def server?

  def run(output_queue)
    run_worker do |data|
      begin
        event = LogStash::Event.new(data)
        decorate(event)
        output_queue << event
      rescue LogStash::ShutdownSignal
        # shutdown
        return
      rescue => e
        @logger.debug("[log-collector] Error", :subscriber => @zsocket, :exception => e)
        retry
      end # begin
    end
  end # def run

  private
  def build_source_string
    id = @address.first.clone
  end

  # Helper function that returns a new configured socket
  # connected to the Paranoid Pirate queue
  def worker_socket(context, poller)
    worker = context.socket @zmq_const

    if @sockopt
      setopts(worker, @sockopt)
    end

    @address.each do |addr|
      setup(worker, addr)
    end

    poller.register_readable worker
    worker.send_string PPP_READY
    worker
  end

  def run_worker(&block)
    poller = ZMQ::Poller.new
    @zsocket = worker_socket context, poller

    begin
      liveness = @ping_liveness
      interval = INTERVAL_INIT

      loop do
        
        while poller.poll(@ping_interval*1000) > 0
          poller.readables.each do |readable|
            if readable==@zsocket
              
              # Get message
              # - 3-part envelope + content -> request
              # - 1-part PING -> ping
              @zsocket.recv_strings msgs = []
              if msgs.length==2
                clientid = msgs[0]
                @logger.debug "[log-collector] got msg client=#{clientid} len=#{msgs.length}"
                if msgs[1]==PPP_PING
                  @logger.debug "[log-collector] recv queue ping, send pong"
                  @zsocket.send_string PPP_PONG
                end
              elsif msgs.length>=4
                # msgs[0]: empty delimiter
                # msgs[1]: client id
                # msgs[2]: empty delimiter
                # msgs[3]: request
                clientid = msgs[1]
                request = msgs[3]
                @logger.debug "[log-collector] got msg client=#{clientid} len=#{msgs.length} msgs=#{msgs[0..-2]}"

                # handle request by feeding the log events to logstash
                batch = JSON.parse(Zlib::Inflate.inflate(request))
                host = batch['host'].force_encoding(Encoding::UTF_8)
                batch['events'].each do |ev|
                  data = {
                    '@timestamp' => ev['ts'],
                    'host' => host,
                    'file' => ev['file'].force_encoding(Encoding::UTF_8),
                    'message' => ev['msg'].force_encoding(Encoding::UTF_8)
                  }
                  ev['flds'].each {|f,v| data[f] = v.force_encoding(Encoding::UTF_8)}
                  block.call(data)
                end

                # send an ACK back to client when finished
                @zsocket.send_strings ['', clientid, '', ['ACK',batch['serial'],batch['n']].to_json]
              else
                @logger.error "[log-collector] Invalid message: #{msgs}"
              end

              liveness = @ping_liveness
              interval = INTERVAL_INIT

            end # if readable==@zsocket

          end # poller.readables.each
        end # while poller.poll

        liveness -= 1
        if liveness==0
          @logger.debug "[log-collector] Queue failure (no pings or requests)"
          @logger.debug "[log-collector] Reconnecting in #{interval}s"

          poller.deregister_readable @zsocket
          @zsocket.setsockopt ZMQ::LINGER, 0
          @zsocket.close

          sleep interval
          interval *= 2 if interval < INTERVAL_MAX

          @zsocket = worker_socket context, poller
          liveness = @ping_liveness
        end

      end # loop
    rescue LogStash::ShutdownSignal
      # shutdown
      return
    rescue => e
      @logger.debug("[log-collector] Error", :subscriber => @zsocket, :exception => e)
      retry
    ensure
      @zsocket.close
    end
  end

  def setup(socket, address)
    if server?
      error_check(socket.bind(address), "binding to #{address}")
    else
      error_check(socket.connect(address), "connecting to #{address}")
    end
    @logger.info("[log-collector] #{server? ? 'bound' : 'connecting'}", :address => address)
  end

  def error_check(rc, doing)
    unless ZMQ::Util.resultcode_ok?(rc)
      @logger.error("log-collector error while #{doing}", { :error_code => rc })
      raise "log-collector error while #{doing}"
    end
  end # def error_check

  def setopts(socket, options)
    options.each do |opt,value|
      sockopt = opt.split('::')[1]
      option = ZMQ.const_defined?(sockopt) ? ZMQ.const_get(sockopt) : ZMQ.const_missing(sockopt)
      unless STRING_OPTS.include?(sockopt)
        begin
          Float(value)
          value = value.to_i
        rescue ArgumentError
          raise "[log-collector] #{sockopt} requires a numeric value. #{value} is not numeric"
        end
      end # end unless
      error_check(socket.setsockopt(option, value),
              "while setting #{opt} == #{value}")
    end # end each
  end # end setopts

end # class LogStash::Inputs::LogCollector
