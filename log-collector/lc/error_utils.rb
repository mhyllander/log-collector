class AssertionError < RuntimeError
end

module LogCollector
  module ErrorUtils

    def assert(&block)
      raise AssertionError unless yield
    end
    
    def on_exception(exception,reraise=true)
      begin
        $logger.error "Exception raised: #{exception.inspect}. Using default handler in #{self.class.name}. Backtrace: #{exception.backtrace}"
      rescue
      end
      raise exception if reraise
    end

  end
end

