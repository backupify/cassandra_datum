require 'exception_helper/retry'

module CassandraDatum
  module CassandraRetry
    CASSANDRA_EXCEPTIONS = [
      ::Thrift::Exception,
      ::ThriftClient::NoServersAvailable,
    ].freeze

    class << self
      def cassandra_exceptions
        const_get(:CASSANDRA_EXCEPTIONS)
      end

      def default_options
        {
          :exceptions => cassandra_exceptions,
          :retry_count => 10,
          :retry_sleep => 0,
        }
      end
    end

    def self.included(base)
      base.send(:include, ExceptionHelper::Retry)
      base.extend(ClassMethods)
    end

    module ClassMethods
      def retry_cassandra_exceptions(opts = {}, &block)
        options = CassandraRetry.default_options.merge!(opts)
        exceptions = options.delete(:exceptions)
        retry_on_failure(*exceptions, options, &block)
      end
    end

    def retry_cassandra_exceptions(opts = {}, &block)
      self.class.retry_cassandra_exceptions(opts, &block)
    end
  end
end
