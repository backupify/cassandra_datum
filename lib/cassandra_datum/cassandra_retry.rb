require 'exception_helper/retry'

module CassandraDatum
  module Exceptions
    unless Object.const_defined?('::Cassandra::Error')
      class Cassandra::Error < StandardError
        def initialize(message)
          super
          @message = message
        end

        attr_reader :message
      end
    end

    # This code is left here for documentation sake. We used to get TransportException
    # from Thrift, but there is no equivalent with our CQL driver.
    ##################################
    # unless Object.const_defined?('Thrift::TransportException')
    #   class Thrift::TransportException < Thrift::Exception
    #     UNKNOWN = 0
    #     NOT_OPEN = 1
    #     ALREADY_OPEN = 2
    #     TIMED_OUT = 3
    #     END_OF_FILE = 4

    #     attr_reader :type

    #     def initialize(type = UNKNOWN, message = nil)
    #       super(message)
    #       @type = type
    #     end
    #   end
    # end

    unless Object.const_defined?('::ThriftClient::NoServersAvailable')
      class ::ThriftClient::NoServersAvailable < Cassandra::Error
      end
    end

    unless Object.const_defined?('::CassandraThrift::TimedOutException')
      class ::CassandraThrift::TimedOutException < Cassandra::Error
      end
    end
  end

  module CassandraRetry
    CASSANDRA_EXCEPTIONS = [
        ::Cassandra::Errors::NoHostsAvailable,
        ::Cassandra::Errors::UnavailableError,
        ::Cassandra::Errors::TimeoutError,
        ::Cassandra::Errors::ReadTimeoutError,
        ::Cassandra::Errors::WriteTimeoutError,
        ::Cassandra::Error, ThreadError,
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
