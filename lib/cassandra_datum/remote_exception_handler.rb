require "remotely_exceptional/handlers/prioritized_handler"

module CassandraDatum::RemoteExceptionHandler
  include RemotelyExceptional::Handler
  include RemotelyExceptional::Handlers::PrioritizedHandler

  CASSANDRA_EXCEPTIONS = [
    ::Thrift::Exception,
    ::ThriftClient::NoServersAvailable,
  ].freeze

  class DefaultThriftExceptionHandler
    include RemotelyExceptional::Handler

    @matcher = lambda do |exception|
      CASSANDRA_EXCEPTIONS.any? { |cass_ex| cass_ex === exception }
    end

    def handle
      context[:retry_count] ||= 1
      if context[:retry_count] < retry_count
        context[:retry_count] += 1
        sleep(retry_sleep) if retry_sleep > 0
        :retry
      else
        :raise
      end
    end

    def retry_count
      10
    end

    def retry_sleep
      0
    end
  end

  # Register the default Thrift exception handler at default priority.
  register_handler(DefaultThriftExceptionHandler)
end
