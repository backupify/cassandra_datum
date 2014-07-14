require 'rails'

module CasandraDatum
  class Railtie < Rails::Railtie

    rake_tasks do
      require 'cassandra_datum/tasks'
    end

  end
end
