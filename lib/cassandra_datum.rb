require 'cassandra_datum/base'

module CassandraDatum

  def self.configuration
    @@configuration ||= begin
      require 'erb'
      env = defined?(Rails) ? Rails.env : 'development'
      config_file = ENV['CONFIG_FILE'] || (defined?(Rails) ? "#{Rails.root}/config/cassandra.yml" : nil)
      fail "No CONFIG_FILE or Rails.root defined" unless config_file
      config_file = File.expand_path(config_file)
      config = YAML::load(ERB.new(IO.read(config_file)).result)
      config[env]
    end
  end

end
