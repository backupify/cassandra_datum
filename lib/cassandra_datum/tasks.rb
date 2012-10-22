namespace :cassandra do

  # TODO (davebenvenuti 10/4/12) these tasks should use the hosts field from configuration, but we need to figure out a way to deal with the embedded ruby first

  desc "Reset (Drop, Create, Remigrate) the Cassandra schema"
  task :reset do
    begin
      Rake::Task['cassandra:drop'].invoke
    rescue Thrift::Exception => e
      puts "ignoring thrift exception #{e} (keyspace probably doesn't exist)"
    end

    Rake::Task['cassandra:create'].invoke
    Rake::Task['cassandra:migrate'].invoke
  end

  desc "Drop the keyspace from Cassandra"
  task :drop do
    client = Cassandra.new "system", ["#{`hostname`.strip}:9160"]

    begin
      puts "Dropping keyspace #{keyspace_name}..."

      with_thrift_timeout_retry do
        client.drop_keyspace keyspace_name
      end

    rescue Thrift::Exception => e
      if ENV['IGNORE_THRIFT_EXCEPTIONS']
        puts "ignoring thrift exception #{e}"
      else
        raise e
      end
    ensure
      client.disconnect!
    end
  end

  desc "Create the keyspace in Cassandra"
  task :create do
    client = Cassandra.new "system", ["#{`hostname`.strip}:9160"]

    begin
      puts "Creating keyspace #{keyspace_name}..."

      keyspace_definition = CassandraThrift::KsDef.new({
          :name => keyspace_name,
          :strategy_class => 'org.apache.cassandra.locator.SimpleStrategy',
          :strategy_options => { 'replication_factor' => '1' },
          :cf_defs => []
      })

      with_thrift_timeout_retry do
        client.add_keyspace keyspace_definition
      end

    rescue Thrift::Exception => e
      if ENV['IGNORE_THRIFT_EXCEPTIONS']
        puts "ignoring thrift exception #{e}"
      else
        raise e
      end
    ensure
      client.disconnect!
    end

    true
  end

  desc "Bring the keyspace up to date"
  task :migrate do
    client = Cassandra.new keyspace_name, ["#{`hostname`.strip}:9160"]


    # the migrate task needs a little more resilience with respect to timeouts and thrift errors.  we should try 3 times with a random sleep in between

    begin
      column_families.each do |cf|
        cf_def = CassandraThrift::CfDef.new({
                                       :name => cf['name'],
                                       :column_type => cf['column_type'],
                                       :comparator_type => cf['compare_with'],
                                       :subcomparator_type => cf['compare_subcolumns_with'],
                                       :keyspace => keyspace_name
        })

        with_thrift_timeout_retry do

          client.keyspace = keyspace_name # reloads the schema so the column_families are up to date

          if client.column_families.has_key?(cf['name'])
            puts "Skipping column family #{cf['name']}, already exsits"
          else
            puts "Creating column family #{cf['name']}"

            client.add_column_family cf_def
          end

        end

      end

    rescue Thrift::Exception => e
      if ENV['IGNORE_THRIFT_EXCEPTIONS']
        puts "ignoring thrift exception #{e}"
      else
        raise e
      end

    ensure
      client.disconnect!
    end

    true
  end

  def keyspace_name
    CassandraDatum.configuration['keyspace']
  end

  def column_families
    CassandraDatum.configuration['column_families']
  end

  def with_thrift_timeout_retry
    max_tries = 3
    current_try = 0

    begin
      yield
    rescue CassandraThrift::Cassandra::Client::TransportException => e
      if (current_try < max_tries) && (e.type =~ /Timed out reading/)
        puts "Encountered thrift exception #{e}, retrying..."

        sleep rand(5)

        current_try += 1

        retry
      else
        raise e
      end
    end

  end

end
