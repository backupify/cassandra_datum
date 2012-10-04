namespace :cassandra do

  # TODO (davebenvenuti 10/4/12) these tasks should use the hosts field from configuration, but we need to figure out a way to deal with the embedded ruby first

  task :reset do
    Rake::Task['cassandra:drop'].invoke rescue nil # the keyspace may not exist
    Rake::Task['cassandra:create'].invoke
    Rake::Task['cassandra:migrate'].invoke
  end

  task :drop do
    client = Cassandra.new "system", ["#{`hostname`.strip}:9160"]

    keyspace_name = CassandraDatum.configuration['keyspace']

    begin
      puts "Dropping keyspace #{keyspace_name}..."

      client.drop_keyspace keyspace_name
    ensure
      client.disconnect!
    end
  end

  task :create do
    client = Cassandra.new "system", ["#{`hostname`.strip}:9160"]

    keyspace_name = CassandraDatum.configuration['keyspace']

    begin
      puts "Creating keyspace #{keyspace_name}..."

      keyspace_definition = CassandraThrift::KsDef.new({
          :name => keyspace_name,
          :strategy_class => 'org.apache.cassandra.locator.SimpleStrategy',
          :strategy_options => { 'replication_factor' => '1' },
          :cf_defs => []
      })

      client.add_keyspace keyspace_definition
    ensure
      client.disconnect!
    end
  end

  task :migrate do
    keyspace_name = CassandraDatum.configuration['keyspace']
    column_families = CassandraDatum.configuration['column_families']
    client = Cassandra.new keyspace_name, ["#{`hostname`.strip}:9160"]

    begin
      column_families.each do |cf|
        cf_def = CassandraThrift::CfDef.new({
                                       :name => cf['name'],
                                       :column_type => cf['column_type'],
                                       :comparator_type => cf['compare_with'],
                                       :subcomparator_type => cf['compare_subcolumns_with'],
                                       :keyspace => keyspace_name
        })

        puts "Creating column family #{cf['name']}"

        client.add_column_family cf_def

      end

    ensure
      client.disconnect!
    end

  end

end
