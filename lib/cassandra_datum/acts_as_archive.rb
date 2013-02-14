module CassandraDatum
  module ActsAsArchive
    # The active record extension implements the archive functionality to cassandra database. After record get deleted
    # it replicates the copy of all attributes of that object to cassandra.
    #
    # Example of usage
    #   class Service < ActiveRecord::Base
    #     include CassandraDatum::ActsAsArchive
    #
    #     acts_as_archive
    #   end
    #
    # After you delete a record, it will be automatically replicated to cassandra in the following format
    #
    #   column_family will be 'deleted_records'
    #   row_id will be the same as table name, for example above it will be 'services'
    #   column_name is the removal timestamp in unix format, it looks like '1360754413'
    #   column_attributes will contain the active record attributes
    #
    # If you want to get list of deleted services for some period of time you do
    #   Service.archived_after(7.days.ago)

    def self.included(base)
      base.extend ClassMethods
      base.send :include, InstanceMethods
    end

    module InstanceMethods
      def archive
        archived_at = Helpers.current_time
        ::CassandraDatum::Base.cassandra_client.insert('DeletedRecords', self.class.table_name, {archived_at.to_i.to_s => self.attributes})
      end
    end

    module ClassMethods
      def archived_after(time)
        start_at = time
        finish_at = Helpers.current_time
        CassandraDatum::Base.cassandra_client.get('DeletedRecords', table_name, :start => start_at.to_i.to_s, :finish => finish_at.to_i.to_s)
      end

      def acts_as_archive
        class_eval do
          after_commit :on => :destroy do
            archive
          end
        end
      end
    end

    module Helpers
      def self.current_time
        defined?(Rails) ? Time.zone.now : Time.now
      end
    end
  end
end
