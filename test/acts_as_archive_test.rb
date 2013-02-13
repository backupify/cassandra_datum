require File.expand_path(File.dirname(__FILE__) + '/helper.rb')

module CassandraDatum
  class Model
    def self.after_commit(options = {})
    end

    def self.table_name
      "models"
    end

    include CassandraDatum::ActsAsArchive

    acts_as_archive

    def attributes
      {
        "attribute_name1" => "value1",
        "attribute_name2" => "value2",
        "attribute_name3" => "value3",
      }
    end
  end

  class ActsAsArchiveTest < Test::Unit::TestCase
    context 'archive' do
      setup do
        @record = Model.new
        @before_archive_time = Time.now
        @record.archive

        @archived_records = Model.archived_after(@before_archive_time)
        @deletion_timestamp = @archived_records.keys.first
      end

      should 'archive record and return it in archived_after response' do
        assert_equal 1, @archived_records.size
      end

      should 'should archive all attributes correctly' do
        archived_record = Model.archived_after(@before_archive_time)[@deletion_timestamp]

        assert_equal 'value1', archived_record['attribute_name1']
        assert_equal 'value2', archived_record['attribute_name2']
        assert_equal 'value3', archived_record['attribute_name3']
      end
    end
  end
end