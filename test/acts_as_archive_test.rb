require File.expand_path(File.dirname(__FILE__) + '/helper.rb')

module CassandraDatum
  class TestModel < ActiveRecord::Base
    include CassandraDatum::ActsAsArchive
  end

  class ActsAsArchiveTest < Test::Unit::TestCase
    should 'archive record to cassandra after record destroying' do
      record = TestModel.create(:title => "title", :description => "description")
      record.destroy

      # get list of archived records
      archived_records = TestModel.archived_after(record.created_at)

      # find the id of last archived record
      id = archived_records.keys.last

      # get the last archived record
      last_archived_record = archived_records[id]

      assert_equal 1, archived_records.size

      assert_equal "title", last_archived_record['title']
      assert_equal "description", last_archived_record['description']
    end
  end
end