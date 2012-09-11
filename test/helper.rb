require 'rubygems'
require 'bundler'
begin
  Bundler.setup(:default, :development)
rescue Bundler::BundlerError => e
  $stderr.puts e.message
  $stderr.puts "Run `bundle install` to install missing gems"
  exit e.status_code
end
require 'test/unit'
require 'shoulda'
require 'factory_girl'

$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))

require 'cassandra_datum'
require 'cassandra_datum/test_helper'

::CASSANDRA_CLIENT = Cassandra.new('BackupifyMetadata_test', %w[localhost:9160])

class Test::Unit::TestCase
  include CassandraDatum::TestHelper
end

class MockCassandraDatum < CassandraDatum::Base

  attribute! :row_id
  attribute! :document_id
  attribute! :timestamp, :type => DateTime
  attribute :payload

  validates_presence_of :timestamp
end

class OverrideColumnFamilyDatum < CassandraDatum::Base
  column_family 'MockCassandraData'

  attribute :payload
end

class STICassandraDatum < MockCassandraDatum
  attribute :type, :type => String
end

FactoryGirl.define do
  factory :mock_cassandra_datum, :aliases => [:cassandra_datum] do
    row_id { SecureRandom.hex(8) }
    document_id { SecureRandom.hex(8) }
    timestamp { Time.now }
    sequence(:payload) { |n| "data payload #{n}" }
  end

  factory :sti_cassandra_datum, :class => STICassandraDatum, :parent => :cassandra_datum do
  end
end
