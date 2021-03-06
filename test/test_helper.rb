require 'rubygems'
if ENV["CI"]
  require "coveralls"
  Coveralls.wear!
end
require 'bundler'

begin
  Bundler.setup(:default, :development)
rescue Bundler::BundlerError => e
  $stderr.puts e.message
  $stderr.puts "Run `bundle install` to install missing gems"
  exit e.status_code
end
require 'minitest/autorun'
require 'factory_girl'

$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))

require 'cassandra_datum'
require 'cassandra_datum/test_helper'

::CASSANDRA_CLIENT = TwitterCassandra.new('BackupifyMetadata_test', %w[127.0.0.1:9160])

I18n.enforce_available_locales = false

module CassandraDatum
  class TestCase < MiniTest::Spec
    include CassandraDatum::TestHelper

    # make minitest spec dsl similar to shoulda
    class << self
      alias :setup :before
      alias :teardown :after
      alias :context :describe
      alias :should :it
    end
  end
end

class MockCassandraDatum < CassandraDatum::Base

  attribute! :row_id
  attribute! :document_id
  attribute! :timestamp, :type => DateTime
  attribute :payload

  validates_presence_of :timestamp

  # Just to test before_save callback
  before_save do |datum|
    @@before_save_counts ||= {}
    @@before_save_counts[datum].present? ? @@before_save_counts[datum] += 1 : @@before_save_counts[datum] = 1
  end

  def self.before_save_counts
    @@before_save_counts
  end

  def self.reset_before_save_counts!
    @@before_save_counts = {}
  end
end

class OverrideColumnFamilyDatum < CassandraDatum::Base
  column_family 'MockCassandraData'

  attribute :payload
end

class OverrideDifferentColumnFamilyDatum < CassandraDatum::Base
  column_family 'OverrideCassandraData'
end

class PolymorphicCassandraDatum < MockCassandraDatum
  column_family 'MockCassandraData'
  attribute :type, :type => String
end

class DatumWithArrayAndHash < MockCassandraDatum
  column_family 'MockCassandraData'
  attribute :type, :type => String

  attribute :a_hash, :type => Hash
  attribute :an_array, :type => Array
end

require 'active_support/concern'

module MockCassandraDatumObserver
  extend ActiveSupport::Concern

  included do
    before_save :count_before_save
  end

  private

  def count_before_save
    @@before_save_counts ||= {}
    @@before_save_counts[self].present? ? @@before_save_counts[self] += 1 : @@before_save_counts[self] = 1
  end

  def self.before_save_counts
    @@before_save_counts
  end

  def self.reset_before_save_counts!
    @@before_save_counts = {}
  end
end

FactoryGirl.define do
  factory :mock_cassandra_datum, :aliases => [:cassandra_datum] do
    row_id { SecureRandom.hex(8) }
    document_id { SecureRandom.hex(8) }
    timestamp { Time.now }
    sequence(:payload) { |n| "data payload #{n}" }
  end

  factory :polymorphic_cassandra_datum, :class => PolymorphicCassandraDatum, :parent => :cassandra_datum do
  end

  factory :datum_with_array_and_hash, :class => DatumWithArrayAndHash, :parent => :cassandra_datum do
  end
end
