require 'active_attr/model'
require 'active_model/callbacks'
require 'exception_helper/retry'
require 'active_record/errors'
require 'active_record/validations'
require 'cassandra'

module CassandraDatum
class Base
  include ActiveAttr::Model

  extend ActiveModel::Callbacks

  include ExceptionHelper::Retry

  define_model_callbacks :save
  define_model_callbacks :destroy

  attr_reader :updated_at

  FIRST_KEY = ''
  LAST_KEY = 'a~0'
  DEFAULT_ALL_COUNT = 50
  DEFAULT_WALK_ROW_COUNT = 1000
  SINGLETON = 1

  before_save :populate_type_if_exists

  def initialize_with_updated_at(*attr)
    # the OrderedHash returned by the cassandra client has a timestamps method which contains the write date of each column
    if attr.size > 0 && attr.first.respond_to?(:timestamps)
      timestamp_in_microseconds = attr.first.timestamps.values.max
      @updated_at = Time.at(timestamp_in_microseconds / 1000000, timestamp_in_microseconds % 1000000).to_datetime
    end

    initialize_without_updated_at(*attr)
  end

  def initialize_with_utf8_encoding(*attr)
    if attr.size > 0 && attr.first.is_a?(Hash)
      #careful not to trounce timestamps in Cassandra::OrderedHash
      timestamps = attr.first.is_a?(Cassandra::OrderedHash) ? attr.first.timestamps : nil
      attr.first.each { |k, v| attr.first[k] = encode_value(v) unless v.blank? }
      attr.first.instance_variable_set(:@timestamps, timestamps) if timestamps.present?
    end

    initialize_without_utf8_encoding(*attr)
  end

  alias_method_chain :initialize, :updated_at
  alias_method_chain :initialize, :utf8_encoding

  def self.create(*attr)
    new(*attr).tap(&:save!)
  end

  def self.column_family(*name)
    if name.present?
      @column_family = name.first
    else
      @column_family || model_name.plural.camelize
    end
  end

  #key can be from to_param as well as just the key. This function handles both
  def self.find(key)
    row_id, column_name = Base64.decode64(key.tr('-_', '+/')).split(':', 2)

    res = cassandra_client.get(column_family, row_id, column_name)

    raise ActiveRecord::RecordNotFound.new if res.blank?

    initialize_datum res
  end

  def self.find_by_key(key)
    find(key)
  rescue ActiveRecord::RecordNotFound
    nil
  end

  #will always return data in reverse chronological order
  # @option[row_id] the row_id to paginate through (optional if passing in a before_id or after_id)
  # @option[reversed] return the results in reversed order (exclusive)
  # @option[before_id] return a page of data that occurs before this key (exclusive)
  # @option[after_id] return a page of data that occurs after this key (exclusive)
  # @option[count] limit the number of data returned (default 50)
  def self.all(options={})
    options.symbolize_keys! if options.respond_to?(:symbolize_keys!)

    # Check options to make sure more than one exclusive key is not passed in
    exclusive_keys = options.keys & [:reversed, :before_id, :after_id]
    raise ArgumentError.new("Cannot use #{exclusive_keys} together.") if exclusive_keys.size > 1

    cass_options = {}
    cass_options[:count] = (options[:count] || DEFAULT_ALL_COUNT).to_i

    row_id = options[:row_id].to_s

    if options[:reversed]
      cass_options[:reversed] = true
      cass_options[:start] = LAST_KEY
    elsif options[:before_id]
      row_id, cass_options[:start] = Base64.decode64(options[:before_id].tr('-_', '+/')).split(':', 2)
      cass_options[:reversed] = true
      cass_options[:count] += 1
    elsif options[:after_id]
      row_id, cass_options[:start] = Base64.decode64(options[:after_id].tr('-_', '+/')).split(':', 2)
      cass_options[:count] += 1
    end

    result = cassandra_client.get(column_family, row_id, cass_options).collect do |k, v|
      initialize_datum v
    end

    if options[:before_id]
      result.delete_at(0) if result.size > 0 && result[0].key == options[:before_id]
      result.reverse!
    elsif options[:after_id]
      result.delete_at(0) if result.size > 0 && result[0].key == options[:after_id]
    end

    result
  end

  # don't overuse this.  it crawls an entire row
  def self.find_each(row_id, options = {})
    walk_row(row_id, options) do |k, v|
      yield initialize_datum(v)
    end
  end

  # don't overuse this.  it crawls an entire row
  def self.find_each_key(row_id, options = {})
    walk_row(row_id, options) { |k, v| yield Base64.strict_encode64([row_id, k].join(':')).tr('+/', '-_') }
  end

  def row_id
    SINGLETON.to_s
  end

  def document_id
    SINGLETON
  end

  def timestamp
    Time.at(SINGLETON).to_datetime
  end

  def column_name
    encode_for_cassandra("#{document_id}~#{timestamp.to_i}")
  end

  def key
    Base64.strict_encode64([row_id, column_name].join(':')).tr('+/', '-_')
  end

  def encode_for_cassandra(str, opts = {})
    CassandraDatum::Base.encode_for_cassandra(str, opts)
  end

  def self.encode_for_cassandra(str, opts = {})
    encode_opts = {
        :invalid => :replace,
        :undef => :replace,
        :replace => ''
    }.merge(opts)

    str.encode('UTF-8', encode_opts).force_encoding('ASCII-8BIT')
  end


  def to_param
    self.key
  end

  def save
    save!
  rescue Exception => e
    false
  end

  def save!
    run_callbacks :save do
      attrs = {}

      attributes.reject { |k, v| v.nil? }.each do |k, v|
        attrs[k] = [Array, Hash].any?{ |collection_class| v.is_a?(collection_class) } ? v.to_json : "#{v}"
        attrs[k] = encode_for_cassandra(attrs[k])
      end

      raise ActiveRecord::RecordInvalid.new(self) unless self.valid?

      self.class.cassandra_client.insert(self.class.column_family, self.row_id, {self.column_name => attrs})

      # this value might be a tad different from the value in cassandra.  the only way to get the true updated_at value is to reload the datum
      @updated_at = DateTime.now
    end
  end

  def reload
    self.class.find(self.key)
  end

  def destroy
    run_callbacks :destroy do
      self.delete
    end
  end

  def self.delete_all(row_id)
    cassandra_client.remove column_family, row_id
  end

  def self.delete(row_id, *column_names)
    column_names.flatten.each { |column_name| CASSANDRA_CLIENT.remove column_family, row_id, column_name }
  end

  def delete
    self.class.delete(self.row_id, self.column_name)
  end

  def self.cassandra_client
    if defined?(::CASSANDRA_CLIENT)
      ::CASSANDRA_CLIENT
    else
      logger.error("No cassandra client defined. Please set CASSANDRA_CLIENT")
      nil
    end
  end


  def as_json(options={})
    options = {:only => self.class.accessor_names}.merge(options)
    self.include_root_in_json = false
    super(options)
  end

  def new_record?
    self.updated_at.blank?
  end

  def encode_value(v)
    case v
    when Hash, Array
      v = v.to_json
    else
      v = "#{v}"
    end
    v.force_encoding('UTF-8')
  end

  protected

  # don't overuse this.  it crawls an entire row
  def self.walk_row(row_id, options = {})
    options = {:count => DEFAULT_WALK_ROW_COUNT}.merge(options)

    start = options[:start] || (options[:reversed] ? LAST_KEY : FIRST_KEY)
    last_start = nil

    loop do
      retry_on_failure(::Thrift::Exception, :retry_count => 5, :retry_sleep => 10) do
        last_start = start

        res = cassandra_client.get(column_family, row_id, options.merge(:start => start))

        res.each do |k, v|
          next if k == last_start # ignore the first result we get back.  since start is the last record in the previous get, it'll always be off by 1
          start = k
          yield [k, v]
        end
      end

      break if last_start == start
    end
  end

  def populate_type_if_exists
    self.type = self.class.name if self.respond_to?(:type=) && self.type.blank?
  end

  def self.initialize_datum(res)
    datum_class = res['type'].present? ? res['type'].constantize : self
    datum_class.new res
  end
end
end
