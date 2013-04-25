require File.expand_path(File.dirname(__FILE__) + '/helper.rb')

module CassandraDatum
class BaseTest < Test::Unit::TestCase

  should 'use timestamp long in column_name' do
    time = DateTime.now
    datum = FactoryGirl.create(:cassandra_datum, :timestamp => time)

    assert datum.column_name.end_with?(time.to_i.to_s)
  end

  should "have a reload function that pulls from cassandra" do
    datum = FactoryGirl.create(:cassandra_datum)

    assert_datum_equal datum, datum.reload
  end

  should 'properly encode string' do
    name = "No\u00eblle"

    assert_equal "UTF-8", name.encoding.to_s

    encoded_name = CassandraDatum::Base.encode_for_cassandra(name)

    assert_equal "ASCII-8BIT", encoded_name.encoding.to_s
    assert_equal name.encode('UTF-8').force_encoding('ASCII-8BIT'), encoded_name
  end

  should "handle encodings" do
    enc = "\u20ACuro"
    assert_equal "UTF-8", enc.encoding.to_s

    datum = FactoryGirl.create(:cassandra_datum, :payload => enc)

    datum = MockCassandraDatum.find(datum.key)

    assert_equal enc, datum.payload
    assert_equal 'UTF-8', datum.payload.encoding.to_s
  end

  should "populate type field if possible" do
    datum = FactoryGirl.create(:polymorphic_cassandra_datum)
    assert_equal datum.class.to_s, datum.type
  end

  context 'save' do
    should 'save attributes to cassandra' do
      datum = FactoryGirl.create(:cassandra_datum)

      cass_entry = MockCassandraDatum.find(datum.key)

      assert cass_entry.present?

      cass_entry.attributes.each do |k, v|
        assert !v.nil?
        assert_equal v, datum.send(k).to_s
      end
    end

    should 'reject nil values during save' do
      datum = FactoryGirl.create(:cassandra_datum)

      cass_td = CASSANDRA_CLIENT.get(datum.class.column_family, datum.row_id, datum.column_name)
      assert cass_td.keys.include?('payload')

      datum = FactoryGirl.create(:cassandra_datum, :payload => nil)

      cass_td = CASSANDRA_CLIENT.get(datum.class.column_family, datum.row_id, datum.column_name)
      assert !cass_td.keys.include?('payload')
    end

    should 'not save an invalid datum' do
      datum = FactoryGirl.build(:cassandra_datum, :timestamp => nil)
      assert !datum.valid?
      assert !datum.save
      assert_raises(ActiveRecord::RecordInvalid) { datum.save! }
    end

    should 'strip invalid characters when encoding to UTF-8' do
      datum = FactoryGirl.build(:cassandra_datum, :payload => "my payload\xEF")

      assert_nothing_raised { datum.save! }

      fetched_datum = MockCassandraDatum.find(datum.key)
      assert_equal 'my payload', fetched_datum.payload
    end

    should 'convert arrays and hashes to json' do
      array_value = ['some', 'values']
      hash_value = { 'foo' => 'bar' }

      datum = FactoryGirl.create(:datum_with_array_and_hash, :an_array => array_value, :a_hash => hash_value)

      res = CASSANDRA_CLIENT.get(datum.class.column_family, datum.row_id, datum.column_name)
      assert_equal array_value.to_json, res['an_array']
      assert_equal hash_value.to_json, res['a_hash']
    end

    should 'return self when save! is called' do
      datum = FactoryGirl.build(:cassandra_datum)

      assert_equal datum, datum.save!
    end
  end


  context 'destroy' do
    should 'remove datum from cassandra' do
      datum = FactoryGirl.create(:cassandra_datum)

      datum.destroy

      assert MockCassandraDatum.find_by_key(datum.key).blank?
    end
  end

  context "delete_all" do
    setup do
      @row_id = SecureRandom.hex(8)
      3.times { FactoryGirl.create(:cassandra_datum, :row_id => @row_id) }
    end

    should "delete an entire row from cassandra" do
      MockCassandraDatum.delete_all(@row_id)
      assert_equal [], MockCassandraDatum.all(:row_id => @row_id)
    end
  end


  context 'delete' do
    setup do
      @row_id = SecureRandom.hex(8)
      @data = 3.times.collect { FactoryGirl.create(:cassandra_datum, :row_id => @row_id) }
    end

    should "delete a list of column ids" do
      MockCassandraDatum.delete(@row_id, @data[0].column_name, @data[1].column_name)

      @data[0..1].each { |datum| assert_nil MockCassandraDatum.find_by_key datum.key }
      assert MockCassandraDatum.find(@data[2].key).present?
    end

    should "flatten arguments" do
      MockCassandraDatum.delete(@row_id, [@data[0].column_name], @data[1].column_name)

      @data[0..1].each { |datum| assert_nil MockCassandraDatum.find_by_key datum.key }
      assert MockCassandraDatum.find(@data[2].key).present?
    end
  end


  context 'document lookup' do
    setup do
      @row_id = SecureRandom.hex(8)
    end

    context 'find' do

      should 'find by key' do
        datum = FactoryGirl.create(:cassandra_datum)

        doc = MockCassandraDatum.find(datum.key)

        assert_datum_equal datum, doc
      end

      should 'find by key, initialize polymorphically ' do
        datum = FactoryGirl.create(:polymorphic_cassandra_datum)

        # when we fetch with the base class, it should initialize an instance of the constantized :type attribute
        doc = MockCassandraDatum.find(datum.key)

        assert_datum_equal datum, doc
        assert_instance_of PolymorphicCassandraDatum, doc
      end

    end

    context 'all' do

      should 'be sorted by timestamp in reverse order' do
        data = 3.times.collect { |i| FactoryGirl.build(:cassandra_datum, :row_id => @row_id, :timestamp => DateTime.now + i) }

        data.shuffle!
        data.each {|d| d.save! } #save in random order
        data = data.sort_by(&:timestamp).reverse #reverse sort by timestamp

        res = MockCassandraDatum.all(:row_id => @row_id)

        assert_data_equal data, res, "not sorted properly: #{res.collect(&:column_name)}.\n expected: #{data.collect(&:column_name)}"
      end

      should 'convert count option to integer' do
        3.times.collect { |i| FactoryGirl.create(:cassandra_datum, :row_id => @row_id, :timestamp => DateTime.now + i) }

        res = MockCassandraDatum.all(:row_id => @row_id, :count => '2')

        assert_equal 2, res.size
      end

      should 'honor polymorphic :type column' do
        data = [
            FactoryGirl.create(:cassandra_datum, :row_id => @row_id, :timestamp => DateTime.now),
            FactoryGirl.create(:polymorphic_cassandra_datum, :row_id => @row_id, :timestamp => DateTime.now - 1),
            FactoryGirl.create(:cassandra_datum, :row_id => @row_id, :timestamp => DateTime.now - 2)
        ]

        res = MockCassandraDatum.all(:row_id => @row_id)

        assert_data_equal data, res
      end

    end

  end

  context "URL ID encoding" do
    setup do
      @datum = FactoryGirl.create(:cassandra_datum)
    end

    should "encode to_param" do
      assert_equal @datum.key, @datum.to_param
    end
  end

  context '#new_record?' do
    should 'be a new record before saving' do
      datum = FactoryGirl.build(:cassandra_datum)

      assert datum.new_record?
    end

    should 'not be a new record after saving' do
      datum = FactoryGirl.create(:cassandra_datum)

      assert !datum.new_record?
    end

    should 'not be a new record when coming from #find' do
      datum = FactoryGirl.create(:cassandra_datum)
      retrieved = MockCassandraDatum.find(*datum.key)

      assert !retrieved.new_record?
    end

    should 'not be a new record when coming from #all' do
      row_id = SecureRandom.uuid
      3.times { FactoryGirl.create(:cassandra_datum, :row_id => row_id) }

      retrieved = MockCassandraDatum.all(:row_id => row_id)

      retrieved.each do |retrieved_datum|
        assert !retrieved_datum.new_record?
      end
    end
  end

  context "find_each and find_each_key" do

    setup do
      @row_id = SecureRandom.uuid
    end

    should 'yield nothing for service with with no records' do
      yielded = false
      MockCassandraDatum.find_each(@row_id){ yielded = true }
      MockCassandraDatum.find_each_key(@row_id){ yielded = true }
      assert !yielded, "CassandraDatum#each should not have yielded anything"
    end

    should 'yield all records and keys' do
      data = []

      #cover all cases while crossing the per-page boundry of walk_row
      4.times do |i|
        data << FactoryGirl.create(:cassandra_datum, :row_id => @row_id, :timestamp => i.days.ago)

        yielded_data = []
        yielded_keys = []
        MockCassandraDatum.find_each(@row_id, :count => 3) { |datum| yielded_data << datum }
        MockCassandraDatum.find_each_key(@row_id, :count => 3) { |key| yielded_keys << key }

        assert_data_equal data, yielded_data
        assert_data_equal data.collect(&:key), yielded_keys

        #reversed should work as well
        yielded_data = []
        yielded_keys = []
        MockCassandraDatum.find_each(@row_id, :count => 3, :reversed => true) { |datum| yielded_data << datum }
        MockCassandraDatum.find_each_key(@row_id, :count => 3, :reversed => true) { |key| yielded_keys << key }

        assert_data_equal data.reverse, yielded_data
        assert_data_equal data.reverse.collect(&:key), yielded_keys
      end
    end
  end

  context "updated_at" do
    # note for all of these tests that cassandra timestamp values are in microseconds by default, hence the / 1000000

    should "return correct updated_at for a single object" do
      datum = FactoryGirl.create(:cassandra_datum)

      cassandra_tr = CASSANDRA_CLIENT.get(datum.class.column_family, datum.row_id, datum.column_name)
      cassandra_time = cassandra_tr.timestamps.values.max / 1000000

      assert_equal cassandra_time, MockCassandraDatum.find(datum.key).updated_at.to_i
    end

    should "return correct updated_at for a multi get" do
      row_id = SecureRandom.uuid
      3.times { FactoryGirl.create(:cassandra_datum, :row_id => row_id) }

      MockCassandraDatum.find_each(row_id) do |datum|
        cassandra_tr = CASSANDRA_CLIENT.get(datum.class.column_family, datum.row_id, datum.column_name)
        cassandra_time = cassandra_tr.timestamps.values.max / 1000000

        assert_equal cassandra_time, datum.updated_at.to_i
      end
    end

    should "have its value populated with the initialize method when given a Cassandra::OrderedHash" do
      datum = FactoryGirl.create(:cassandra_datum)
      ordered_hash = CASSANDRA_CLIENT.get(datum.class.column_family, datum.row_id, datum.column_name)
      expected_time = ordered_hash.timestamps.values.max / 1000000

      datum = MockCassandraDatum.new(ordered_hash)
      assert datum.updated_at.is_a?(DateTime)
      assert_equal expected_time, datum.updated_at.to_time.to_i
    end
  end

  context "column_family" do
    should "default to the pluralization of the class name" do
      assert_equal 'MockCassandraData', MockCassandraDatum.column_family
    end

    should "allow override in declaration" do
      assert_equal 'MockCassandraData', OverrideColumnFamilyDatum.column_family
      datum = OverrideColumnFamilyDatum.create :payload => 'mock payload'

      assert_datum_equal datum, OverrideColumnFamilyDatum.find(datum.key)
      assert MockCassandraDatum.find(datum.key).present? #both objects are using the same column family
    end
  end

  should 'support observers' do
    MockCassandraDatum.reset_before_save_counts!

    datum = FactoryGirl.create(:cassandra_datum)

    # see MockCassandraDatum definition in
    assert_equal 1, MockCassandraDatum.before_save_counts[datum]
  end

  should 'support activerecord before/after callbacks' do
    MockCassandraDatumObserver.reset_before_save_counts!

    ActiveRecord::Base.observers = MockCassandraDatumObserver
    ActiveRecord::Base.instantiate_observers

    datum = FactoryGirl.create(:cassandra_datum)

    # see MockCassandraDatumObserver definition in helper.rb
    assert_equal 1, MockCassandraDatumObserver.before_save_counts[datum]
  end

end
end