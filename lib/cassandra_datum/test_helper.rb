module CassandraDatum
  module TestHelper
      extend ActiveSupport::Concern

      included do
        CASSANDRA_CLIENT.clear_keyspace! if defined?(Rails) && Rails.env.test? # extra paranoid with an operation like this
      end

      def assert_data_equal(list1, list2, explanation=nil)
        assert_equal list1.size, list2.size, explanation
        list1.each_with_index do |x, i|
          y = list2[i]
          if x.is_a? Array
            assert_data_equal x, y, explanation
          elsif x.is_a? CassandraDatum::Base
            assert_datum_equal x, y, explanation
          else
            assert_equal x, y, explanation
          end
        end
      end

      def assert_datum_equal(datum1, datum2, explanation=nil)
        assert_equal datum1.row_id, datum2.row_id, explanation
        assert_equal datum1.column_name, datum2.column_name, explanation
        assert_hashes_equal datum1.attributes, datum2.attributes, explanation
      end

      def assert_hashes_equal(hash1, hash2, explanation=nil)
        assert_equal hash1.keys.size, hash2.keys.size, explanation
        hash1.keys.each do |k|
          if hash1[k].is_a? DateTime
            assert_equal hash1[k].to_i, hash2[k].to_i, explanation
          else
            assert_equal hash1[k], hash2[k], explanation
          end
        end
      end
    end

end
