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
        assert_attributes_equal datum1, datum2, explanation
      end

      def assert_attributes_equal(datum1, datum2, explanation=nil)
        assert_equal datum1.attributes.size, datum2.attributes.size, "different number of attributes"
        datum1.attributes.keys.each do |attribute_name|
          value1 = datum1.send(attribute_name)
          value2 = datum2.send(attribute_name)

          full_explanation = explanation.present? ? "#{attribute_name}: #{explanation}" : "datum1.#{attribute_name} != datum2.#{attribute_name}"

          if value1.is_a?(Date)
            assert_equal value1.to_i, value2.to_i, full_explanation
          else
            assert_equal value1, value2, full_explanation
          end
        end
      end
    end

end
