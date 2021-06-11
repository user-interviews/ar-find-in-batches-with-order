# frozen_string_literal: true

require "ar-find-in-batches-with-order/version"

module ActiveRecord
  module FindInBatchesWithOrder
    def find_in_batches_with_order(options = {})
      relation = self

      # be explicit about the options to ensure proper ordering and retrieval
      direction = options.delete(:direction) ||
        (arel.orders.first.try(:ascending?) ? :asc : nil) ||
        (arel.orders.first.try(:descending?) ? :desc : nil) || :desc

      start = options.delete(:start)
      batch_size = options.delete(:batch_size) || 1000
      with_start_ids = []

      # try to deduct the property_key, but safer to specify directly
      property_key = options.delete(:property_key) ||
        arel.orders.first.try(:value).try(:name) ||
        arel.orders.first.try(:split,' ').try(:first)

      table_name = options.delete(:property_table_name) || table.name
      tbl = connection.quote_table_name(table_name)
      sanitized_key = "#{tbl}.#{connection.quote_column_name(property_key)}"
      relation = relation.limit(batch_size)

      exclusive_comparison = direction == :desc ? '<' : '>'
      inclusive_comparison = direction == :desc ? '<=' : '>='

      records =
        if !start
          relation
        else
          relation.where("#{sanitized_key} #{inclusive_comparison} ?", start)
        end.to_a

      while records.any?
        records_size = records.size

        yield records
        break if records_size < batch_size

        next_start = records.last.try(property_key)

        if with_start_ids.none? && start == next_start
          raise 'did not increment next start'
        end

        with_start_ids.clear if start != next_start
        start = next_start

        records.each do |record|
          if record.try(property_key) == start
            with_start_ids << record.id
          end
        end

        with_start_ids.compact!

        without_duplicates =
          if with_start_ids.any?
            # Not all queries will be selecting a primary key in the result set
            # (common with group by queries using property key, for example)
            relation
              .where.not(relation.klass.primary_key => with_start_ids)
              .where("#{sanitized_key} #{inclusive_comparison} ?", start)
          else
            # ...so in that case exclude the start entirely, rather than allow its
            # further inclusion, otherwise there is a chance of infinite looping
            relation.where("#{sanitized_key} #{exclusive_comparison} ?", start)
          end

        without_duplicates.to_a
      end
    end

    def find_each_with_order(options = {})
      find_in_batches_with_order(options) do |records|
        records.each do |record|
          yield record
        end
      end
    end
  end

  class Relation
    include FindInBatchesWithOrder
  end
end
