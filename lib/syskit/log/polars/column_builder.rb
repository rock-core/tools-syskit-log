# frozen_string_literal: true

module Syskit
    module Log
        module Polars
            # Dispatch of data to a single column
            class ColumnBuilder
                attr_reader :name
                attr_reader :value_transform
                attr_reader :global_transform

                def initialize(name:, value_transform: nil, global_transform: nil)
                    @name = name
                    @value_transform = value_transform
                    @global_transform = global_transform
                end

                def apply_value_transform(value)
                    @value_transform ? @value_transform.call(value) : value
                end

                def create_series(data)
                    ::Polars::Series.new(@name, data, dtype: @dtype)
                end

                def apply_global_transform(series)
                    @global_transform ? @global_transform.call(series) : series
                end
            end
        end
    end
end
