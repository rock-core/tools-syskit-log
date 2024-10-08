# frozen_string_literal: true

module Syskit
    module Log
        module Polars
            # @api private
            #
            # Dispatch of a stream's logical time into a single dataframe column
            class ColumnLogicalTimeBuilder < ColumnBuilder
                def initialize(name:, value_transform: nil, global_transform: nil)
                    @dtype = :u64
                    @na_value = nil

                    super
                end

                def resolve(time, _sample)
                    value = time.tv_sec * 1_000_000 + time.tv_usec
                    apply_value_transform(value)
                end
            end
        end
    end
end
