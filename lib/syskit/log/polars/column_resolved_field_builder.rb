# frozen_string_literal: true

module Syskit
    module Log
        module Polars
            # @api private
            #
            # Dispatch of a stream's field data into a single column
            class ColumnResolvedFieldBuilder < ColumnBuilder
                def initialize(
                    name:, path:, type:, value_transform:, global_transform:
                )
                    @path = path
                    @type = type
                    super(name: name, value_transform: value_transform,
                          global_transform: global_transform)
                end

                def resolve(_time, value)
                    v = @path.resolve(value).first.to_ruby
                    apply_value_transform(v)
                end

                def na_value
                    Float::NAN if @type <= Typelib::NumericType && !@type.integer?
                end

                POLARS_DTYPES_INTEGER_UNSIGNED = [
                    ::Polars::UInt8, ::Polars::UInt16, ::Polars::UInt32, ::Polars::UInt64
                ].freeze
                POLARS_DTYPES_INTEGER_SIGNED = [
                    ::Polars::UInt8, ::Polars::UInt16, ::Polars::UInt32, ::Polars::UInt64
                ].freeze
                POLARS_DTYPES_FLOAT = [::Polars::Float32, ::Polars::Float64] .freeze

                def dtype
                    return ::Polars::Object unless @type <= Typelib::NumericType

                    type_size_bits = @type.size.bit_length
                    if @type.integer?
                        if @type.unsigned?
                            POLARS_DTYPES_INTEGER_UNSIGNED.at(type_size_bits - 1)
                        else
                            POLARS_DTYPES_INTEGER_SIGNED.at(type_size_bits - 1)
                        end
                    else
                        POLARS_DTYPES_FLOAT.at(type_size_bits - 3)
                    end
                end
            end
        end
    end
end
