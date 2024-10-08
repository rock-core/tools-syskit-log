# frozen_string_literal: true

module Syskit
    module Log
        module Polars
            # @api private
            #
            # Dispatch of a stream's field data into a single column
            class ColumnResolvedFieldBuilder < ColumnBuilder
                NA_UNSET = Object.new

                def initialize( # rubocop:disable Metrics/ParameterLists
                    name:, path:, type:, value_transform:, global_transform:,
                    dtype: nil
                )
                    if dtype && !dtype.respond_to?(:to_sym)
                        raise ArgumentError,
                              "'dtype' must be given in symbol form (e.g. :f32 "\
                              "instead of Polars::Float32)"
                    end

                    @path = path
                    @type = type
                    @dtype =
                        dtype || ColumnResolvedFieldBuilder.dtype_from_typelib_type(type)
                    @na_value =
                        ColumnResolvedFieldBuilder.na_value_from_dtype(@dtype)

                    super(name: name, value_transform: value_transform,
                          global_transform: global_transform)
                end

                def resolve(_time, value)
                    v = @path.resolve(value).first.to_ruby
                    apply_value_transform(v)
                end

                def self.na_value_from_dtype(dtype)
                    @na_value = (Float::NAN if dtype.to_s.start_with?("f"))
                end

                def self.dtype_from_typelib_type(type)
                    return ::Polars::Object unless type <= Typelib::NumericType

                    category =
                        if type.integer?
                            if type.unsigned?
                                "u"
                            else
                                "i"
                            end
                        else
                            "f"
                        end

                    "#{category}#{type.size * 8}"
                end
            end
        end
    end
end
