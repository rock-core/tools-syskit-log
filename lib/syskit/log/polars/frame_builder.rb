# frozen_string_literal: true

module Syskit
    module Log
        module Polars # :nodoc:
            def self.allocate_series(name, size, type)
                ::Polars::Series.new(name, [nil] * size, dtype: type)
            end

            # Dispatch of samples from a single stream towards the different columns
            class FrameBuilder
                def initialize(type)
                    @type = type
                    @fields = []
                    @time_fields = []
                end

                # Tests whether there is already a column with a given name
                def column?(name)
                    @fields.any? { |f| f.name == name }
                end

                # This builder's column names
                def column_names
                    @fields.map(&:name)
                end

                # Save the stream's logical time in the given column
                def add_logical_time(name = "time")
                    add_resolved_field(ColumnLogicalTimeBuilder.new(name: name))
                    @time_fields << (@fields.size - 1)
                end

                # Add a field that will be interpreted as time and shifted by center_time
                #
                # The field must represent microseconds in the same frame than
                # center_time
                def add_time_field(name = nil, &block)
                    add(name, &block)
                    @time_fields << (@fields.size - 1)
                end

                # Extract a field as a column in the resulting frame
                #
                # @param [String,nil] the column name. If it is not given, the column
                #    name is generated from the extracted fields (see below).
                # @yieldparam [PathBuilder] an object that allows to extract specific
                #    fields and/or apply transformations before the value gets
                #    stored in the frame
                def add(name = nil, dtype: nil, &block)
                    raise ArgumentError, "a block is required" unless block_given?

                    resolved = resolve_field(name: name, dtype: dtype, &block)
                    add_resolved_field(resolved)
                end

                # @api private
                #
                # Register a resolved field
                #
                # @raise ArgumentError if the field's name is a duplicate
                def add_resolved_field(resolved)
                    if column?(resolved.name)
                        raise ArgumentError, "field #{name} already defined"
                    end

                    @fields << resolved
                    resolved
                end

                class InvalidDataType < ArgumentError; end

                # @api private
                #
                # Helper that resolves a field from the block given to {#add} and {#time}
                #
                # @return [ColumnBuilder]
                def resolve_field(name: nil, dtype: nil)
                    builder = yield(PathBuilder.new(@type))
                    unless builder.__terminal?
                        raise InvalidDataType,
                              "field resolved to type #{builder.__type}, "\
                              "which is not simple nor transformed"
                    end
                    ColumnResolvedFieldBuilder.new(
                        name: name || builder.__name, path: builder.__path,
                        type: builder.__type,
                        value_transform: builder.__transform,
                        global_transform: builder.__vector_transform,
                        dtype: dtype
                    )
                end

                def create_series(chunks)
                    @fields.zip(chunks).map { |f, a| f.create_series(a) }
                end

                # @api private
                def create_chunks(size)
                    @fields.map { Array.new(size) }
                end

                # @api private
                #
                # Return the array of N/A values for each vectors in this frame
                def na_values
                    @na_values ||= @fields.map(&:na_value)
                end

                # @api private
                #
                # Called during resolution to update a data row
                def update_row(vectors, row, time, sample)
                    @fields.each_with_index do |f, i|
                        vectors[i][row] = f.resolve(time, sample)
                    end
                end

                # @api private
                #
                # Set this row to N/A
                #
                # @param vectors the object returned by {#create_vectors}
                # @param [Integer] row the row index
                # @param na the array of N/A values as created by na_values
                def update_row_na(chunks, row)
                    chunks.zip(na_values).each do |a, v|
                        a[row] = v
                    end
                end

                # @api private
                #
                # Apply the center time on a time field if there is one
                def recenter_time_series(series, center_time)
                    center_time_usec =
                        center_time.tv_sec * 1_000_000 + center_time.tv_usec

                    @time_fields.each do |field_index|
                        field_name = @fields[field_index].name
                        s = (series[field_name] - center_time_usec)
                        series[field_name] = s.cast(:f64) / 1_000_000
                    end
                end

                # Convert the registered fields into a Daru frame
                #
                # @param [Time] center_time the time that should be used as
                #   zero in the frame index
                # @param [#raw_each] samples the object that will enumerate samples
                #   It must yield [realtime, logical_time, sample] the way
                #   Pocolog::SampleEnumerator does
                def to_polars_frame(
                    center_time, stream, timeout: nil, chunk_size: CHUNK_SIZE
                )
                    Polars.create_aligned_frame(
                        center_time, [self], SingleStreamAdapter.new(stream),
                        timeout: timeout, chunk_size: chunk_size
                    )
                end

                # @api private
                #
                # Adapter to provide a StreamAligner-like interface compatible
                # with daru's building procedure for a single pocolog stream
                class SingleStreamAdapter
                    def initialize(stream)
                        @stream = stream
                    end

                    def raw_each
                        @stream.raw_each do |_, lg, sample|
                            yield(0, lg, sample)
                        end
                    end
                end
            end
        end
    end
end
