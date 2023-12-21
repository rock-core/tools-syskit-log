# frozen_string_literal: true

module Syskit
    module Log
        module DSL
            # Representation of a transformation from a stream that is being aligned
            # into a processed stream
            class AlignmentBuilder
                def initialize(type)
                    @type = type
                    @output_streams = {}
                end

                # @api private
                #
                # Helper that resolves a field from the block given to {#add} and {#time}
                #
                # @return [ResolvedField]
                def resolve_output_stream
                    builder = PathBuilder.new(@type)
                    builder = yield(builder) if block_given?
                    OutputStream.new(builder.__name, builder.__path,
                                     builder.__type,
                                     builder.__transform)
                end

                def each_output_stream(&block)
                    @output_streams.each_value(&block)
                end

                def process(time, sample)
                    return enum_for(__method__, time, sample) unless block_given?

                    each_output_stream do |s|
                        yield [time, s.resolve(time, sample)]
                    end
                end

                # Add a stream to the alignment output, optionally transforming the data
                #
                # @param [String] the output stream name. It must be unique for a
                #    complete alignment
                # @yieldparam [PathBuilder] an object that allows to extract specific
                #    fields and/or apply transformations before the value gets
                #    stored in the frame
                def add(name, &block)
                    if @output_streams.key?(name)
                        raise ArgumentError, "there is already a stream called #{name}"
                    end

                    resolved = resolve_output_stream(&block)
                    resolved.name = name
                    @output_streams[resolved.name] = resolved
                    resolved
                end

                OutputStream = Struct.new :name, :path, :type, :transform do
                    def resolve(_time, value)
                        v = path.resolve(value).first.to_ruby
                        transform ? transform.call(v) : v
                    end
                end
            end
        end
    end
end
