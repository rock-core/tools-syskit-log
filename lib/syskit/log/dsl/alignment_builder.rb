# frozen_string_literal: true

module Syskit
    module Log
        module DSL
            # Representation of a transformation from a stream that is being aligned
            # into a processed stream
            class AlignmentBuilder
                # AlignmentBuilder-specific PathBuilder that allows to change metadata
                class PathBuilder < DSL::PathBuilder
                    def initialize(
                        type, metadata, name = "", path = ::Typelib::Path.new([]),
                        transform = nil
                    )
                        super(type, name, path, transform)

                        @metadata = metadata
                    end

                    def __metadata
                        @metadata
                    end

                    def __new(type, name, path, transform = nil)
                        PathBuilder.new(type, @metadata, name, path, transform)
                    end
                end

                # @param [#upgrade_ops_for_target] upgrade_with return an object that
                #   convert the stream type into its latest representation on the system
                def initialize(type, metadata, upgrade_with: nil)
                    @stream_type = type
                    @metadata = metadata
                    @output_streams = {}
                    @upgrade = false
                    @upgrade_with = upgrade_with
                end

                def type
                    return @type if @type
                    return (@type = @stream_type) unless upgrade?

                    @type = Roby.app.default_loader.resolve_type(
                        @stream_type.name
                    )
                end

                # Controls whether the samples from this stream should be upgraded to
                # their latest representation during processing, or left as-is
                #
                # @see upgrade?
                def upgrade(flag)
                    return if @upgrade == flag

                    if @type
                        raise "cannot change the upgrade flag, the stream type has "\
                              "already been accessed"
                    end

                    @upgrade = flag
                    self
                end

                # Whether the samples from this stream should be upgraded to
                # their latest representation during processing, or left as-is
                #
                # @see upgrade=
                def upgrade?
                    @upgrade
                end

                def prepare_upgrade_if_needed
                    return unless upgrade?

                    unless @upgrade_with
                        raise "cannot upgrade, no upgrade object provided"
                    end

                    @upgrade_ops = @upgrade_with.upgrade_ops_for_target(type)
                end

                # @api private
                #
                # Helper that resolves a field from the block given to {#add} and {#time}
                #
                # @return [ResolvedField]
                def resolve_output_stream
                    builder = PathBuilder.new(type, @metadata)
                    builder = yield(builder) if block_given?
                    OutputStream.new(builder.__name, builder.__path,
                                     builder.__type,
                                     builder.__metadata,
                                     builder.__transform)
                end

                def each_output_stream(&block)
                    @output_streams.each_value(&block)
                end

                def process(time, sample)
                    return enum_for(__method__, time, sample) unless block_given?

                    if upgrade?
                        upgraded_sample = type.new
                        @upgrade_ops.call(upgraded_sample, sample)
                        sample = upgraded_sample
                    end

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

                OutputStream = Struct.new :name, :path, :type, :metadata, :transform do
                    def resolve(_time, value)
                        v = path.resolve(value).first.to_ruby
                        transform ? transform.call(v) : v
                    end
                end
            end
        end
    end
end
