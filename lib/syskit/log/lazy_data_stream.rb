# frozen_string_literal: true

module Syskit::Log
    # Placeholder for Pocolog::DataStream that does not load any actual data /
    # index from the stream.
    #
    # It is used to manipulate the streams in the infrastructure/modelling phase
    # while only loading when actually needed
    #
    # To simplify the data management, it requires the stream to be bound to a
    # single file, which is done with 'syskit pocolog normalize'
    class LazyDataStream
        # The path to the streams' backing file
        #
        # @return [Pathname]
        attr_reader :path

        # The path to the index directory
        #
        # @return [Pathname]
        attr_reader :index_dir

        # The stream name
        #
        # @return [String]
        attr_reader :name

        # The stream type
        #
        # @return [Typelib::Type]
        attr_reader :type

        # The stream metadata
        #
        # @return [Hash]
        attr_accessor :metadata

        # The size, in samples, of the stream
        #
        # @return [Integer]
        attr_reader :size

        # The realtime interval
        attr_reader :interval_rt

        # The logical-time interval
        attr_reader :interval_lg

        def initialize(path, index_dir, name, type, metadata, interval_rt, interval_lg, size)
            @path = path
            @index_dir = index_dir
            @name = name
            @type = type
            @metadata = metadata
            @interval_rt = interval_rt
            @interval_lg = interval_lg
            @size = size
            @pocolog_stream = nil
        end

        def task_name
            metadata["rock_task_name"]
        end

        def task_object_name
            metadata["rock_task_object_name"]
        end

        # True if the size of this stream is zero
        def empty?
            size == 0
        end

        # The underlying typelib registry
        def registry
            type.registry
        end

        # Method used when the stream's data is actually needed
        #
        # @return [Pocolog::DataStream]
        def syskit_eager_load
            return @pocolog_stream if @pocolog_stream

            file = Pocolog::Logfiles.open(
                Syskit::Log.decompressed(path, index_dir).to_s,
                index_dir: index_dir
            )
            @pocolog_stream = file.streams.first
        end

        # Return an object that allows to enumerate this stream's samples
        #
        # This causes the stream to be actually loaded
        #
        # @return [Pocolog::SampleEnumerator]
        def samples
            syskit_eager_load.samples
        end

        # Return a data stream that starts at the given time
        def from_logical_time(time)
            syskit_eager_load.from_logical_time(time)
        end

        # Return a data stream that starts at the given time
        def to_logical_time(time)
            syskit_eager_load.to_logical_time(time)
        end

        # Enumerate the stream's samples, not converting the values to their Ruby equivalent
        def raw_each(&block)
            syskit_eager_load.raw_each(&block)
        end

        # Enumerate the stream's samples, converting the values to their Ruby equivalent
        def each(&block)
            syskit_eager_load.each(&block)
        end

        def duration_lg
            if interval_lg.empty?
                0
            else
                interval_lg[1] - interval_lg[0]
            end
        end
    end
end
