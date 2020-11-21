# frozen_string_literalr: true

require "syskit/log"
require "syskit/log/daru"
require "syskit/log/datastore"
require "syskit/log/dsl/summary"

module Syskit
    module Log
        # Toplevel module that defines a DSL for log manipulation in e.g. Jupyter
        # notebooks
        #
        # All the DSL methods are prefixed with `ds_`
        module DSL
            def self.extend_object(object)
                super
                object.__syskit_log_dsl_initialize
            end

            def __syskit_log_dsl_initialize
                @datastore = Datastore.default if Datastore.default_defined?
                @interval = []
                @streams = {}
            end

            # Select a specific datastore
            #
            # By defautl, the default datastore is used (as defined by the
            # SYSKIT_LOG_STORE environment variable)
            def datastore_select(path)
                @datastore = Datastore.new(Pathname(path))
            end

            # The current datastore
            #
            # This is the default datastore unless {#datastore_select} has been
            # called
            attr_reader :datastore

            # The current dataset as selectedby {#dataset_select}
            attr_reader :dataset

            # The configured interval
            #
            # Sample enumeration performed through {#samples_of} (as for instance
            # in #to_daru_frame) will restrict themselves to this interval
            #
            # @return [nil,(Time,Time)]
            attr_reader :interval

            # Select the curent dataset
            #
            # @param [String,Hash] query if a string, this is interpreted as a
            #   dataset digest. Otherwise, it is used as a metadata query and
            #   given to {Datastore#find}
            def dataset_select(query = nil)
                if query.respond_to?(:to_str)
                    @dataset = datastore.get(query)
                    return summarize(@dataset)
                end

                matches =
                    if query
                        datastore.find_all(query)
                    else
                        datastore.each_dataset.to_a
                    end

                if matches.size == 1
                    @dataset = matches.first
                elsif matches.empty?
                    raise ArgumentError, "no dataset matches the given metadata"
                else
                    @dataset = __dataset_user_select(matches)
                end

                summarize(@dataset)
            end

            def __dataset_user_select(candidates)
                this = self
                candidates = candidates.each_with_object({}) do |dataset, h|
                    format = this.__dataset_format(dataset)
                    h[format] = dataset
                end

                result = IRuby.form do
                    radio(:selected_dataset, *candidates.keys)
                    button
                end
                candidates.fetch(result[:selected_dataset])
            end

            def __dataset_format(dataset)
                description = dataset.metadata_fetch_all(
                    "description", "<no description>"
                )
                digest = @datastore.short_digest(dataset)
                format = "% #{digest.size}s"
                description = description
                              .zip([digest])
                              .map { |a, b| "#{format % [b]} #{a}" }
                              .join

                metadata = dataset.metadata.map do |k, v|
                    next if k == "description"

                    "#{k}: #{v.to_a.sort.join(', ')}"
                end.compact.join("; ")

                "#{description} - #{metadata}"
            end

            # Select the time interval that will be processed
            def interval_select(start_time, end_time, reset_zero: true)
                @base_interval = [start_time, end_time]
                interval_reset(reset_zero: reset_zero)
            end

            # Select the time interval that will be processed using the start
            # and end time of a stream
            def interval_select_from_stream(stream, reset_zero: true)
                @base_interval = stream.interval_lg
                interval_reset(reset_zero: reset_zero)
            end

            # Pick the zero time
            def interval_select_zero_time(time)
                @interval_zero_time = time
            end

            # Select how often will a sample be picked by e.g. to_daru_frame
            # or samples_of
            def interval_sample_every(samples: nil, seconds: nil)
                unless (samples && !seconds) || (!samples && seconds)
                    raise ArgumentError, "need exactly one of 'samples' or 'seconds'"
                end

                @interval_sample_by_sample = samples
                @interval_sample_by_time = seconds
            end

            # Reset the interval to the last interval selected by
            # {#interval_select_from_stream}
            def interval_reset(reset_zero: true)
                @interval = @base_interval.dup
                @interval_zero_time = @interval[0] if reset_zero
                @interval
            end

            # The start of the interval
            #
            # @return [Time,nil]
            def interval_start
                @interval[0]
            end

            # The end of the interval
            #
            # @return [Time,nil]
            def interval_end
                @interval[1]
            end

            # The zero time
            attr_reader :interval_zero_time

            # Shift the interval start by that many seconds (fractional),
            # starting at the start of the main selected interval
            #
            # @param [Float] offset
            def interval_shift_start(offset)
                @interval[0] += offset
                @interval
            end

            # Set the interval end to that many seconds after the current start
            #
            # @param [Float] offset
            def interval_shift_end(offset)
                @interval[1] = @interval[0] + offset
                @interval
            end

            # Convert fields of a data stream into a Daru frame
            def to_daru_frame(*streams, timeout: nil)
                samples = streams.map { |s| samples_of(s) }
                builders = streams.map { |s| Daru::FrameBuilder.new(s.type) }
                yield(*builders)

                @interval_zero_time ||= streams.first.interval_lg[0]

                if builders.size == 1
                    builders.first.to_daru_frame(
                        @interval_zero_time, samples, timeout: timeout
                    )
                else
                    joint_stream = Pocolog::StreamAligner.new(false, *samples)
                    Daru.create_aligned_frame(
                        @interval_zero_time, builders, joint_stream,
                        samples.first.size, timeout: timeout
                    )
                end
            end

            # Resolve a sample enumerator from a stream object
            def samples_of(stream)
                stream = stream.syskit_eager_load if stream.syskit_eager_load
                stream = stream.from_logical_time(@interval[0]) if @interval[0]
                stream = stream.to_logical_time(@interval[1]) if @interval[1]
                if @interval_sample_by_sample
                    stream = stream.resample_by_index(@interval_sample_by_sample)
                end
                if @interval_sample_by_time
                    stream = stream.resample_by_time(@interval_sample_by_time)
                end
                stream
            end

            def roby
                RobySQLIndex::Accessors::Root.new(dataset.roby_sql_index)
            end

            # Select the interval before a given event
            #
            # @param [RobySQLIndex::Accessors::Event] accessor
            def around_event(accessor, before: 60, after: 60, zero: true)
                event = __select_emitted_event(accessor)

                interval_select event.time - before, event.time + after
                interval_select_zero_time event.time if zero
            end

            # @api private
            #
            # Selects a single emitted event from an event accessor
            def __select_emitted_event(accessor)
                return accessor if accessor.respond_to?(:time)

                emissions = accessor.each_emission.to_a
                if emissions.empty?
                    raise ArgumentError,
                          "found no emissions for #{event}"
                elsif emissions.size > 1
                    __emitted_event_select(emissions)
                else
                    emissions.first
                end
            end

            # @api private
            #
            # Spawn a form to select an event emission among candidates
            def __emitted_event_select(candidates)
                candidates = candidates.each_with_object({}) do |event, h|
                    format = "#{event.full_name} #{event.time}"
                    h[format] = event
                end

                result = IRuby.form do
                    radio(:selected_event, *candidates.keys)
                    button
                end
                candidates.fetch(result[:selected_event])
            end

            # @api private
            #
            # Find the streams originating from the same task, by its deployed name
            #
            # @return [nil,TaskStreams]
            def __find_task_by_name(name)
                return unless @dataset

                @dataset.streams.find_task_by_name(name)
            end

            def respond_to_missing?(m, include_private = false)
                MetaRuby::DSLs.has_through_method_missing?(
                    self, m,
                    "_task" => "__find_task_by_name"
                ) || super
            end

            # Give access to the streams per-task by calling <task_name>_task
            def method_missing(m, *args, &block)
                MetaRuby::DSLs.find_through_method_missing(
                    self, m, args,
                    "_task" => "__find_task_by_name"
                ) || super
            end

            # Generating timing statistics of the given stream
            def time_vector_of(stream, &block)
                frame = to_daru_frame(stream) do |df|
                    if block
                        df.add_time_field("time", &block) if block
                    else
                        df.add_logical_time
                    end
                end
                frame["time"]
            end

            # Generic entry point to see information about an object
            def summarize(object)
                DSL::Summary.new(object)
            end

            # Sample period information about a port or all ports of a task
            def periods(stream)
                time_vector_of(stream).summary
            end
        end
    end
end
