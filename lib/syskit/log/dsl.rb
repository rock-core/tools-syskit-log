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
                matches =
                    if query.respond_to?(:to_str)
                        [datastore.get(query)]
                    elsif query
                        datastore.find_all(query)
                    else
                        datastore.each_dataset.to_a
                    end

                if matches.size == 1
                    @dataset = matches.first
                elsif matches.empty?
                    raise ArgumentError, "no dataset matches '#{query}'"
                else
                    @dataset = __dataset_user_select(matches)
                end

                interval_select(*@dataset.interval_lg) unless @dataset.empty?
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

            INTERVAL_DEFAULT_GROW = 30

            # Select the time interval that will be processed
            #
            # The different forms of this method will attempt to resolve its arguments
            # as either timepoints or intervals, following these rules:
            #
            # * Time objects are timepoints (obviously)
            # * Roby's emitted events (RobySQLIndex::Accessors::EventEmission)
            #   are timepoints
            # * when Roby's event and event models are given, a widget is displayed
            #   to pick an emission, which is then used as a timepoint
            # * Roby tasks are resolved as intervals, using their start and stop events
            # * when a Roby task model is given, a widget is displayed
            #   to pick an instance, which is resolved as an interval
            # * a data stream (e.g. `example_task.out_port`) is interpreted as an
            #   interval, based on its first and last samples
            #
            # @param [Float] grow grow the resulting interval by this many seconds
            #   The default is dependent on the object used to select the interval
            #   (see below)
            # @param [Boolean] reset_zero whether the current zero should be kept
            #   (false), or set to the interval start (false)
            #
            # @overload interval_select(from, to, reset_zero: true)
            #   @param from either a timepoint or an interval. In the latter case,
            #      the interval's min time is used
            #   @param to either a timepoint or an interval. In the latter case,
            #      the interval's max time is used
            #
            # @overload interval_select(interval, reset_zero: true, grow: 0)
            #   @param interval set the interval. If given a timepoint, it creates
            #      an interval around this timepoint
            #
            # @overload interval_select(timepoint, reset_zero: true, grow: 30)
            #   @param timepoint set an interval of `grow` seconds around the given
            #      timepoint
            #
            def interval_select(*args, reset_zero: true, grow: nil)
                if args.size == 2
                    @base_interval = [
                        __resolve_timepoint(args[0], 0),
                        __resolve_timepoint(args[1], 1)
                    ]
                    grow ||= 0
                elsif args.size == 1
                    @base_interval = __resolve_interval(args[0])
                    single_time = @base_interval[0] == @base_interval[1]
                    grow ||= single_time ? INTERVAL_DEFAULT_GROW : 0
                else
                    raise ArgumentError, "expected 1 or 2 arguments, got #{args.size}"
                end

                interval_reset(reset_zero: reset_zero)
                interval_grow(grow)
            end
            # @api private
            #
            # Try to resolve the given object as a time
            #
            # @return [Time,nil] the time, or nil if the object cannot
            #   be interpreted as a time
            # @see __resolve_timepoint
            def __try_resolve_timepoint(obj)
                if obj.respond_to?(:each_emission)
                    __select_emitted_event(obj.each_emission.to_a).time
                elsif obj.respond_to?(:time)
                    obj.time
                elsif obj.kind_of?(Time)
                    obj
                end
            end

            # @api private
            #
            # Resolve the given object as a time point
            #
            # @param [Integer] interval_index when deducing a timepoint from an
            #   interval, the index in the interval array (either 0 or 1)
            # @return [Time] the time
            # @raise ArgumentError if the object cannot be interpreted as a timepoint
            # @see __try_resolve_timepoint
            def __resolve_timepoint(obj, interval_index)
                if (result = __try_resolve_timepoint(obj))
                    result
                elsif (interval = __try_resolve_interval(obj))
                    interval[interval_index]
                else
                    raise ArgumentError, "cannot resolve #{obj} as a timepoint"
                end
            end

            # @api private
            #
            # Try to resolve the given object as a time interval
            #
            # @return [(Time,Time),nil] the interval, or nil if the object cannot
            #   be interpreted as an interval
            # @see __resolve_interval
            def __try_resolve_interval(obj)
                case obj
                when RobySQLIndex::Accessors::TaskModel
                    obj = __select_task(obj.each_task.to_a)
                end

                obj.interval_lg if obj.respond_to?(:interval_lg)
            end

            # @api private
            #
            # Resolve the given object as a time interval
            #
            # @return [(Time,Time)] the interval
            # @raise ArgumentError if the if the object cannot
            #   be interpreted as an interval
            # @see __try_resolve_interval
            def __resolve_interval(obj)
                if (interval = __try_resolve_interval(obj))
                    interval
                elsif (timepoint = __try_resolve_timepoint(obj))
                    [timepoint, timepoint]
                else
                    raise ArgumentError, "cannot resolve #{obj} as an interval"
                end
            end

            # Pick the zero time
            def interval_select_zero_time(time)
                @interval_zero_time = time
            end

            # Grow the current interval by this many seconds
            def interval_grow(seconds)
                @interval[0] -= seconds
                @interval[1] += seconds
                @interval
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

            # Map a Time object into the interval value
            #
            # @return [nil,Integer] nil if the time is outside the interval's bounds,
            #   the relative time otherwise
            def interval_map_time(time)
                return if time < @interval[0] || time > @interval[1]

                time - @interval_zero_time
            end

            # Map the intersection of a time interval with the currently selected interval
            #
            # @return [nil,Integer] nil if the time is outside the interval's bounds,
            #   the relative time otherwise
            def interval_map_intersection(start, stop)
                return if stop < interval_start || start > interval_end

                min = interval_start - interval_zero_time
                max = interval_end - interval_zero_time
                [
                    [start - interval_zero_time, min].max,
                    [stop - interval_zero_time, max].min
                ]
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

            TIME_EPSILON = 1/1_000_000r

            # Convert fields of a data stream into a Daru frame
            def to_daru_frame(*streams, timeout: nil)
                interval_start, interval_end = streams.map(&:interval_lg).transpose
                interval_start = interval_start.min
                interval_end = interval_end.max
                interval_start = [interval_start, @interval[0]].max if @interval[0]
                interval_end = [interval_end, @interval[1]].min if @interval[1]

                samples = streams.map do |s|
                    samples_of(s, from: interval_start, to: interval_end)
                end
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

            # Restricts a data stream to the current interval and sample selection
            #
            # @see interval_select interval_shift_start interval_shift_end
            def samples_of(stream, from: @interval[0], to: @interval[1])
                stream = stream.syskit_eager_load if stream.syskit_eager_load
                stream = stream.from_logical_time(from) if from
                stream = stream.to_logical_time(to + TIME_EPSILON) if to

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

            # @api private
            #
            # Spawn a form to select an event emission among candidates
            def __select_task(candidates)
                raise ArgumentError, "found no tasks" if candidates.empty?
                return candidates.first if candidates.size == 1

                candidates = candidates.each_with_object({}) do |task, h|
                    format = "#{task.id} #{task.interval_lg[0]} #{task.interval_lg[1]}"
                    h[format] = task
                end

                result = IRuby.form do
                    radio(:selected_task, *candidates.keys)
                    button
                end
                candidates.fetch(result[:selected_task])
            end

            # @api private
            #
            # Spawn a form to select an event emission among candidates
            def __select_emitted_event(candidates)
                raise ArgumentError, "found no event emissions" if candidates.empty?
                return candidates.first if candidates.size == 1

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
                DSL::Summary.new(object, interval_zero_time)
            end

            # Sample period information about a port or all ports of a task
            def periods(stream)
                time_vector_of(stream).summary
            end
        end
    end
end
