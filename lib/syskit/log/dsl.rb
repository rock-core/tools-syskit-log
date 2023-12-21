# frozen_string_literal: true

require "syskit/log"
require "syskit/log/daru"
require "syskit/log/datastore"
require "syskit/log/dsl/maps"
require "syskit/log/dsl/summary"
require "syskit/log/dsl/path_builder"
require "syskit/log/dsl/alignment_builder"
require "syskit/log/dsl/export_to_single_file"

module Syskit
    module Log
        # Implementation of high-level functionality for log inspection and analysis
        #
        # It is specifically designed to be used in a Jupyter notebook using IRuby,
        # but (almost) all the functionality can also be used in simple Ruby scripts
        #
        # A template notebooks can be found in this repository's
        # [examples/](https://github.com/rock-core/tools-syskit-log/tree/master/examples)
        module DSL
            def self.extend_object(object)
                super
                object.__syskit_log_dsl_initialize
            end

            # @api private
            #
            # Method called when {DSL} is included, to initialize its instance
            # variables
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
            # The method resets the interval and zero time to the dataset's interval
            #
            # @param [String,Hash] query if a string, this is interpreted as a
            #   dataset digest. Otherwise, it is used as a metadata query and
            #   given to {Datastore#find}
            #
            # If more than one dataset matches the given query, a widget is shown
            # to select it interactively
            #
            # The returned value will display summary information about the
            # dataset and its various streams. When in a jupyter notebook, it
            # is recommended to call {#dataset_select} last in a cell to have that
            # information displayed
            #
            # @see #dataset
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

            # @api private
            #
            # Ask the user to select a dataset among candidates. Nothing is
            # shown if the set of candidates has exactly one element
            def __dataset_user_select(candidates)
                this = self
                candidates =
                    candidates.sort_by(&:timestamp).each_with_object({}) do |dataset, h|
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

                    "#{k}: #{v.to_a.compact.sort.join(', ')}"
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

            # Select the whole dataset as the interval
            def interval_select_whole(reset_zero: true)
                interval_select(*@dataset.interval_lg, reset_zero: reset_zero)
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
                if (interval = __try_resolve_interval(obj))
                    interval[interval_index]
                elsif (result = __try_resolve_timepoint(obj))
                    result
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

            # Reset sampling intervals
            def interval_sample_everything
                @interval_sample_by_sample = nil
                @interval_sample_by_time = nil
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
            # @return [Float] time relative to the current interval's zero time
            def interval_map_time(time)
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
            #
            # @param [Array] streams an array if objects that can be converted to
            #   samples using {#samples_of}
            # @param [Boolean] accurate prefer accuracy over speed (see below)
            # @param [Float,nil] timeout how long, since the last received sample
            #   from a stream, the method will start introducing NAs to replace
            #   the stream's values (NA is either NAN for float values, or nil)
            # @yield a {FrameBuilder} object used to describe the frame to be built
            #
            # This method uses the first given stream as a "master" stream, and
            # attempts to load the value of the remaining columns at the same
            # real time than the value from the master stream.
            #
            # How the method deals with resampling (when {#interval_sample_every} has
            # been called) depends on the `accurate` parameter. When `false`,
            # the streams are first re-sampled and then aligned. When doing coarse
            # sampling, this can introduce significant misalignments. When true,
            # the method resamples only the first stream, and then aligns the
            # other full non-resampled streams. accurante: false is significantly
            # faster for very dense streams (w.r.t. the sampling period)
            def to_daru_frame(*streams, accurate: false, timeout: nil)
                return ::Daru::DataFrame.new if streams.empty?

                interval_start, interval_end = streams.map(&:interval_lg).transpose
                interval_start = interval_start.min
                interval_end = interval_end.max
                interval_start = [interval_start, @interval[0]].max if @interval[0]
                interval_end = [interval_end, @interval[1]].min if @interval[1]

                if accurate
                    first_samples =
                        samples_of(streams[0], from: interval_start, to: interval_end)

                    samples = [first_samples] + streams[1..-1].map do |s|
                        samples_of(s, from: interval_start, to: interval_end,
                                      every_samples: nil, every_seconds: nil)
                    end
                else
                    samples = streams.map do |s|
                        samples_of(s, from: interval_start, to: interval_end)
                    end
                end

                builders = streams.map { |s| Daru::FrameBuilder.new(s.type) }
                yield(*builders)

                @interval_zero_time ||= streams.first.interval_lg[0]

                if builders.size == 1
                    builders.first.to_daru_frame(
                        @interval_zero_time, samples.first, timeout: timeout
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
            def samples_of(
                stream, from: @interval[0], to: @interval[1],
                every_samples: @interval_sample_by_sample,
                every_seconds: @interval_sample_by_time
            )
                stream = stream.syskit_eager_load if stream.syskit_eager_load
                stream = stream.from_logical_time(from) if from
                stream = stream.to_logical_time(to + TIME_EPSILON) if to

                if every_samples
                    stream = stream.resample_by_index(every_samples)
                end

                if every_seconds
                    stream = stream.resample_by_time(every_seconds)
                end
                stream
            end

            # Access the information contained in the Roby index
            #
            # @return [RobySQLIndex::Accessors::Root]
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
                        df.add_time_field("time", &block)
                    else
                        df.add_logical_time
                    end
                end
                frame["time"]
            end

            # Generic entry point to see information about an object
            def summarize(object, **options)
                DSL::Summary.new(object, interval_zero_time, **options)
            end

            # Sample period information about a port or all ports of a task
            def periods(stream)
                time_vector_of(stream).summary
            end

            # Render a timeline of the given tasks, restricted to the current
            # interval
            def roby_task_timeline(*tasks)
                tasks = tasks.flat_map do |t|
                    if t.respond_to?(:each_task)
                        t.each_task.to_a
                    else
                        t
                    end
                end

                data = roby_vega_task_timeline_data(*tasks)
                Vega.lite
                    .data(data)
                    .mark(type: "bar")
                    .encoding(y: { field: "task", type: "ordinal" },
                              x: { field: "start", type: "quantitative",
                                   scale: { zero: false } },
                              x2: { field: "stop" },
                              color: { field: "model" })
            end

            # Transform an array of task instances into a vega data array
            #
            # The returned array has the 'task' (task name) and 'start', 'stop'
            # fields (relative times)
            def roby_vega_task_timeline_data(*tasks)
                tasks.filter_map do |t|
                    start, stop = interval_map_intersection(
                        t.start_event.first.time,
                        t.stop_event.first.time
                    )
                    next unless start

                    {
                        task: "#{t.id}: #{t.model.name}", model: t.model.name,
                        start: start, stop: stop
                    }
                end
            end

            # Convert a Daru frame into a vega data array
            def daru_to_vega(frame, every: 1)
                data = []
                frame.each_row.each_with_index do |row, i|
                    data << row.to_h if i % every == 0
                end

                keys = data.first.keys
                frame.each_vector_with_index do |v, _|
                    keys.delete(v) if v.dtype == :gsl
                end

                float_keys = []
                data.each do |sample|
                    sample.each do |k, v|
                        float_keys << k if v.respond_to?(:nan?)
                        keys.delete(k) unless v.nil?
                    end
                    break if keys.empty?
                end

                data.each do |h|
                    float_keys.each do |k|
                        h[k] = nil if h[k]&.nan?
                    end
                end
            end

            # Mark a vega data array with a string that indicates the tasks
            # that is running during its execution
            #
            # @param [String,Symbol] field_name the field that should be assigned
            # @param [Array<Hash>] data the data array
            # @param [Array<Accessors::Task,Accessors::TaskModel>] tasks the tasks
            #   to mark
            # @param [Symbol] time_field the field that holds the data's time,
            #   relative to interval_start (as generated by e.g. {#to_daru_frame})
            def roby_vega_mark_tasks(field_name, data, *tasks, time_field:)
                tasks = tasks.flat_map do |t|
                    if t.respond_to?(:each_task)
                        t.each_task.to_a
                    else
                        t
                    end
                end

                intervals = tasks.filter_map do |t|
                    start = t.start_event.first&.time
                    stop  = t.stop_event.first&.time
                    next unless start && stop

                    start -= interval_zero_time
                    stop  -= interval_zero_time
                    [t, start, stop]
                end

                intervals = intervals.sort_by { |_, start, _| start }
                current_task, current_start, current_stop = intervals.shift
                data.each do |h|
                    time = h[time_field]
                    while !intervals.empty? && current_stop < time
                        current_task, current_start, current_stop = intervals.shift
                    end

                    h[field_name] =
                        if current_start <= time
                            roby_vega_make_task_name(current_task)
                        end
                end
            end

            def roby_vega_make_task_name(task)
                "#{task.id}: #{task.model.name}"
            end

            # Create a reusable view to plot data using Vega lite
            #
            # It creates a x/y plot, with an optional ordering field (time).
            # All given coordinates (x/y/time) are assumed to be quantitative
            #
            # The returned value is a Vega spec object, which can be further tuned
            #
            # @example layered line and circle X/Y plot of power vs. time
            #   line = vega_simple_view(x: "time", y: "power")
            #   circle = vega_simple_view(x: "time", y: "power").mark("circle")
            #   Vega.lite.layer([line, circle])
            #
            # @example repeat line X/Y plot of speed and power vs. time
            #   line = vega_simple_view(x: "time", y: { repeat: "repeat" })
            #   Vega.lite.data(data).repeat(%w[speed power]).spec(line)
            #
            # @see daru_to_vega vega_simple_plot
            def vega_simple_view(x:, y:, time: nil, mark: "line", color: y, width: 800) # rubocop:disable Metrics/ParameterLists
                order =
                    if time
                        { order: { field: time, type: "quantitative" } }
                    else
                        {}
                    end

                color =
                    if color
                        { color: { datum: color.to_s } }
                    else
                        {}
                    end

                Vega.lite
                    .mark(type: mark)
                    .encoding(
                        x: { field: x, type: "quantitative", scale: { zero: false } },
                        y: { field: y, type: "quantitative", scale: { zero: false } },
                        **order, **color
                    )
                    .width(width)
            end

            # Create a reusable view to plot geo data using Vega lite
            #
            # It creates a latitude/longitude plot, with an optional ordering
            # field (time).  All given coordinates (latitude/longitude/time) are
            # assumed to be quantitative
            #
            # The returned value is a Vega spec object, which can be further tuned
            #
            # @example plot a trajectory
            #   Vega.lite.layer([vega_simple_view(lat: "lat", lon: "lon")])
            #
            # @see daru_to_vega vega_simple_plot
            def vega_geo_view(lat:, lon:, time: nil, mark: "line")
                order =
                    if time
                        { order: { field: time, type: "quantitative" } }
                    else
                        {}
                    end

                Vega.lite
                    .mark(type: mark)
                    .encoding(
                        latitude: { field: lat, type: "quantitative",
                                    scale: { zero: false } },
                        longitude: { field: lon, type: "quantitative",
                                     scale: { zero: false } },
                        **order
                    )
            end

            # @api private
            #
            # Checks whether a given object looks like Vega data
            def vega_data?(data)
                data.respond_to?(:to_ary) && data[0].respond_to?(:to_hash)
            end

            # Do a layered plot of multiple parameters
            #
            # It creates a x/y plot, with an optional ordering field (time).
            # All given coordinates (x/y/time) are assumed to be quantitative.
            #
            # The returned value is a Vega spec object, which can be further tuned
            #
            # @example layered line and circle X/Y plot of power vs. time
            #   line = vega_simple_plot(x: "time", y: "power")
            #   circle = vega_simple_view(x: "time", y: "power").mark("circle")
            #   Vega.lite.layer([line, circle])
            #
            # @example repeat line X/Y plot of speed and power vs. time
            #   line = vega_simple_view(x: "time", y: { repeat: "repeat" })
            #   Vega.lite.data(data).repeat(%w[speed power]).spec(line)
            #
            # @see daru_to_vega vega_simple_plot
            def vega_simple_plot(data, x:, y:, time: nil, mark: "line")
                data = daru_to_vega(data) unless vega_data?(data)
                spec = vega_simple_view(
                    x: x, y: { repeat: "layer" },
                    time: time, mark: mark
                )
                Vega.lite
                    .data(data)
                    .repeat(layer: Array(y))
                    .spec(spec)
            end

            # Wrapper for a raw string to pass it to iruby as raw HTML
            class RawHTML
                def initialize(html)
                    @html = html
                end

                def to_s
                    @html
                end

                def to_html
                    @html
                end
            end

            def raw_html(html)
                RawHTML.new(html)
            end

            # Export streams from the dataset into a new multi-stream log file
            #
            # @param [Array] streams an array of objects that can be converted to
            #   samples using {#samples_of}
            # @yield a {DSL::AlignmentBuilder} object used to describe the streams to be
            #   saved
            def export_to_single_file(path, *streams)
                log_file = Pocolog::Logfiles.create(path.to_s)
                return if streams.empty?

                interval_start, interval_end = streams.map(&:interval_lg).transpose
                interval_start = interval_start.min
                interval_end = interval_end.max
                interval_start = [interval_start, @interval[0]].max if @interval[0]
                interval_end = [interval_end, @interval[1]].min if @interval[1]

                samples = streams.map do |s|
                    samples_of(s, from: interval_start, to: interval_end)
                end

                builders = streams.map { |s| DSL::AlignmentBuilder.new(s.type) }
                yield(*builders)

                log_file_streams =
                    DSL.export_to_single_file_create_output_streams(log_file, builders)
                joint_stream = Pocolog::StreamAligner.new(false, *samples)
                DSL.export_to_single_file_process(
                    builders, log_file_streams, joint_stream
                )
            rescue ::Exception # rubocop:disable Lint/RescueException
                path.unlink
                raise
            ensure
                log_file&.close
            end
        end
    end
end
