# frozen_string_literal: true

module Syskit::Log
    # The object that manages the replay itself
    #
    # Deployments register and deregister themselves when started/stopped.
    #
    # There is one per execution engine, which is accessible via
    # {Extensions::ExecutionEngine#pocolog_replay_manager}. The way to add/remove
    # deployment tasks is through {.register} and {.deregister}, which are
    # already automatically called on the deployment's start/stop events.
    class ReplayManager
        # The underlying stream aligner
        #
        # @return [Pocolog::StreamAligner]
        attr_reader :stream_aligner

        # The current logical time
        attr_reader :time

        # The execution engine we should use to run
        attr_reader :execution_engine

        # @api private
        #
        # The realtime base for {#process_in_realtime}
        #
        # The realtime play process basically works by assuming that
        # base_real_time == {#base_logical_time}
        attr_reader :base_real_time

        # @api private
        #
        # The logical time base for {#process_in_realtime}
        #
        # The realtime play process basically works by assuming that
        # base_real_time == {#base_logical_time}
        attr_reader :base_logical_time

        DispatchInfo = Struct.new :deployments, :syskit_stream do
            def in_use?
                !deployments.empty?
            end
        end

        def initialize(
            execution_engine, log_file: File.join(Roby.app.log_dir, "replay.0.log")
        )
            @execution_engine = execution_engine
            @handler_id = nil
            @stream_aligner = Pocolog::StreamAligner.new(false)

            @stream_syskit_to_pocolog = {}
            @dispatch_info = {}
            @log_file = Pocolog::Logfiles.create(log_file)
            @log_streams = {}
        end

        # Time of the first sample in the aligner
        def start_time
            stream_aligner.interval_lg[0]
        end

        # Time of the last sample in the aligner
        def end_time
            stream_aligner.interval_lg[1]
        end

        OutputLog = Struct.new :block_stream, :index_as_buffer, :sample

        # Add the given stream to the replay log
        #
        # @param [Pocolog::DataStream] in_stream the replayed stream
        # @return [Pocolog::OutputLog] information about the stream in the output log
        def add_log_stream(stream)
            output_stream =
                @log_file.create_stream(stream.name, stream.type, stream.metadata)
            OutputLog.new(stream.logfile.block_stream.dup,
                          [output_stream.index].pack("v"),
                          output_stream.type.new)
        end

        # Return the deployment tasks that are "interested by" a given stream
        #
        # @param [Pocolog::DataStream,Syskit::Log::LazyDataStream] stream
        # @return [Array<Syskit::Log::Deployment>]
        def find_deployments_of_stream(stream)
            if (match = @stream_syskit_to_pocolog[stream])
                stream = match
            end

            if (match = @dispatch_info[stream])
                match.deployments
            else
                []
            end
        end

        # Register a deployment task
        #
        # @param [Deployment] deployment_task the task to register
        # @return [void]
        def register(deployment_task)
            new_streams = []
            deployment_task.model.each_stream_mapping.each do |s, _|
                if (pocolog = @stream_syskit_to_pocolog[s])
                    @dispatch_info[pocolog].deployments << deployment_task
                else
                    pocolog = s.syskit_eager_load
                    @stream_syskit_to_pocolog[s] = pocolog
                    @dispatch_info[pocolog] = DispatchInfo.new([deployment_task], s)
                    new_streams << pocolog
                end
            end

            new_streams.each do |pocolog|
                @log_streams[pocolog] ||= add_log_stream(pocolog)
            end

            if stream_aligner.add_streams(*new_streams)
                _, @time = stream_aligner.step_back
            else
                reset_replay_base_times
                @time = stream_aligner.eof? ? end_time : start_time
            end
            reset_replay_base_times
        end

        # Deregisters a deployment task
        def deregister(deployment_task)
            removed_streams = []
            deployment_task.model.each_stream_mapping do |s, _|
                pocolog = @stream_syskit_to_pocolog[s]
                @dispatch_info[pocolog].deployments.delete(deployment_task)
                unless @dispatch_info[pocolog].in_use?
                    @stream_syskit_to_pocolog.delete(s)
                    @dispatch_info.delete(pocolog)
                    removed_streams << pocolog
                end
            end

            # Remove the streams, and make sure that if the aligner read one
            # sample, that sample will still be available at the next step
            if stream_aligner.remove_streams(*removed_streams)
                _, @time = stream_aligner.step_back
            else
                @time = stream_aligner.eof? ? end_time : start_time
            end
            reset_replay_base_times
        end

        # Seek to the given time or sample index
        def seek(time_or_index)
            stream_index, time = stream_aligner.seek(time_or_index, false)
            return unless stream_index

            dispatch(stream_index, time)
            reset_replay_base_times
        end

        # Process the next sample, and feed it to the relevant deployment(s)
        def step
            stream_index, time = stream_aligner.step
            dispatch(stream_index, time) if stream_index
        end

        # Whether we're doing realtime replay
        def running?
            @handler_id
        end

        class StateMismatch < RuntimeError; end

        # Start replaying in realtime
        def start(replay_speed: 1)
            raise StateMismatch, "already running" if running?

            reset_replay_base_times
            @handler_id = execution_engine.add_side_work_handler(
                description: "syskit-pocolog replay handler for #{self}"
            ) { process_in_realtime(replay_speed) }
        end

        def stop
            raise StateMismatch, "not running" unless running?

            execution_engine.remove_side_work_handler(@handler_id)
            @handler_id = nil
        end

        # The minimum amount of time we should call sleep() for. Under that,
        # {#process_in_realtime} will not call sleep and just play some samples
        # slightly in advance
        MIN_TIME_DIFF_TO_SLEEP = 0.01

        # @api private
        #
        # Returns the end of the current engine cycle
        #
        # @return [Time]
        def end_of_current_engine_cycle
            execution_engine.cycle_start + execution_engine.cycle_length
        end

        # @api private
        #
        # Play samples required by the current execution engine's time
        def process_in_realtime(
            replay_speed,
            limit_real_time: end_of_current_engine_cycle
        )
            return unless base_logical_time

            limit_logical_time = base_logical_time +
                                 (limit_real_time - base_real_time) * replay_speed

            loop do
                stream_index, time = stream_aligner.step
                return false unless stream_index

                if time > limit_logical_time
                    stream_aligner.step_back
                    return true
                end

                target_real_time = base_real_time +
                                   (time - base_logical_time) / replay_speed
                time_diff = target_real_time - Time.now
                sleep(time_diff) if time_diff > MIN_TIME_DIFF_TO_SLEEP
                dispatch(stream_index, time)
            end
        end

        # @api private
        #
        # Immediately dispatch the sample last read by {#stream_aligner}
        def dispatch(stream_index, time)
            @time = time
            pocolog_stream = stream_aligner.streams[stream_index]
            info = @dispatch_info.fetch(pocolog_stream)

            stream, position = stream_aligner.sample_info(stream_index)
            sample = log_read_sample(stream, position)
            info.deployments.each do |task|
                task.process_sample(info.syskit_stream, time, sample)
            end
        end

        # Read a sample as indicated by the stream aligner
        #
        # This is reimplemented from pocolog to copy the raw data to the output
        # logs without having to re-marshal it
        def log_read_sample(stream, position)
            input = @log_streams[stream]
            block_pos = stream.stream_index.file_position_by_sample_number(position)
            bs = input.block_stream
            bs.seek(block_pos)
            _, block_raw, payload_raw = bs.read_block_raw
            log_update_stream_index(block_raw, input.index_as_buffer)
            log_update_realtime(payload_raw, Time.now)
            @log_file.io.write(block_raw)
            @log_file.io.write(payload_raw)

            input.sample.from_buffer_direct(
                payload_raw[Pocolog::Format::Current::DATA_BLOCK_HEADER_SIZE,
                            payload_raw.size]
            )
            input.sample
        end

        def log_update_stream_index(block_raw, index_as_buffer)
            block_raw[2, 2] = index_as_buffer
        end

        def log_update_realtime(payload_raw, time)
            payload_raw[0, 8] = [time.tv_sec, time.tv_usec].pack("VV")
        end

        # @api private
        #
        # Resets the reference times used to manage the realtime replay
        def reset_replay_base_times
            @base_real_time = Time.now
            @base_logical_time = time || start_time
        end
    end
end
