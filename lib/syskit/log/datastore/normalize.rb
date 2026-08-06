# frozen_string_literal: true

require "digest/sha2"
require "open3"

module Syskit::Log
    class Datastore
        # @return [Array<Dataset::IdentityEntry>]
        def self.normalize(
            paths,
            output_path: paths.first.dirname + "normalized", reporter: NullReporter.new,
            delete_input: false, compress: false,
            executor: Concurrent::ImmediateExecutor.new,
            config: NormalizeConfiguration.new
        )
            Normalize.new(compress: compress, executor: executor, config: config)
                     .normalize(
                         paths,
                         output_path: output_path, reporter: reporter,
                         delete_input: delete_input
                     )
        end

        # Encapsulation of the operations necessary to normalize a dataset
        class Normalize
            include Logger::Hierarchy
            extend Logger::Hierarchy
            class InvalidFollowupStream < RuntimeError; end

            # Configuration of the normalization process
            #
            # @return [NormalizeConfiguration]
            attr_reader :config

            # Mapping of path for created output files to their {Output} object
            #
            # @return [{Pathname=>Output}]
            attr_reader :out_files

            ZERO_BYTE = [0].pack("v").freeze

            class LogicalTimeReader
                def initialize(type, field_path)
                    @field_path = field_path
                    @native_type = resolve_native_type(type)
                    @sample = @native_type.new
                end

                def resolve_native_type(type)
                    native_registry =
                        Pocolog::DataStream
                        .update_container_types_to_native(type.registry)
                    native_registry.build(type.name)
                end

                def call(raw_payload)
                    # Skip 21 bytes as they belong to the data stream declaration block
                    # information before the marshalled data.
                    # See rock-core/tools-pocolog/blob/master/spec/spec-v2.txt
                    raw_sample = @sample.from_buffer(raw_payload[21..-1])
                    t = @field_path.inject(raw_sample) do |s, name|
                        s.raw_get(name)
                    end
                    t.microseconds
                rescue ArgumentError => e
                    raise unless e.message.match?(/parts.of.the.provided.buffer/)

                    raise Pocolog::InvalidBlockFound, e.message, e.backtrace
                end
            end

            class LogicalTimeReaderOpt
                def initialize(offset)
                    # Skip 21 bytes as they belong to the data stream declaration block
                    # information before the marshalled data.
                    # See rock-core/tools-pocolog/blob/master/spec/spec-v2.txt
                    @offset = offset + 21
                    @expected_size = @offset + 8
                end

                def call(raw_payload)
                    if raw_payload.size < @expected_size
                        raise Pocolog::InvalidBlockFound,
                              "buffer too small when extracting logical time, expected " \
                              "at least #{@expected_size} bytes but got " \
                              "#{raw_payload.size}"
                    end

                    raw_payload[@offset, 8].unpack1("q")
                end
            end

            # @api private
            #
            # Internal representation of the output of a normalization operation
            class Output
                attr_accessor :path
                attr_reader :logical_time_reader
                attr_reader :stream_block
                attr_accessor :digest
                attr_reader :stream_size
                attr_reader :stream_block_pos
                attr_reader :last_data_block_time
                attr_reader :interval_rt
                attr_reader :interval_lg
                attr_accessor :string_digest

                attr_reader :stats

                Stats = Struct.new(
                    :rt_time_not_monotonic, :lg_time_not_monotonic,
                    :rt_time_duplicate, :lg_time_duplicate,
                    :invalid_logical_time, :logical_time_overrides,
                    :rejected_samples,
                    keyword_init: true
                ) do
                    def warn?
                        !zero?
                    end

                    def zero?
                        each.all?(&:zero?)
                    end
                end

                WRITE_BLOCK_SIZE = 8 * 1024

                def initialize(
                    path, wio, stream_block, stream_block_pos, config
                )
                    @path = path
                    @wio = wio
                    @stream_block = stream_block
                    @stream_block_pos = stream_block_pos
                    @logical_time_reader = resolve_logical_time_reader(stream_block)
                    @stream_size = 0
                    @interval_rt = []
                    @interval_lg = []
                    @buffer = "".dup
                    @allow_duplicates = {
                        "rt" => config.rt_allow_duplicates?,
                        "lg" => config.lg_allow_duplicates?
                    }

                    @stats = Stats.new(
                        rt_time_not_monotonic: 0, lg_time_not_monotonic: 0,
                        rt_time_duplicate: 0, lg_time_duplicate: 0,
                        invalid_logical_time: 0, logical_time_overrides: 0,
                        rejected_samples: 0
                    )
                end

                def tell
                    @wio.tell
                end

                def size
                    path.stat.size
                end

                def write_pocolog_minimal_index
                    index_path = Syskit::Log.minimal_index_path(path)
                    Syskit::Log.write_pocolog_minimal_index([index_stream_info], index_path)
                end

                def index_stream_info
                    Pocolog::Format::Current::IndexStreamInfo.new(
                        declaration_pos: stream_block_pos,
                        index_pos: 0,
                        base_time: 0,
                        stream_size: stream_size,
                        rt_min: interval_rt[0] || 0,
                        rt_max: interval_rt[1] || 0,
                        lg_min: interval_lg[0] || 0,
                        lg_max: interval_lg[1] || 0
                    )
                end

                def write(data)
                    if data.size + @buffer.size > WRITE_BLOCK_SIZE
                        @wio.write @buffer + data
                        @buffer.clear
                    else
                        @buffer.concat(data)
                    end
                end

                def flush
                    @wio.write @buffer unless @buffer.empty?
                    @wio.flush
                    @buffer.clear
                end

                def close
                    flush
                    @wio.close
                end

                def closed?
                    @wio.closed?
                end

                def create_block_stream
                    Pocolog::BlockStream.new(@wio.dup)
                end

                def update_raw_payload_logical_time(raw_payload, logical_time_us)
                    # Logical time are bytes from 8..15
                    raw_payload[8..11] = [logical_time_us / 1_000_000].pack("V")
                    raw_payload[12..15] = [logical_time_us % 1_000_000].pack("V")
                    raw_payload
                end

                def read_logical_time(raw_payload)
                    return unless (lg_time_us = @logical_time_reader&.call(raw_payload))

                    if lg_time_us == 0
                        stats.invalid_logical_time += 1
                        return
                    end

                    stats.logical_time_overrides += 1
                    lg_time_us
                end

                def add_data_block(rt_time, lg_time, raw_data, raw_payload)
                    @stream_size += 1

                    write raw_data[0, 2]
                    write ZERO_BYTE
                    write raw_data[4..-1]

                    write raw_payload

                    @interval_rt[0] ||= rt_time
                    @interval_rt[1] = rt_time
                    @interval_lg[0] ||= lg_time
                    @interval_lg[1] = lg_time
                    @last_data_block_time = [rt_time, lg_time]
                end

                # Return an object that extracts the logical time of a sample if
                # needed, and nil otherwise.
                #
                # This method sets up the normalization to save the logical time saved
                # in the data samples in the log file's logical time field, but only if
                # it has not been done by Rock's logger already.
                #
                # If the stream metadata contains rock_timestamp_field, the method assumes
                # that this was done by Rock's logger already and returns nil.
                # Otherwise, it looks for a field with the logical_time role
                #
                # The returned object must have a `call` method which is given
                # the sample in marshalled form (as saved on disk) and return
                # the time in microseconds
                #
                # @return [#call,nil]
                def resolve_logical_time_reader(stream_block)
                    path = resolve_logical_time_field_path(stream_block)
                    return unless path

                    stream_type = stream_block.type

                    opt = Output
                          .compound_field_path_directly_addressable?(stream_type, path)

                    if opt
                        _, offset = path.inject([stream_type, 0]) do |(t, o), name|
                            [t[name], o + t.offset_of(name)]
                        end

                        LogicalTimeReaderOpt.new(offset)
                    else
                        LogicalTimeReader.new(stream_type, path[0..-2])
                    end
                end

                def resolve_logical_time_field_path(stream_block)
                    return if stream_block.metadata["rock_timestamp_field"]

                    field_name = stream_block.metadata["rock_timestamp_field_override"]
                    stream_type = stream_block.type
                    unless field_name
                        field_name = logical_time_field(stream_type)
                        return unless field_name
                    end

                    field_path = field_name.split(".") + ["microseconds"]
                    field_type = field_path.inject(stream_type) do |t, name|
                        t[name]
                    end

                    unless valid_logical_time_type?(field_type)
                        raise ArgumentError,
                              "field #{field_name} of #{type}, of type #{field_type}, " \
                              "is marked as logical time, but it does not have an " \
                              "integer type field called 'microseconds'"
                    end
                    field_path
                end

                def display_stats(reporter)
                    if @stats.warn?
                        reporter.warn "#{path}: WARNING"
                        @stats.each_pair do |k, v|
                            reporter.warn "  #{k}: #{v}" unless v == 0
                        end
                    elsif !@stats.zero?
                        reporter.info "#{path}:"
                        @stats.each_pair do |k, v|
                            reporter.info "  #{k}: #{v}" unless v == 0
                        end
                    end
                end

                # Find the name of the first field in a type to have the
                # logical_time role
                #
                # This field will be used during the normalization process to
                #
                # The method does not validate that the field's type is
                # compatible with the logical time extraction logic
                def logical_time_field(type)
                    return unless type < Typelib::CompoundType

                    metadata = type.field_metadata
                    type.each_field do |field|
                        role = metadata[field].get("role").first
                        return field if role == "logical_time"
                    end
                    nil
                end

                # Validate that the given type is valid as a 'logical time' type
                #
                # In practice, it has to follow Rock's base::Time
                # implementation, which means
                # - being a compound
                # - having a 'microseconds' field of type int64_t
                def valid_logical_time_type?(us_type)
                    us_type <= Typelib::NumericType && us_type.size == 8
                end

                def self.compound_field_path_directly_addressable?(
                    compound_type, field_path
                )
                    type = compound_type
                    field_path.each do |name|
                        unless compound_field_directly_addressable?(type, name)
                            return false
                        end

                        type = type[name]
                    end
                    true
                end

                def validate_time_followup(data_block_header)
                    rt = data_block_header.rt_time
                    lg = data_block_header.lg_time
                    return true unless last_data_block_time

                    previous_rt, previous_lg = last_data_block_time
                    rt_valid =
                        validate_time_followup_rtlg("rt", previous_rt, rt)
                    lg_valid =
                        validate_time_followup_rtlg("lg", previous_lg, lg)

                    unless rt_valid && lg_valid
                        stats.rejected_samples += 1
                        return
                    end

                    true
                end

                def validate_time_followup_rtlg(field, previous, actual)
                    return true if previous < actual

                    if previous > actual
                        stats["#{field}_time_not_monotonic"] += 1
                        return false
                    elsif previous == actual
                        stats["#{field}_time_duplicate"] += 1
                        return @allow_duplicates[field]
                    end

                    true
                end

                # Validates that a compound's field offset is fixed in its
                # marshalled form
                #
                # This is a precondition to use the optimized codepath to
                # extract a sample's logical time.
                #
                # @raise [ArgumentError] if the field does not exist in the type
                def self.compound_field_directly_addressable?(compound_type, field_name)
                    compound_type.each_field do |field|
                        return true if field == field_name

                        field_type = compound_type[field]
                        return false if field_type <= Typelib::ContainerType
                    end

                    raise ArgumentError,
                          "no field #{field_name} in #{compound_type}"
                end
            end

            def initialize(
                executor: Concurrent::ImmediateExecutor.new,
                compress: false, config: NormalizeConfiguration.new
            )
                @out_files = {}
                @executor = executor
                @finalization_executor = Concurrent::ImmediateExecutor.new
                @compress = compress
                @config = config
            end

            def compress?
                @compress
            end

            # @return [Array<Dataset::IdentityEntry>]
            def normalize(
                paths,
                output_path: paths.first.dirname + "normalized",
                reporter: NullReporter.new, delete_input: false
            )
                output_path.mkpath
                logfile_groups = paths.group_by do
                    /\.\d+\.log(?:\.zst)?$/.match(_1.basename.to_s).pre_match
                end

                async_failure = Concurrent::Event.new

                groups = logfile_groups.to_a
                postprocess = []
                until groups.empty?
                    key, files = groups.shift
                    id = groups.size

                    reporter.info "Normalizing group #{key}"

                    temp_output_path = output_path / id.to_s
                    temp_output_path.mkdir
                    group_output = normalize_logfile_group(
                        async_failure, files,
                        output_path: temp_output_path, reporter: reporter
                    )

                    if async_failure.set?
                        # Make sure the promise is waited-on below
                        postprocess.concat(group_output)
                        break
                    end

                    group_postprocess = group_output.map do |output_stream|
                        postprocess_output(output_path, output_stream)
                    end
                    group_postprocess =
                        Concurrent::Promises
                        .zip_futures_on(@finalization_executor, *group_postprocess)
                    group_postprocess = group_postprocess.then_on(
                        @finalization_executor, files, temp_output_path
                    ) do |*identities, f, p|
                        f.each { _1.unlink } if delete_input
                        p.rmdir
                        identities
                    end
                    group_postprocess.on_rejection! { async_failure.set }
                    postprocess << group_postprocess
                end

                Concurrent::Promises
                    .zip_futures_on(@finalization_executor, *postprocess)
                    .value!.flatten
            end

            # Postprocess a single {Output} normalized by {#normalize_logfile_group}
            def postprocess_output(output_path, output)
                future = Concurrent::Promises.future_on(@executor, output.path) do |path|
                    subcommand_compute_digest(path)
                end

                if compress?
                    compress_future =
                        Concurrent::Promises.future_on(@executor, output.path) do |path|
                            subcommand_compress_path(path)
                        end
                    future = future.zip(compress_future)
                end

                path = output.path
                future.then_on(@finalization_executor) do |digest|
                    size = path.stat.size
                    final_path_basename =
                        if compress?
                            path.unlink
                            "#{path.basename}.zst"
                        else
                            path.basename
                        end

                    source_path = path.dirname / final_path_basename
                    FileUtils.mv source_path, output_path
                    FileUtils.mv path.sub_ext(".idx"), output_path

                    Dataset::IdentityEntry.new(
                        output_path / final_path_basename, size, digest
                    )
                end
            end

            # Normalize a group of log files
            #
            # A "group" of log files are all log files from the same logger. They are
            # expected to be rotations of the same set of streams, and therefore are
            # being normalized to the same set of log files. In addition, we expect
            # two different groups to not have overlapping streams
            #
            # @return [Array<Output>]
            def normalize_logfile_group(
                async_failure, files, output_path:, reporter: NullReporter.new
            )
                files.each do |logfile_path|
                    return if async_failure.set? # rubocop:disable Lint/NonLocalExitFromIterator

                    normalize_logfile(logfile_path, output_path, reporter: reporter)
                rescue Exception # rubocop:disable Lint/RescueException
                    reporter.warn(
                        "normalize: exception caught while processing #{logfile_path}"
                    )
                    raise
                end

                out_files.each_value do |output|
                    output.write_pocolog_minimal_index
                    output.close
                    output.display_stats(reporter)
                end
                out_files.values
            rescue Exception # rubocop:disable Lint/RescueException
                reporter.warn(
                    "normalize: deleting #{out_files.size} output files and their indexes"
                )
                out_files.each_value { _1.path.unlink if _1.path.exist? }
                raise
            ensure
                out_files.each_value { _1.close unless _1.closed? }
                out_files.clear
            end

            def default_index_pathname(logfile_path, index_dir:)
                logfile_path = logfile_path.sub_ext("") if logfile_path.extname == ".zst"
                path = Pocolog::Logfiles.default_index_filename(
                    logfile_path, index_dir: index_dir
                )
                Pathname.new(path)
            end

            # @api private
            #
            # Compress the given file
            #
            # The compressed file is #{path}.zst. The original path is kept
            #
            # @return [void]
            def subcommand_compress_path(path)
                Open3.popen3(
                    "zstd", "--keep", path.to_s, "-o", "#{path}.zst",
                    "--no-progress"
                ) do |stdin, stdout, stderr, wait_thread|
                    stdin.close
                    err = Thread.new { stderr.read }
                    out = Thread.new { stdout.read }
                    err, out = [err, out].map(&:value)
                    status = wait_thread.value
                    unless status.success?
                        raise SubcommandFailed,
                              "compression of #{path.basename} failed: " \
                              "out=#{out} err=#{err}"
                    end
                end
                nil
            end

            # @api private
            #
            # Compute the sha256 digest of the given file
            #
            # @return [String] the digest
            def subcommand_compute_digest(path)
                Open3.popen2("sha256sum", "-b") do |stdin, stdout, wait_thread|
                    IO.copy_stream(
                        path.to_s, stdin,
                        path.stat.size - Pocolog::Format::Current::PROLOGUE_SIZE,
                        Pocolog::Format::Current::PROLOGUE_SIZE
                    )
                    stdin.close
                    output = stdout.read
                    status = wait_thread.value
                    unless status.success?
                        raise SubcommandFailed,
                              "digest computation for #{path.basename} " \
                              "failed: #{output}"
                    end
                    output.split(" ").first.strip
                end
            end

            def self.format_timestamp(time_us)
                Time.at(time_us / 1_000_000).strftime("%Y-%m-%d %H:%M:%S:%6N")
            end

            NormalizationState =
                Struct.new(:out_io_streams, :control_blocks, keyword_init: true)

            # @api private
            #
            # Normalize a single logfile
            #
            # It detects followup streams from previous calls. This is really
            # designed to be called by {#normalize}, and leaves a lot of cleanup to
            # {#normalize}. Do not call directly
            #
            # @return [(nil,Array<IO>),(Exception,Array<IO>)] returns a potential
            #   exception that has been raised during processing, and the IOs that
            #   have been touched by the call.
            def normalize_logfile(logfile_path, output_path, reporter: NullReporter.new)
                state = NormalizationState.new(out_io_streams: [], control_blocks: +"")

                in_io = Syskit::Log.open_in_stream(logfile_path)
                in_block_stream =
                    normalize_logfile_init(logfile_path, in_io, reporter: reporter)
                return unless in_block_stream

                reporter_offset = reporter.current
                normalize_logfile_process_block_stream(
                    output_path, state, in_block_stream,
                    reporter: reporter,
                    progress_position: -> { progress_position(in_io) }
                )
            rescue Pocolog::InvalidBlockFound => e
                report_truncated_file(reporter, logfile_path, e.message)
                reporter.current = Syskit::Log.io_disk_size(in_io) + reporter_offset
            rescue RuntimeError => e
                if e.message != "decompress error error code: Data corruption detected"
                    raise
                end

                report_truncated_file(reporter, logfile_path, e.message)
                reporter.current = Syskit::Log.io_disk_size(in_io) + reporter_offset
            ensure
                state.out_io_streams.each(&:flush)
                in_block_stream&.close
            end

            def report_truncated_file(reporter, logfile_path, message)
                reporter.warn "#{logfile_path.basename} looks truncated or contains "\
                              "garbage (#{message}), stopping processing but keeping "\
                              "the samples processed so far"
            end

            def normalize_logfile_process_block_stream(
                output_path, state, in_block_stream,
                progress_position:, reporter: NullReporter.new
            )
                reporter_offset = reporter.current

                last_progress_report = Time.now
                while (block_header = in_block_stream.read_next_block_header)
                    begin
                        normalize_logfile_process_block(
                            output_path, state, block_header, in_block_stream.read_payload
                        )
                    rescue InvalidFollowupStream => e
                        raise e, "while processing #{in_block_stream.io.path}: #{e.message}"
                    end

                    now = Time.now
                    if (now - last_progress_report) > 0.1
                        reporter.current = progress_position.call + reporter_offset
                        last_progress_report = now
                    end
                end
            end

            def progress_position(io)
                if io.respond_to?(:compressed_tell)
                    io.compressed_tell
                else
                    io.tell
                end
            end

            # @api private
            #
            # Process a single in block and dispatch it into separate
            # normalized logfiles
            def normalize_logfile_process_block(
                output_path, state, block_header, raw_payload
            )
                stream_index = block_header.stream_index

                # Control blocks must be saved in all generated log files
                # (they apply to all streams). Write them to all streams
                # seen so far, and write them when we (re)open an existing
                # file
                if block_header.kind == Pocolog::CONTROL_BLOCK
                    normalize_logfile_process_control_block(
                        state, block_header.raw_data, raw_payload
                    )
                elsif block_header.kind == Pocolog::STREAM_BLOCK
                    normalize_logfile_process_stream_block(
                        state, output_path, stream_index, block_header.raw_data,
                        raw_payload
                    )
                else
                    normalize_logfile_process_data_block(
                        state, stream_index, block_header.raw_data, raw_payload
                    )
                end
            end

            # @api private
            #
            # Open a log file and make sure it's actually a pocolog logfile
            def normalize_logfile_init(logfile_path, in_io, reporter: NullReporter.new)
                in_block_stream = Pocolog::BlockStream.new(in_io)
                in_block_stream.read_prologue
                in_block_stream
            rescue Pocolog::InvalidFile
                reporter.warn "#{logfile_path.basename} does not seem to be "\
                                "a valid pocolog file, skipping"
                reporter.current += Syskit::Log.io_disk_size(in_io)
                nil
            end

            # @api private
            #
            # Process a single control block in {#normalize_logfile_process_block}
            def normalize_logfile_process_control_block(state, raw_block)
                state.control_blocks << raw_block
                state.out_io_streams.each { |wio| wio.write raw_block }
            end

            # @api private
            #
            # Process a single stream definition block in
            # {#normalize_logfile_process_block}
            def normalize_logfile_process_stream_block(
                state, output_path, stream_index, raw_data, raw_payload
            )
                stream_block = Pocolog::BlockStream::StreamBlock.parse(raw_payload)
                stream_block = normalize_stream_definition(stream_block)
                output = create_or_reuse_out_io(
                    output_path, raw_data, stream_block, state.control_blocks
                )
                state.out_io_streams[stream_index] = output
            end

            # @api private
            #
            # Normalize stream definition, to avoid quirks that exist(ed) in
            # during log generation
            def normalize_stream_definition(stream_block)
                metadata = stream_block.metadata.dup
                metadata = Streams.sanitize_metadata(
                    metadata, stream_name: stream_block.name
                )
                name = Streams.normalized_stream_name(metadata)
                metadata = apply_metadata_from_config(stream_block, metadata)
                Pocolog::BlockStream::StreamBlock.new(
                    name, stream_block.typename,
                    stream_block.registry_xml, YAML.dump(metadata)
                )
            end

            # @api private
            #
            # Apply extra metadata to streams
            #
            # Used to "fixup" metadata on import
            def apply_metadata_from_config(stream_block, metadata)
                @config.stream_config_for_type(stream_block.typename)
                       .metadata_update(metadata)
            end

            # @api private
            #
            # Process a single data block in {#normalize_logfile_process_block}
            def normalize_logfile_process_data_block(
                state, stream_index, raw_data, raw_payload
            )
                data_block_header =
                    Pocolog::BlockStream::DataBlockHeader.parse(raw_payload)
                output = state.out_io_streams[stream_index]
                if (lg_time_override = output.read_logical_time(raw_payload))
                    data_block_header.lg_time = lg_time_override
                end

                valid = output.validate_time_followup(data_block_header)
                return unless valid

                if lg_time_override
                    raw_payload = output.update_raw_payload_logical_time(
                        raw_payload, lg_time_override
                    )
                end

                output.add_data_block(
                    data_block_header.rt_time, data_block_header.lg_time,
                    raw_data, raw_payload
                )
            end

            def create_or_reuse_out_io(
                output_path, raw_header, stream_block, initial_blocks
            )
                basename = Streams.normalized_filename(stream_block.metadata)
                out_file_path = output_path + "#{basename}.0.log"

                # Check if that's already known to us (multi-part
                # logfile)
                if (existing = out_files[out_file_path])
                    # This is a file we've already seen, reuse its info
                    # and do some consistency checks
                    if existing.stream_block.type != stream_block.type
                        raise InvalidFollowupStream,
                              "multi-IO stream #{stream_block.name} is not consistent: "\
                              "type mismatch"
                    end
                    # Note: normalize_logfile is checking that the files follow
                    # each other
                    return existing
                end

                raw_payload = stream_block.encode
                raw_header[4, 4] = [raw_payload.size].pack("V")
                initialize_out_file(
                    out_file_path, stream_block, raw_header, raw_payload, initial_blocks
                )
            end

            # @api private
            #
            # Initialize an output file suitable for {#normalize_logfile}
            #
            # @return [Output]
            def initialize_out_file(
                out_file_path, stream_block, raw_header, raw_payload, initial_blocks
            )
                wio = Syskit::Log.open_out_stream(out_file_path)
                config = @config.stream_config_for_type(stream_block.typename)

                Pocolog::Format::Current.write_prologue(wio)
                output = Output.new(out_file_path, wio, stream_block, wio.tell, config)
                output.write initial_blocks
                output.write raw_header[0, 2]
                output.write ZERO_BYTE
                output.write raw_header[4..-1]
                output.write raw_payload
                out_files[out_file_path] = output
            rescue Exception # rubocop:disable Lint/RescueException
                wio&.close
                out_file_path&.unlink if out_file_path&.exist?
                raise
            end
        end
    end
end
