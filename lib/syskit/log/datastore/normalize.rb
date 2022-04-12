# frozen_string_literal: true

require "digest/sha2"

module Syskit::Log
    class Datastore
        def self.normalize(paths, output_path: paths.first.dirname + "normalized", reporter: Pocolog::CLI::NullReporter.new, compute_sha256: false, index_dir: output_path)
            Normalize.new.normalize(paths, output_path: output_path, reporter: reporter, compute_sha256: compute_sha256, index_dir: index_dir)
        end

        # Encapsulation of the operations necessary to normalize a dataset
        class Normalize
            include Logger::Hierarchy
            extend Logger::Hierarchy
            class InvalidFollowupStream < RuntimeError; end

            attr_reader :out_files

            ZERO_BYTE = [0].pack("v").freeze

            class Output
                attr_reader :path, :stream_info, :digest, :stream_block_pos,
                            :index_map, :last_data_block_time, :tell

                WRITE_BLOCK_SIZE = 1024**2

                def initialize(path, wio, stream_info, digest, stream_block_pos)
                    @path = path
                    @wio = wio
                    @stream_info = stream_info
                    @stream_block_pos = stream_block_pos
                    @digest = digest
                    @index_map = []
                    @tell = wio.tell
                    @buffer = "".dup
                end

                def write(data)
                    if data.size + @buffer.size > WRITE_BLOCK_SIZE
                        @wio.write @buffer + data
                        @tell += @buffer.size + data.size
                        @buffer.clear
                    else
                        @buffer.concat(data)
                    end
                end

                def flush
                    @wio.write @buffer unless @buffer.empty?
                    @wio.flush
                    @tell += @buffer.size
                    @buffer.clear
                end

                def close
                    flush
                    @wio.close
                end

                def create_block_stream
                    Pocolog::BlockStream.new(@wio.dup)
                end

                def add_data_block(rt_time, lg_time, raw_data, raw_payload)
                    @index_map << (@tell + @buffer.size) << lg_time
                    write raw_data[0, 2]
                    write ZERO_BYTE
                    write raw_data[4..-1]
                    write raw_payload
                    @last_data_block_time = [rt_time, lg_time]
                end
            end

            DigestIO = Struct.new :wio, :digest do
                def write(string)
                    wio.write string
                    digest.update string
                end

                def close
                    wio.close
                end

                def flush
                    wio.flush
                end

                def tell
                    wio.tell
                end

                def closed?
                    wio.closed?
                end

                def seek(pos)
                    wio.seek(pos)
                end

                def read(count)
                    wio.read(count)
                end

                def path
                    wio.path
                end

                def size
                    wio.size
                end

                def stat
                    wio.stat
                end
            end

            def initialize
                @out_files = {}
            end

            def normalize(
                paths,
                output_path: paths.first.dirname + "normalized",
                index_dir: output_path, reporter: Pocolog::CLI::NullReporter.new,
                compute_sha256: false
            )
                output_path.mkpath
                paths.each do |logfile_path|
                    e, out_io = normalize_logfile(
                        logfile_path, output_path,
                        reporter: reporter, compute_sha256: compute_sha256
                    )

                    if e
                        normalize_cleanup_after_error(
                            reporter, logfile_path, out_io, index_dir
                        )
                        raise e
                    end
                end

                # Now write the indexes
                index_dir.mkpath
                out_files.each_value do |output|
                    block_stream = output.create_block_stream
                    raw_stream_info = Pocolog::IndexBuilderStreamInfo.new(output.stream_block_pos, output.index_map)
                    stream_info = Pocolog.create_index_from_raw_info(block_stream, [raw_stream_info])
                    index_path = Pocolog::Logfiles.default_index_filename(output.path, index_dir: index_dir)
                    File.open(index_path, "w") do |io|
                        Pocolog::Format::Current.write_index(io, block_stream.io, stream_info)
                    end
                end

                if compute_sha256
                    result = {}
                    out_files.each_value.map { |output| result[output.path] = output.digest }
                    result
                else
                    out_files.each_value.map(&:path)
                end
            ensure
                out_files.each_value(&:close)
            end

            def normalize_cleanup_after_error(reporter, logfile_path, out_io, index_dir)
                reporter.warn(
                    "normalize: exception caught while processing "\
                    "#{logfile_path}, deleting #{out_io.size} output files: "\
                    "#{out_io.map(&:path).sort.join(', ')}"
                )
                out_io.each do |output|
                    out_files.delete(output.path)
                    output.close

                    Pathname.new(output.path).unlink
                    index_path = Pathname.new(
                        Pocolog::Logfiles.default_index_filename(
                            output.path, index_dir: index_dir
                        )
                    )
                    index_path.unlink if index_path.exist?
                end
            end

            NormalizationState =
                Struct
                .new(:out_io_streams, :control_blocks, :followup_stream_time) do
                    def validate_time_followup(stream_index, data_block_header)
                        # Second part of the followup stream validation (see above)
                        last_stream_time = followup_stream_time[stream_index]
                        return unless last_stream_time

                        followup_stream_time[stream_index] = nil
                        previous_rt, previous_lg = last_stream_time
                        if previous_rt > data_block_header.rt_time
                            raise InvalidFollowupStream,
                                  "found followup stream whose real time is before "\
                                  "the stream that came before it"
                        elsif previous_lg > data_block_header.lg_time
                            raise InvalidFollowupStream,
                                  "found followup stream whose logical time is "\
                                  "before the stream that came before it"
                        end
                    end
                end

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
            def normalize_logfile(
                logfile_path, output_path,
                reporter: Pocolog::CLI::NullReporter.new, compute_sha256: false
            )
                state = Struct
                        .new(:out_io_streams, :control_blocks, :followup_stream_time)
                        .new([], +"", [])

                reporter_offset = reporter.current
                last_progress_report = Time.now

                in_io = logfile_path.open
                in_block_stream = Pocolog::BlockStream.new(in_io)
                begin
                    in_block_stream.read_prologue
                rescue Pocolog::InvalidFile
                    reporter.warn "#{logfile_path.basename} does not seem to be "\
                                  "a valid pocolog file, skipping"
                    reporter.current = in_io.size + reporter_offset
                    return nil, []
                end

                state = NormalizationState.new([], +"", [])

                while (block_header = in_block_stream.read_next_block_header)
                    begin
                        normalize_logfile_process_block(
                            output_path, state, block_header, in_block_stream.read_payload,
                            compute_sha256: compute_sha256
                        )
                    rescue InvalidFollowupStream => e
                        raise e, "while processing #{in_block_stream.io.path}: #{e.message}"
                    end

                    now = Time.now
                    if (now - last_progress_report) > 0.1
                        reporter.current = in_block_stream.tell + reporter_offset
                        last_progress_report = now
                    end
                end
                [nil, state.out_io_streams]
            rescue Pocolog::InvalidBlockFound => e
                reporter.warn "#{logfile_path.basename} looks truncated or contains "\
                              "garbage (#{e.message}), stopping processing but keeping "\
                              "the samples processed so far"
                reporter.current = in_io.size + reporter_offset
                [nil, state.out_io_streams]
            rescue StandardError => e
                [e, (state.out_io_streams || [])]
            ensure
                state.out_io_streams.each(&:flush)
                in_block_stream&.close
            end

            # @api private
            #
            # Process a single in block and dispatch it into separate
            # normalized logfiles
            def normalize_logfile_process_block(
                output_path, state, block_header, raw_payload, compute_sha256: false
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
                        raw_payload, compute_sha256: compute_sha256
                    )
                else
                    normalize_logfile_process_data_block(
                        state, stream_index, block_header.raw_data, raw_payload
                    )
                end
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
                state, output_path, stream_index, raw_data, raw_payload,
                compute_sha256: false
            )
                stream_block = Pocolog::BlockStream::StreamBlock.parse(raw_payload)
                stream_block = normalize_stream_definition(stream_block)
                output = create_or_reuse_out_io(
                    output_path, raw_data, stream_block, state.control_blocks,
                    compute_sha256: compute_sha256
                )
                state.out_io_streams[stream_index] = output

                # If we're reusing a stream, save the time of the last
                # written block so that we can validate that the two streams
                # actually follow each other
                state.followup_stream_time[stream_index] = output.last_data_block_time
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
                Pocolog::BlockStream::StreamBlock.new(
                    name, stream_block.typename,
                    stream_block.registry_xml, YAML.dump(metadata)
                )
            end

            # @api private
            #
            # Process a single data block in {#normalize_logfile_process_block}
            def normalize_logfile_process_data_block(
                state, stream_index, raw_data, raw_payload
            )
                data_block_header =
                    Pocolog::BlockStream::DataBlockHeader.parse(raw_payload)
                state.validate_time_followup(stream_index, data_block_header)

                output = state.out_io_streams[stream_index]
                output.add_data_block(
                    data_block_header.rt_time, data_block_header.lg_time,
                    raw_data, raw_payload
                )
            end

            def create_or_reuse_out_io(
                output_path, raw_header, stream_info, initial_blocks,
                compute_sha256: false
            )
                basename = Streams.normalized_filename(stream_info.metadata)
                out_file_path = output_path + "#{basename}.0.log"

                # Check if that's already known to us (multi-part
                # logfile)
                if (existing = out_files[out_file_path])
                    # This is a file we've already seen, reuse its info
                    # and do some consistency checks
                    if existing.stream_info.type != stream_info.type
                        raise InvalidFollowupStream,
                              "multi-IO stream #{stream_info.name} is not consistent: "\
                              "type mismatch"
                    end
                    # Note: normalize_logfile is checking that the files follow
                    # each other
                    return existing
                end

                raw_payload = stream_info.encode
                raw_header[4, 4] = [raw_payload.size].pack("V")
                initialize_out_file(
                    out_file_path, stream_info, raw_header, raw_payload, initial_blocks,
                    compute_sha256: compute_sha256
                )
            end

            # @api private
            #
            # Initialize an output file suitable for {#normalize_logfile}
            #
            # @return [Output]
            def initialize_out_file(
                out_file_path, stream_info, raw_header, raw_payload, initial_blocks,
                compute_sha256: false
            )
                wio = out_file_path.open("w+")

                Pocolog::Format::Current.write_prologue(wio)
                if compute_sha256
                    digest = Digest::SHA256.new
                    wio = DigestIO.new(wio, digest)
                end

                output = Output.new(out_file_path, wio, stream_info, digest, wio.tell + initial_blocks.size)
                output.write initial_blocks
                output.write raw_header[0, 2]
                output.write ZERO_BYTE
                output.write raw_header[4..-1]
                output.write raw_payload
                out_files[out_file_path] = output
            rescue Exception # rubocop:disable Lint/RescueException
                wio&.close
                out_file_path&.unlink
                raise
            end
        end
    end
end
