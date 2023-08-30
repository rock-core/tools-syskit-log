# frozen_string_literal: true

require "pocolog"
require "syskit"

module Syskit
    # Toplevel module for all the log management functionality
    module Log
        extend Logger::Root("Syskit::Log", Logger::WARN)
    end
end

require "digest/sha2"
require "metaruby/dsls/find_through_method_missing"
require "pocolog/cli/null_reporter"
require "zstd-ruby"
require "syskit/log/version"
require "syskit/log/exceptions"
require "syskit/log/digest_io"
require "syskit/log/zstd_io"
require "syskit/log/lazy_data_stream"
require "syskit/log/streams"
require "syskit/log/task_streams"
require "syskit/log/rock_stream_matcher"

require "syskit/log/models/deployment"
require "syskit/log/deployment"
require "syskit/log/models/replay_task_context"
require "syskit/log/replay_task_context"
require "syskit/log/replay_manager"

require "syskit/log/extensions"
require "syskit/log/shell_interface"
require "syskit/log/registration_namespace"
require "syskit/log/plugin"

require "syskit/log/datastore"

require "rom-sql"
require "syskit/log/roby_sql_index/entities"
require "syskit/log/roby_sql_index/definitions"
require "syskit/log/roby_sql_index/index"
require "syskit/log/roby_sql_index/accessors"

module Syskit
    module Log # rubocop:disable Style/Documentation
        NullReporter = Pocolog::CLI::NullReporter

        # Returns the paths of the pocolog log files in a given directory
        #
        # The returned paths are sorted in 'pocolog' order, i.e. multi-IO files are
        # following each other in the order of their place in the overall IO
        # sequence
        #
        # @param [Pathname] dir_path path to the directory
        def self.logfiles_in_dir(dir_path)
            real_path = Pathname.new(dir_path).realpath

            paths = logfiles_glob(real_path).map do |path|
                basename = path.basename
                m = /(.*)\.(\d+)\.log(?:\.zst)?$/.match(basename.to_s)
                [m[1], Integer(m[2]), path] if m
            end
            paths.compact.sort.map { |_, _, path| path }
        end

        def self.logfiles_glob(path)
            Pathname.enum_for(:glob, path + "*.*.log") +
                Pathname.enum_for(:glob, path + "*.*.log.zst")
        end

        def self.open_in_stream(path, &block)
            return path.open(&block) unless path.extname == ".zst"
            return ZstdIO.new(path.open) unless block_given?

            path.open do |io|
                yield(ZstdIO.new(io))
            end
        end

        def self.open_out_stream(path, &block)
            return path.open("w", &block) unless path.extname == ".zst"

            unless block_given?
                return ZstdIO.new(path.open("w"), read: false, write: true)
            end

            path.open("w") do |io|
                zstd = ZstdIO.new(io, read: false, write: true)
                yield zstd
            ensure
                zstd.flush
            end
        end

        # Returns the decompressed version of a given file
        #
        # @param [Pathname] in_path the file
        # @param [Pathname] cache_path the path in which the decompressed versions
        #   should be cached
        # @param [Boolean] force if true, decompress the file even if there is
        #   already a decompressed version in cache_path. Otherwise, return the
        #   existing file.
        # @return [Pathname] the path to the decompressed file
        def self.decompressed(
            in_path, cache_path, force: false, reporter: NullReporter.new
        )
            return in_path unless in_path.extname == ".zst"

            out_path = decompressed_path(in_path, cache_path)
            return out_path if !force && out_path.exist?

            out_path.unlink if out_path.exist?
            decompress(in_path, out_path, reporter: reporter)
            out_path
        end

        # Decompress a zst-compressed file in a given output
        def self.decompress(in_path, out_path, reporter: NullReporter.new)
            out_path.dirname.mkpath

            reporter.current = 0
            atomic_write(out_path) do |temp_io|
                in_path.open do |compressed_io|
                    decompress_io(compressed_io, temp_io, reporter: reporter)
                end
            end
        end

        def self.decompress_io(compressed_io, out_io, reporter: NullReporter.new)
            zstd_io = ZstdIO.new(compressed_io)
            while (data = zstd_io.read(1024**2))
                out_io.write data
                reporter.current = compressed_io.tell * 100 / compressed_io.size
            end
        end

        # Find an existing file at the given path, or at the compressed version of it
        #
        # @param [Pathname] path
        # @return [Pathname,nil]
        def self.find_path_plain_or_compressed(path)
            return path if path.exist?

            compressed_path = path.dirname + "#{path.basename}.zst"
            compressed_path if compressed_path.exist?
        end

        # Write a file atomically
        #
        # It lets us write into a temporary file and move the file in place on
        # success
        def self.atomic_write(out_path)
            out_path.dirname.mkpath
            Tempfile.open("", out_path.dirname.to_s) do |temp_io|
                result = yield(temp_io)

                temp_io.close
                File.rename(temp_io.path, out_path.to_s)
                result
            end
        end

        # Compress a file in a given output using Zstd
        def self.compress(
            in_path, out_path, compute_digest: false, reporter: NullReporter.new
        )
            reporter.reset_progressbar "[:bar]", total: 1.0
            reporter.current = 0
            atomic_write(out_path) do |out_io|
                in_path.open do |in_io|
                    compress_io(
                        in_io, out_io, reporter: reporter, compute_digest: compute_digest
                    )
                end
            end
        end

        def self.compress_io(
            in_io, out_io, compute_digest: false, reporter: NullReporter.new
        )
            buffer = +""
            compressed_io = ZstdIO.new(out_io, read: false, write: true)
            compressed_io = DigestIO.new(compressed_io) if compute_digest
            while (data = in_io.read(1024**2, buffer))
                compressed_io.write data
                reporter.current = Float(in_io.tell) / in_io.size
            end
            compressed_io.string_digest if compute_digest
        ensure
            compressed_io&.close
        end

        def self.read_single_lazy_data_stream(logfile_path, minimal_index_path, index_dir)
            open_in_stream(logfile_path) do |file_io|
                minimal_index_path.open do |index_io|
                    read_single_lazy_data_stream_from_io(
                        file_io, index_io, logfile_path, index_dir
                    )
                end
            end
        end

        def self.read_single_lazy_data_stream_from_io(
            file_io, index_io, logfile_path, index_dir
        )
            stream_info = Pocolog::Format::Current.read_minimal_info(
                index_io, file_io, validate: false
            )
            stream_block, index_stream_info = stream_info.first

            interval_rt = index_stream_info.interval_rt.map do |t|
                Pocolog::StreamIndex.time_from_internal(t, 0)
            end
            interval_lg = index_stream_info.interval_lg.map do |t|
                Pocolog::StreamIndex.time_from_internal(t, 0)
            end

            LazyDataStream.new(
                logfile_path,
                index_dir,
                stream_block.name,
                stream_block.type,
                stream_block.metadata,
                interval_rt,
                interval_lg,
                index_stream_info.stream_size
            )
        end

        # Generate an index file that contains only stream definition for lazy loading
        #
        # These index files are very small and can therefore be saved along with the
        # core data, making getting a new dataset a lot more agile
        def self.generate_pocolog_minimal_index(
            logfile_io, index_path, compute_digest: false
        )
            block_stream = Pocolog::BlockStream.new(logfile_io)
            block_stream.read_prologue

            if compute_digest
                digest_io = DigestIO.new(logfile_io)
                block_stream = Pocolog::BlockStream.new(digest_io)
            end

            stream_info = Pocolog.file_index_builder(block_stream, skip_payload: false)
            write_pocolog_minimal_index(stream_info, index_path)
            digest_io&.digest
        end

        def self.write_pocolog_minimal_index(stream_info, index_path)
            index_path.open("w") do |index_io|
                Pocolog::Format::Current.write_index_header(index_io, 0, Time.now, 1)
                Pocolog::Format::Current.write_index_stream_info(
                    index_io, stream_info
                )
            end
        end

        def self.decompressed_path(file_path, cache_path)
            return file_path unless file_path.extname == ".zst"

            cache_path + file_path.basename(".zst")
        end

        def self.index_path(logfile_path, cache_path)
            logfile_path = decompressed_path(logfile_path, cache_path)
            index_path = Pocolog::Logfiles.default_index_filename(
                logfile_path.to_s, index_dir: cache_path.to_s
            )
            Pathname(index_path)
        end

        def self.minimal_index_path(logfile_path)
            basename = logfile_path.basename(".zst").to_s
            index = Pocolog::Logfiles.default_index_filename(
                basename, index_dir: logfile_path.dirname.to_s
            )
            Pathname(index)
        end
    end
end
