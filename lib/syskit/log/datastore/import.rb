# frozen_string_literal: true

require "pocolog/cli/tty_reporter"

require "roby/droby/plan_rebuilder"
require "syskit/log/datastore/normalize"

module Syskit::Log
    class Datastore
        # Importation of a raw dataset into a datastore
        class Import
            class DatasetAlreadyExists < RuntimeError; end

            BASENAME_IMPORT_TAG = ".syskit-pocolog-import"

            def initialize(
                output_path,
                cache_path: output_path, compress: false, reporter: NullReporter.new
            )
                @reporter = reporter

                @output_path = output_path
                @cache_path = cache_path
                @compress = compress
            end

            def compress?
                @compress
            end

            # Compute the information about what will need to be done during the
            # import
            def prepare_import(dir_path)
                pocolog_files = Syskit::Log.logfiles_in_dir(dir_path)
                text_files =
                    Pathname.glob(dir_path + "*.txt").to_a +
                    Pathname.glob(dir_path + "*.txt.zst").to_a
                roby_files =
                    Pathname.glob(dir_path + "*-events.log").to_a +
                    Pathname.glob(dir_path + "*-events.log.zst").to_a
                if roby_files.size > 1
                    raise ArgumentError,
                          "more than one Roby event log found (#{roby_files})"
                end

                ignored = pocolog_files.map do |p|
                    p = p.sub_ext("") if p.extname == ".zst"
                    Pathname.new(Pocolog::Logfiles.default_index_filename(p.to_s))
                end
                ignored += roby_files.map do |p|
                    p = p.sub_ext("") if p.extname == ".zst"
                    p.sub_ext(".idx")
                end

                all_files = Pathname.enum_for(:glob, dir_path + "*").to_a
                remaining = (all_files - pocolog_files -
                             text_files - roby_files - ignored)
                [pocolog_files, text_files, roby_files, remaining]
            end

            # Default steps for the "include" argument to {#import} and
            # {#normalize_dataset}
            IMPORT_DEFAULT_STEPS = %I[pocolog roby text ignored].freeze

            # Find if a directory has already been imported
            #
            # @param [Pathname] path
            # @return [(String,Time)] the digest and time of the last import
            def self.find_import_info(path)
                info_path = (path + BASENAME_IMPORT_TAG)
                return unless info_path.exist?

                info = YAML.safe_load(info_path.read, [Time])
                [info["digest"], info["time"]]
            end

            # Save import info, used by {.find_import_info}
            #
            # @param [Pathname] path
            # @param [ImportInfo] info
            def self.save_import_info(path, dataset, time: Time.now)
                (path + BASENAME_IMPORT_TAG).open("w") do |io|
                    h = { "digest" => dataset.digest, "time" => time }
                    YAML.dump(h, io)
                end
            end

            # Move the given dataset to the store
            #
            # @param [Pathname] dir_path the imported directory
            # @param [Dataset] dataset the normalized dataset, ready to be moved in
            #   the store
            # @return [Dataset] the dataset at its final place
            def self.move_dataset_to_store(dataset, datastore)
                if dataset.dataset_path == dataset.cache_path
                    raise ArgumentError,
                          "cannot move a dataset that has identical cache and data paths"
                end

                dataset_digest = dataset.digest
                final_core_dir = datastore.core_path_of(dataset_digest)
                FileUtils.mv dataset.dataset_path, final_core_dir

                final_cache_dir = datastore.cache_path_of(dataset_digest)
                if dataset.cache_path.exist?
                    FileUtils.mv dataset.cache_path, final_cache_dir
                end

                Dataset.new(final_core_dir,
                            digest: dataset_digest,
                            cache: final_cache_dir)
            end

            # @api private
            #
            # Verifies that the given data should be imported
            def self.validate_dataset_import(
                datastore, dataset, force: false, reporter: NullReporter.new
            )
                return unless datastore.has?(dataset.digest)

                if force
                    datastore.delete(dataset.digest)
                    reporter.warn "Replacing existing dataset #{dataset.digest} "\
                                  "with new one"
                    return
                end

                raise DatasetAlreadyExists,
                      "a dataset identical to #{dataset.dataset_path} already "\
                      "exists in the store (computed digest is #{dataset.digest})"
            end

            # Import Roby's info.yml information into the dataset metadata
            def import_roby_metadata(dataset, roby_info_yml_path)
                begin roby_info = YAML.safe_load(roby_info_yml_path.read)
                rescue Psych::SyntaxError
                    warn "failed to load Roby metadata from #{roby_info_yml_path}"
                    return
                end

                roby_info_has_metadata =
                    roby_info&.respond_to?(:to_ary) &&
                    roby_info.first.respond_to?(:to_hash)
                return unless roby_info_has_metadata

                roby_info.first.to_hash.each do |k, v|
                    dataset.metadata_add("roby:#{k}", v)
                end
            end

            # Normalize the contents of the source folder into a dataset folder
            # structure
            #
            # It does not import the result into the store
            #
            # @param [Pathname] dir_path the input directory
            # @return [Dataset] the resulting dataset
            def normalize_dataset(
                dir_paths, include: IMPORT_DEFAULT_STEPS, delete_input: false
            )
                pocolog_files, text_files, roby_event_logs, ignored_entries =
                    dir_paths.map { |dir| prepare_import(dir) }
                             .transpose.map(&:flatten)

                identity = []
                if include.include?(:pocolog)
                    @reporter.info "Normalizing pocolog log files"
                    identity += normalize_pocolog_files(
                        pocolog_files, delete_input: delete_input
                    )
                end

                if include.include?(:roby)
                    @reporter.info "Copying the Roby event logs"
                    identity += normalize_roby_logs(roby_event_logs)
                elsif include.include?(:roby_no_index)
                    identity += roby_event_logs.map do |log|
                        copy_roby_event_log_no_index(log)
                    end
                end

                if include.include?(:text)
                    @reporter.info "Copying #{text_files.size} text files"
                    copy_text_files(text_files)
                end

                if include.include?(:ignored)
                    @reporter.info "Copying #{ignored_entries.size} remaining "\
                                "files and folders"
                    copy_ignored_entries(ignored_entries)
                end

                import_generate_identity(dir_paths, identity)
            end

            # @api private
            #
            # Copy roby logs to the output path while generating a SQL index
            #
            # @return [Array<Dataset::IdentityEntry>]
            def normalize_roby_logs(roby_event_logs)
                sql_path = @output_path + "roby.sql"
                roby_sql_index = RobySQLIndex::Index.create(sql_path)
                roby_event_logs.map do |roby_event_log|
                    copy_roby_event_log(roby_event_log, roby_sql_index)
                rescue TypeError, RuntimeError => e
                    @reporter.error "Failed to create index from Roby log file"
                    @reporter.error "The log file will still be part of the dataset. "\
                                    "You may attempt to re-create the cached version "\
                                    "later once what is likely to be a bug is fixed"
                    e.full_message.split("\n").each do |line|
                        @reporter.error line
                    end
                    roby_sql_index.close
                    sql_path.unlink

                    copy_roby_event_log_no_index(roby_event_log)
                end
            end

            # @api private
            #
            # Normalize pocolog files into the dataset
            #
            # It computes the log file's SHA256 digests
            #
            # @param [Array<Pathname>] paths the input pocolog log files
            # @return [[Dataset::IdentityEntry]] information needed to compute
            #   the dataset identity
            def normalize_pocolog_files(files, delete_input: false)
                return [] if files.empty?

                out_pocolog_dir = (@output_path + "pocolog")
                out_pocolog_dir.mkpath
                bytes_total = files.inject(0) { |s, p| s + p.size }
                @reporter.reset_progressbar(
                    "|:bar| :current_byte/:total_byte :eta (:byte_rate/s)",
                    total: bytes_total
                )

                entries = Syskit::Log::Datastore.normalize(
                    files,
                    output_path: out_pocolog_dir,
                    delete_input: delete_input, compress: @compress, reporter: @reporter
                )
                entries.each { |e| e.path = identity_path(e.path) }
            ensure
                @reporter.finish
            end

            # @api private
            #
            # Copy text files found in the input directory into the dataset
            #
            # @param [Array<Pathname>] paths the input text file paths
            # @return [void]
            def copy_text_files(files)
                return if files.empty?

                out_text_dir = (@output_path + "text")
                out_text_dir.mkpath
                FileUtils.cp files, out_text_dir
            end

            # @api private
            #
            # Generate identity and metadata files at the end of an import
            def import_generate_identity(input_paths, identity)
                dataset = Dataset.new(@output_path, cache: @cache_path)
                dataset.write_dataset_identity_to_metadata_file(identity)

                input_paths.reverse.each do |dir_path|
                    roby_info_yml_path = (dir_path + "info.yml")
                    if roby_info_yml_path.exist?
                        import_roby_metadata(dataset, roby_info_yml_path)
                    end
                end

                dataset.timestamp # computes the timestamp
                dataset.metadata_write_to_file
                dataset
            end

            # @api private
            #
            # Copy the Roby logs into the target directory
            #
            # It computes the log file's SHA256 digests
            #
            # @param [Array<Pathname>] paths the input roby log files
            # @param [Log::RobySQLIndex::Index] roby_sql_index the database in
            #   which essential Roby information is stored
            # @return [Dataset::IdentityEntry] a hash of the log file's
            #   pathname to the file's SHA256 digest
            def copy_roby_event_log(event_log_path, roby_sql_index) # rubocop:disable Metrics/CyclomaticComplexity
                in_reader, out_path, out_io, in_stat =
                    prepare_roby_event_log_copy(event_log_path)
                unless compress?
                    index_path, index_io =
                        create_roby_event_log_index(out_path, event_log_path)
                end

                @reporter.reset_progressbar(
                    "#{event_log_path.basename} [:bar]", total: in_stat.size
                )

                rebuilder = Roby::DRoby::PlanRebuilder.new
                metadata_update =
                    roby_sql_index.start_roby_log_import(File.basename(out_io.path))
                until in_reader.eof?
                    valid = copy_roby_event_log_one_cycle(
                        in_reader, out_io, index_io,
                        rebuilder, roby_sql_index, in_stat,
                        metadata_update
                    )
                    break unless valid
                end

                entry = Dataset::IdentityEntry.new(
                    identity_path(out_path), out_io.tell, out_io.string_digest
                )
                out_io.close
                FileUtils.touch out_path.to_s, mtime: in_stat.mtime
                entry
            rescue StandardError
                out_path&.unlink
                index_path&.unlink
                raise
            ensure
                in_reader&.close
                index_io&.close
                out_io.close if out_io && !out_io.closed?
            end

            # @api private
            #
            # Read, decode and copy one cycle worth of Roby log data
            def copy_roby_event_log_one_cycle( # rubocop:disable Metrics/ParameterLists
                in_reader, out_io, index_io,
                rebuilder, roby_sql_index, in_stat,
                metadata_update
            )
                pos = in_reader.tell
                @reporter.current = pos
                chunk = in_reader.read_one_chunk
                cycle = in_reader.decode_one_chunk(chunk)

                Roby::DRoby::Logfile.write_entry(out_io, chunk)

                if index_io
                    Roby::DRoby::Logfile::Index.write_one_cycle(index_io, pos, cycle)
                end
                roby_sql_index.add_one_cycle(metadata_update, rebuilder, cycle)
                true
            rescue Roby::DRoby::Logfile::TruncatedFileError => e
                @reporter.warn e.message
                @reporter.warn "truncating Roby log file"
                if index_io
                    index_io.rewind
                    Roby::DRoby::Logfile::Index.write_header(
                        index_io, pos, in_stat.mtime
                    )
                end
                false
            end

            # @api private
            #
            # Copy the Roby logs into the target directory, but do not attempt
            # to decode it and create an index. This is used as fallback if
            # index creation fails.
            #
            # It computes the log file's SHA256 digests
            #
            # @param [Array<Pathname>] paths the input roby log files
            # @return [Dataset::IdentityEntry] the file's identity as used by the
            #   dataset to identity itself
            def copy_roby_event_log_no_index(event_log_path)
                in_reader, out_path, out_io, in_stat =
                    prepare_roby_event_log_copy(event_log_path)
                @reporter.reset_progressbar(
                    "#{event_log_path.basename} [:bar]", total: event_log_path.stat.size
                )

                until in_reader.eof?
                    begin
                        pos = in_reader.tell
                        @reporter.current = pos
                        chunk = in_reader.read_one_chunk

                        Roby::DRoby::Logfile.write_entry(out_io, chunk)
                    rescue Roby::DRoby::Logfile::TruncatedFileError => e
                        @reporter.warn e.message
                        @reporter.warn "truncating Roby log file"
                        break
                    end
                end

                entry = Dataset::IdentityEntry.new(
                    identity_path(out_path), out_io.tell, out_io.string_digest
                )
                out_io.close
                FileUtils.touch out_path.to_s, mtime: in_stat.mtime
                entry
            ensure
                in_reader&.close
                out_io&.close unless out_io&.closed?
            end

            def identity_path(file_path)
                file_path.dirname + file_path.basename(".zst")
            end

            # @api private
            #
            # Initialize the IOs and objects needed for a roby event log copy
            #
            # Helper to both {#copy_roby_event_log} and {#copy_roby_event_log_no_index}
            def prepare_roby_event_log_copy(event_log_path)
                ext = ".zst" if compress?

                i = 0
                while (target_path = @output_path + "roby-events.#{i}.log#{ext}").file?
                    i += 1
                end

                in_stat = event_log_path.stat
                in_io = Syskit::Log.open_in_stream(event_log_path)
                reader = Roby::DRoby::Logfile::Reader.new(in_io)

                end_of_header = in_io.tell
                in_io.rewind
                prologue = in_io.read(end_of_header)
                in_io.seek(end_of_header)

                out_io = Syskit::Log.open_out_stream(target_path)
                digest = Digest::SHA256.new
                out_io = DigestIO.new(out_io, digest)
                out_io.write(prologue)

                [reader, target_path, out_io, in_stat]
            end

            # @api private
            #
            # Create the index file to be filled by {#copy_roby_event_log}
            def create_roby_event_log_index(out_path, in_stat)
                basename =
                    if out_path.extname == ".zst"
                        out_path.basename.sub_ext("").sub_ext(".idx")
                    else
                        out_path.basename.sub_ext(".idx")
                    end

                index_path = @cache_path + basename
                index_io = index_path.open("w")
                Roby::DRoby::Logfile::Index.write_header(
                    index_io, in_stat.size, in_stat.mtime
                )
                [index_path, index_io]
            end

            # @api private
            #
            # Copy the entries in the input directory that are not recognized as a
            # dataset element
            #
            # @param [Array<Pathname>] paths the input elements, which can be
            #   pointing to both files and directories. Directories are copied
            #   recursively
            # @return [void]
            def copy_ignored_entries(paths)
                return if paths.empty?

                out_ignored_dir = (@output_path + "ignored")
                out_ignored_dir.mkpath
                FileUtils.cp_r paths, out_ignored_dir
            end
        end
    end
end
