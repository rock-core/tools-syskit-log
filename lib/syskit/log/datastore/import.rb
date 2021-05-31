# frozen_string_literal: true

require 'syskit/log/datastore/normalize'
require 'pocolog/cli/tty_reporter'

module Syskit::Log
    class Datastore
        def self.import(datastore, dataset_path, silent: false, force: false)
            Import.new(datastore).import(dataset_path, silent: silent, force: force)
        end

        # Import dataset(s) in a datastore
        class Import
            class DatasetAlreadyExists < RuntimeError; end

            BASENAME_IMPORT_TAG = '.syskit-pocolog-import'

            attr_reader :datastore
            def initialize(datastore)
                @datastore = datastore
            end

            # Compute the information about what will need to be done during the
            # import
            def prepare_import(dir_path)
                pocolog_files = Syskit::Log.logfiles_in_dir(dir_path)
                text_files    = Pathname.glob(dir_path + "*.txt")
                roby_files    = Pathname.glob(dir_path + "*-events.log")
                if roby_files.size > 1
                    raise ArgumentError, "more than one Roby event log found"
                end

                ignored = pocolog_files.map do |p|
                    Pathname.new(Pocolog::Logfiles.default_index_filename(p.to_s))
                end
                ignored.concat(roby_files.map { |p| p.sub(/-events.log$/, "-index.log") })

                all_files = Pathname.enum_for(:glob, dir_path + '*').to_a
                remaining = (all_files - pocolog_files -
                             text_files - roby_files - ignored)
                [pocolog_files, text_files, roby_files, remaining]
            end

            # Import a dataset into the store
            #
            # @param [Pathname] dir_path the input directory
            # @return [Pathname] the directory of the imported dataset in the store
            def import(
                in_dataset_paths, force: false, reporter: Pocolog::CLI::NullReporter.new
            )
                datastore.in_incoming do |core_path, cache_path|
                    dataset = normalize_dataset(
                        in_dataset_paths, core_path,
                        cache_path: cache_path, reporter: reporter
                    )
                    validate_dataset_import(
                        dataset, force: force, reporter: reporter
                    )
                    move_dataset_to_store(dataset)
                end
            end

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
            # @param [Boolean] force if force (the default), the method will fail if
            #   the dataset is already in the store. Otherwise, it will erase the
            #   existing dataset with the new one
            # @return [Dataset] the dataset at its final place
            # @raise DatasetAlreadyExists if a dataset already exists with the same
            #   ID than the new one and 'force' is false
            def move_dataset_to_store(dataset)
                dataset_digest = dataset.digest
                final_core_dir = datastore.core_path_of(dataset_digest)
                FileUtils.mv dataset.dataset_path, final_core_dir
                final_cache_dir = datastore.cache_path_of(dataset_digest)
                if final_core_dir != final_cache_dir
                    FileUtils.mv dataset.cache_path, final_cache_dir
                end

                Dataset.new(final_core_dir,
                            digest: dataset_digest,
                            cache: final_cache_dir)
            end

            # @api private
            #
            # Verifies that the given data should be imported
            def validate_dataset_import(
                dataset, force: false, reporter: Pocolog::CLI::NullReporter.new
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
            # @param [Pathname] output_dir_path the output directory
            # @return [Dataset] the resulting dataset
            def normalize_dataset(
                dir_paths,
                output_dir_path,
                cache_path: output_dir_path,
                reporter: CLI::NullReporter.new
            )
                pocolog_files, text_files, roby_event_logs, ignored_entries =
                    dir_paths.map { |dir| prepare_import(dir) }
                             .transpose.map(&:flatten)

                reporter.info "Normalizing pocolog log files"
                normalize_pocolog_files(
                    output_dir_path, pocolog_files,
                    cache_path: cache_path,
                    reporter: reporter
                )

                reporter.info "Copying the Roby event logs"
                roby_event_logs.each do |roby_event_log|
                    copy_roby_event_log(output_dir_path, roby_event_log)
                end

                reporter.info "Copying #{text_files.size} text files"
                copy_text_files(output_dir_path, text_files)

                reporter.info "Copying #{ignored_entries.size} remaining "\
                              "files and folders"
                copy_ignored_entries(output_dir_path, ignored_entries)

                dataset = Dataset.new(output_dir_path, cache: cache_path)
                dataset.write_dataset_identity_to_metadata_file

                dir_paths.reverse.each do |dir_path|
                    roby_info_yml_path = (dir_path + "info.yml")
                    if roby_info_yml_path.exist?
                        import_roby_metadata(dataset, roby_info_yml_path)
                    end
                end

                dataset.metadata_write_to_file
                dataset
            end

            # @api private
            #
            # Normalize pocolog files into the dataset
            #
            # It computes the log file's SHA256 digests
            #
            # @param [Pathname] output_dir the target directory
            # @param [Array<Pathname>] paths the input pocolog log files
            # @return [Hash<Pathname,Digest::SHA256>] a hash of the log file's
            #   pathname to the file's SHA256 digest. The pathnames are
            #   relative to output_dir
            def normalize_pocolog_files(
                output_dir, files,
                reporter: CLI::NullReporter.new, cache_path: output_dir
            )
                return {} if files.empty?

                out_pocolog_dir = (output_dir + "pocolog")
                out_pocolog_dir.mkpath
                out_pocolog_cache_dir = (cache_path + "pocolog")
                bytes_total = files.inject(0) { |s, p| s + p.size }
                reporter.reset_progressbar(
                    "|:bar| :current_byte/:total_byte :eta (:byte_rate/s)",
                    total: bytes_total
                )

                Syskit::Log::Datastore.normalize(
                    files,
                    output_path: out_pocolog_dir, index_dir: out_pocolog_cache_dir,
                    reporter: reporter, compute_sha256: true
                )
            ensure
                reporter&.finish
            end

            # @api private
            #
            # Copy text files found in the input directory into the dataset
            #
            # @param [Pathname] output_dir the target directory
            # @param [Array<Pathname>] paths the input text file paths
            # @return [void]
            def copy_text_files(output_dir, files)
                return if files.empty?

                out_text_dir = (output_dir + 'text')
                out_text_dir.mkpath
                FileUtils.cp files, out_text_dir
            end

            # @api private
            #
            # Copy the Roby logs into the target directory
            #
            # It computes the log file's SHA256 digests
            #
            # @param [Pathname] output_dir the target directory
            # @param [Array<Pathname>] paths the input roby log files
            # @return [Hash<Pathname,Digest::SHA256>] a hash of the log file's
            #   pathname to the file's SHA256 digest
            def copy_roby_event_log(output_dir, event_log)
                i = 0
                i += 1 while (target_file = output_dir + "roby-events.#{i}.log").file?

                FileUtils.cp event_log, target_file
                digest = Digest::SHA256.new
                digest.update(event_log.read)
                Hash[target_file => digest]
            end

            # @api private
            #
            # Copy the entries in the input directory that are not recognized as a
            # dataset element
            #
            # @param [Pathname] output_dir the target directory
            # @param [Array<Pathname>] paths the input elements, which can be
            #   pointing to both files and directories. Directories are copied
            #   recursively
            # @return [void]
            def copy_ignored_entries(output_dir, paths)
                return if paths.empty?

                out_ignored_dir = (output_dir + 'ignored')
                out_ignored_dir.mkpath
                FileUtils.cp_r paths, out_ignored_dir
            end
        end
    end
end
