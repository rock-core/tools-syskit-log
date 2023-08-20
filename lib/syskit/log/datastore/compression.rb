# frozen_string_literal: true

module Syskit::Log
    class Datastore
        # Implementation of compressing a dataset's data
        module Compression
            def self.compress_dataset(dataset, reporter: NullReporter.new)
                # First do the "important" files
                identity = dataset.read_dataset_identity_from_metadata_file
                identity.each do |entry|
                    path = entry.path
                    # Identity entries never have the .zst extension ... if it does
                    # not exist, we can skip it (i.e. it's already compressed)
                    unless path.file?
                        reporter.info "#{path} already compressed"
                        next
                    end

                    path.unlink if compress_file(path, reporter: reporter)
                end

                handle_auxiliary_files(dataset, %w[text ignores]) do |path|
                    compress_file(path, reporter: reporter)
                end
            end

            def self.compress_file(path, reporter: NullReporter.new)
                if path.extname == ".zst"
                    reporter.info "#{path} already compressed"
                    return
                end

                compressed_path = path.dirname + "#{path.basename}.zst"
                reporter.info "compressing #{path}"
                Syskit::Log.compress(path, compressed_path)
                true
            end

            def self.decompress_dataset(dataset, reporter: NullReporter.new)
                # First do the "important" files
                identity = dataset.read_dataset_identity_from_metadata_file
                identity.each do |entry|
                    path = entry.path
                    # Identity entries never have the .zst extension ... if it does
                    # not exist, we can skip it (i.e. it's already compressed)
                    if path.file?
                        reporter.info "#{path} is not compressed"
                        next
                    end

                    compressed_path = path.dirname + "#{path.basename}.zst"
                    if decompress_file(compressed_path, reporter: reporter)
                        compressed_path.unlink
                    end
                end

                handle_auxiliary_files(dataset, %w[text ignores]) do |path|
                    decompress_file(path, reporter: reporter)
                end
            end

            def self.decompress_each_important_file(dataset, reporter: NullReporter.new)
                identity = dataset.read_dataset_identity_from_metadata_file
                identity.each do |entry|
                    path = entry.path
                    # Identity entries never have the .zst extension ... if it does
                    # not exist, we can skip it (i.e. it's already compressed)
                    if path.file?
                        reporter.info "#{path} is not compressed"
                        next
                    end

                    yield(path)
                end
            end

            def self.decompress_file(path, reporter: NullReporter.new)
                if path.extname != ".zst"
                    reporter.info "#{path} is not compressed"
                    return
                end

                decompressed_path = path.sub_ext("")
                reporter.info "decompressing #{path}"
                Syskit::Log.decompress(path, decompressed_path)
                true
            end

            def self.handle_auxiliary_files(dataset, subdirs)
                subdirs.each do |subdir|
                    (dataset.dataset_path + subdir).glob("*") do |path|
                        next unless path.file?

                        path.unlink if yield(path)
                    end
                end
            end
        end
    end
end
