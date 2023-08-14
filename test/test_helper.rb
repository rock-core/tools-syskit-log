# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "syskit/test/self"
require "syskit/log"
require "pocolog"
require "pocolog/test_helpers"
require "minitest/autorun"

require "syskit/log/datastore/index_build"
require "syskit/log/datastore/import"

module Syskit::Log
    module Test
        include Pocolog::TestHelpers

        def setup
            @pocolog_log_level = Pocolog.logger.level
            Pocolog.logger.level = Logger::WARN
            unless Roby.app.loaded_plugin?("syskit-log")
                Roby.app.add_plugin("syskit-log", Syskit::Log::Plugin)
            end

            @temp_paths = []
            super
        end

        def teardown
            Pocolog.logger.level = @pocolog_log_level if @pocolog_log_level
            @temp_paths.each(&:rmtree)
            super
        end

        def make_tmppath
            dir = Dir.mktmpdir
            path = Pathname.new(dir)
            @temp_paths << path
            path
        end

        def create_datastore(path)
            @datastore = Datastore.create(path)
        end

        def create_dataset(digest, metadata: {})
            unless @datastore
                raise ArgumentError, "must call #create_datastore before #create_dataset"
            end

            core_path = @datastore.core_path_of(digest)
            core_path.mkpath
            move_logfile_path(core_path + "pocolog", delete_current: false)
            dataset = Datastore::Dataset.new(core_path, cache: @datastore.cache_path_of(digest))
            return dataset unless block_given?

            begin
                yield
                dataset
            ensure
                identity = dataset.compute_dataset_identity_from_files
                dataset.write_dataset_identity_to_metadata_file(identity)
                metadata.each do |k, v|
                    dataset.metadata_set(k, *v)
                end
                dataset.metadata_write_to_file
                Datastore.index_build(@datastore, dataset)
            end
        end

        def compress?
            ENV["SYSKIT_LOG_TEST_COMPRESS"] == "1"
        end

        def roby_log_path(name)
            Pathname(__dir__) + "roby-logs" + "#{name}-events.log"
        end

        # Create a "ready to use" datastore based on the given data in fixtures/
        #
        # @return [(Datastore, Datastore::Dataset)]
        def prepare_fixture_datastore(name)
            src_path = Pathname.new(__dir__) / "datastore" / "fixtures" / name
            dst_path = make_tmppath
            FileUtils.cp_r src_path, dst_path

            store = Datastore.new(dst_path / name)
            set = store.each_dataset.first
            Datastore::IndexBuild.rebuild(store, set)

            [store, set]
        end

        def logfile_pathname(*path)
            raw = Pathname.new(logfile_path(*path))
            return raw unless /-events\.log$|\.\d+\.log$/.match?(path.last)
            return raw unless compress?

            Pathname.new(logfile_path(*path[0..-2], path.last + ".zst"))
        end

        def open_logfile(path, index_dir: nil, **kw)
            path = logfile_pathname(*path)
            index_dir ||= path.dirname
            return super(path, index_dir: index_dir.to_s, **kw) unless compress?

            decompressed = Syskit::Log.decompressed(path, Pathname(index_dir))
            super(decompressed, index_dir: index_dir.to_s, **kw)
        end

        def open_logfile_stream(path, stream_name, **kw)
            open_logfile(path, **kw).stream(stream_name)
        end

        def read_logfile(*name)
            path = logfile_pathname(*name)
            data = path.read
            return data unless path.extname == ".zst"

            Zstd.decompress(data)
        end

        def write_logfile(name, data)
            path = logfile_pathname(name)
            data = Zstd.compress(data) if path.extname == ".zst"
            path.write data
        end

        def create_logfile(name, truncate: 0)
            path = Pathname.new(super(name))
            path.truncate(path.stat.size - truncate)
            return path unless compress?

            compressed = Zstd.compress(path.read)
            compressed_path = path.sub_ext(".log.zst")
            compressed_path.write(compressed)
            path.unlink
            compressed_path
        end

        def create_roby_logfile(name)
            path = Pathname(logfile_path(name))
            path.open "w" do |io|
                Roby::DRoby::Logfile.write_header(io)
            end
            return path unless compress?

            compressed = Zstd.compress(path.read)
            compressed_path = path.sub_ext(".log.zst")
            compressed_path.write(compressed)
            path.unlink
            compressed_path
        end

        def copy_in_file(in_path, name)
            out_path = logfile_pathname(name)
            data = in_path.read
            data = Zstd.compress(data) if compress?

            out_path.write(data)
        end

        def import_logfiles(path = logfile_pathname)
            root_path = make_tmppath
            datastore_path = root_path + "datastore"
            datastore_path.mkpath
            datastore = Datastore.create(datastore_path)

            import = Datastore::Import.new(
                root_path + "import-core",
                compress: compress?,
                cache_path: root_path + "import-cache"
            )
            dataset = import.normalize_dataset([path])
            dataset = Datastore::Import.move_dataset_to_store(dataset, datastore)
            [datastore, dataset]
        end
    end
end
Minitest::Test.include Syskit::Log::Test
