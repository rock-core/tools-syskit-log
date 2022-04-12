# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "syskit/test/self"
require "syskit/log"
require "pocolog"
require "pocolog/test_helpers"
require "minitest/autorun"
require "syskit/log/datastore/index_build"

module Syskit::Log
    module Test
        include Pocolog::TestHelpers

        def setup
            @pocolog_log_level = Pocolog.logger.level
            Pocolog.logger.level = Logger::WARN
            unless Roby.app.loaded_plugin?("syskit-log")
                Roby.app.add_plugin("syskit-log", Syskit::Log::Plugin)
            end

            super
        end

        def teardown
            Pocolog.logger.level = @pocolog_log_level if @pocolog_log_level
            super
        end

        def logfile_pathname(*basename)
            Pathname.new(logfile_path(*basename))
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

        def roby_log_path(name)
            Pathname(__dir__) + "roby-logs" + "#{name}-events.log"
        end

        def create_roby_logfile(name)
            path = Pathname(logfile_path(name))
            path.open "w" do |io|
                Roby::DRoby::Logfile.write_header(io)
            end
            path
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
    end
end
Minitest::Test.include Syskit::Log::Test
