# frozen_string_literal: true

require "test_helper"

module Syskit::Log
    describe Datastore do
        attr_reader :root_path, :datastore_path, :datastore

        before do
            @root_path = Pathname.new(Dir.mktmpdir)
            @datastore_path = root_path + "datastore"
            datastore_path.mkpath
            @datastore = Datastore.new(datastore_path)

            @__syskit_log_store_envvar = ENV.delete("SYSKIT_LOG_STORE")
        end

        after do
            if @__syskit_log_store_envvar
                ENV["SYSKIT_LOG_STORE"] = @__syskit_log_store_envvar
            else
                ENV.delete("SYSKIT_LOG_STORE")
            end
            root_path.rmtree
        end

        describe ".default" do
            it "returns the default datastore if one is defined" do
                ENV["SYSKIT_LOG_STORE"] = @datastore_path.to_s
                assert Datastore.default_defined?
                datastore = Datastore.default
                assert_equal @datastore_path, datastore.datastore_path
            end

            it "raises if there is no default" do
                refute Datastore.default_defined?
                assert_raises(ArgumentError) do
                    Datastore.default
                end
            end
        end

        describe "#in_incoming" do
            it "creates an incoming directory in the datastore and yields it" do
                datastore.in_incoming do |core_path, cache_path|
                    assert_equal (datastore_path + "incoming" + "0" + "core"), core_path
                    assert core_path.directory?
                    assert_equal (datastore_path + "incoming" + "0" + "cache"), cache_path
                    assert cache_path.directory?
                end
            end
            it "handles having another process create a path concurrently" do
                (datastore_path + "incoming").mkpath
                called = false
                flexmock(Pathname).new_instances.should_receive(:mkdir)
                                  .and_return do
                    unless called
                        called = true
                        raise Errno::EEXIST
                    end
                end

                datastore.in_incoming do |core_path, cache_path|
                    assert_equal (datastore_path + "incoming" + "1" + "core"), core_path
                    assert_equal (datastore_path + "incoming" + "1" + "cache"), cache_path
                end
            end
            it "ignores existing paths" do
                (datastore_path + "incoming" + "0").mkpath
                datastore.in_incoming do |core_path, cache_path|
                    assert_equal (datastore_path + "incoming" + "1" + "core"), core_path
                    assert_equal (datastore_path + "incoming" + "1" + "cache"), cache_path
                end
            end
            it "deletes the created paths if they still exist at the end of the block" do
                created_paths = datastore.in_incoming do |core_path, cache_path|
                    [core_path, cache_path]
                end
                refute created_paths.any?(&:exist?)
            end
            it "does nothing if the path does not exist anymore at the end of the block" do
                datastore.in_incoming do |core_path, cache_path|
                    FileUtils.mv core_path, (root_path + "core")
                    FileUtils.mv cache_path, (root_path + "cache")
                end
                assert (root_path + "core").exist?
                assert (root_path + "cache").exist?
            end
        end

        describe "#has?" do
            attr_reader :digest
            before do
                @digest = Datastore::Dataset.string_digest("exists")
                (datastore_path + "core" + digest).mkpath
            end

            it "returns false if there is no folder with the dataset digest in the store" do
                refute datastore.has?(Datastore::Dataset.string_digest("does_not_exist"))
            end
            it "returns true if there is a folder with the dataset digest in the store" do
                assert datastore.has?(digest)
            end
        end

        describe "#delete" do
            attr_reader :digest, :dataset_path, :cache_path
            before do
                @digest = Datastore::Dataset.string_digest("exists")
                @dataset_path = datastore.core_path_of(digest)
                dataset_path.mkpath
                @cache_path = datastore.cache_path_of(digest)
                cache_path.mkpath
            end

            it "deletes the dataset's path and its contents" do
                FileUtils.touch dataset_path + "file"
                datastore.delete(digest)
                assert !dataset_path.exist?
                assert !cache_path.exist?
            end

            it "ignores a missing cache path" do
                FileUtils.touch dataset_path + "file"
                cache_path.rmtree
                datastore.delete(digest)
                assert !dataset_path.exist?
            end
        end

        describe "#get" do
            attr_reader :digest, :dataset_path
            before do
                @digest = Datastore::Dataset.string_digest("exists")
                @dataset_path = datastore.core_path_of(digest)
                dataset_path.mkpath
                dataset = Datastore::Dataset.new(dataset_path)
                dataset.write_dataset_identity_to_metadata_file
                dataset.metadata_write_to_file
            end

            it "returns a Dataset object pointing to the path" do
                dataset = datastore.get(digest)
                assert_kind_of Datastore::Dataset, dataset
                assert_equal dataset_path, dataset.dataset_path
            end

            it "raises ArgumentError if the dataset does not exist" do
                assert_raises(ArgumentError) do
                    datastore.get(Datastore::Dataset.string_digest("does_not_exist"))
                end
            end

            it "accepts a short digest" do
                dataset = datastore.get(digest[0, 5])
                assert_kind_of Datastore::Dataset, dataset
                assert_equal dataset_path, dataset.dataset_path
            end

            it "handles redirections" do
                root_digest = Datastore::Dataset.string_digest("root")
                intermediate_digest = Datastore::Dataset.string_digest("intermediate")
                datastore.write_redirect(root_digest, to: intermediate_digest)
                datastore.write_redirect(intermediate_digest, to: @digest)

                assert_equal @digest, datastore.get(root_digest).digest
            end
        end

        describe "#find_dataset_from_short_digest" do
            before do
                create_dataset("a0ea") {}
                create_dataset("a0fa") {}
            end
            it "returns a dataset whose digest starts with the given string" do
                assert_equal datastore.core_path_of("a0ea"),
                             datastore.find_dataset_from_short_digest("a0e").dataset_path
            end
            it "returns nil if nothing matches" do
                assert_nil datastore.find_dataset_from_short_digest("b")
            end
            it "raises if more than one dataset matches" do
                assert_raises(Datastore::AmbiguousShortDigest) do
                    datastore.find_dataset_from_short_digest("a0")
                end
            end
        end

        describe "#short_digest" do
            before do
                create_dataset("a0ea") {}
                create_dataset("a0fa") {}
            end
            it "returns the N first digits of the dataset's digest if they are not ambiguous" do
                assert_equal "a0e", datastore.short_digest(flexmock(digest: "a0ea"), size: 3)
            end
            it "returns the full dataset's digest if the prefix is ambiguous" do
                assert_equal "a0ea", datastore.short_digest(flexmock(digest: "a0ea"), size: 2)
            end
        end

        describe "#find_all" do
            before do
                @ds_e = create_dataset("a0ea", metadata: { "a" => %w[some values] }) {}
                @ds_f = create_dataset("a0fa", metadata: { "a" => %w[other values] }) {}
            end

            it "resolves all datasets that have the given values in their metadata" do
                datasets = @datastore.find_all("a" => %w[values])
                assert_equal [@ds_e, @ds_f].map(&:dataset_path).to_set,
                             datasets.map(&:dataset_path).to_set
            end

            it "requires all the given values to match (AND)" do
                datasets = @datastore.find_all("a" => %w[some values])
                assert_equal [@ds_e.dataset_path],
                             datasets.map(&:dataset_path).to_a
            end
        end

        describe "#find" do
            it "returns a single value returned by find_all" do
                flexmock(@datastore).should_receive(:find_all).with(query = flexmock)
                                    .and_return([result = flexmock])
                assert_equal result, @datastore.find(query)
            end

            it "returns nil if find_all returns an empty array" do
                flexmock(@datastore).should_receive(:find_all).with(query = flexmock)
                                    .and_return([])
                assert_nil @datastore.find(query)
            end

            it "raises if more than one dataset matches" do
                flexmock(@datastore).should_receive(:find_all).with(query = flexmock)
                                    .and_return([1, 2, 3])
                assert_raises(ArgumentError) do
                    @datastore.find(query)
                end
            end
        end
    end
end
