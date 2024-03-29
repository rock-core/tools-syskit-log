# frozen_string_literal: true

require "test_helper"
require "tmpdir"

module Syskit::Log
    class Datastore
        describe Dataset do
            attr_reader :root_path, :dataset, :dataset_path, :cache_path, :store
            attr_reader :roby_digest, :pocolog_digest

            def dataset_pathname(*names, compress: compress?)
                path = dataset_path.join(*names[0..-2])
                if compress
                    path.join(names[-1] + ".zst")
                else
                    path.join(names[-1])
                end
            end

            def dataset_open(*names, mode:, compress: compress?)
                path = dataset_pathname(*names, compress: compress)
                io = path.open(mode)
                if path.extname == ".zst"
                    io = ZstdIO.new(
                        io, read: %w[r a].include?(mode), write: %w[w a].include?(mode)
                    )
                end
                return io unless block_given?

                begin
                    yield(io)
                ensure
                    io.close
                end
            end

            before do
                @root_path = Pathname.new(Dir.mktmpdir)
                @store = Datastore.new(root_path)
                @dataset_path = store.core_path_of("dataset")
                (dataset_path + "pocolog").mkpath
                (dataset_path + "text").mkpath
                (dataset_path + "ignored").mkpath
                @cache_path = store.cache_path_of("dataset")
                @dataset = Dataset.new(dataset_path, cache: cache_path, datastore: store)

                move_logfile_path (dataset_path + "pocolog").to_s
                create_logfile "task0::port.0.log" do
                    create_logfile_stream "test",
                                          metadata: Hash["rock_task_name" => "task0", "rock_task_object_name" => "port"]
                end
                FileUtils.touch dataset_pathname("text", "test.txt")
                dataset_open("roby-events.0.log", mode: "w") { |io| io.write "ROBY" }
                dataset_open("ignored", "not_recognized_file", mode: "w").close
                dataset_pathname("ignored", "not_recognized_dir", compress: false).mkpath
                dataset_open("ignored", "not_recognized_dir", "test", mode: "w").close
            end
            after do
                root_path.rmtree
            end

            describe "#read" do
                it "reads a file from the dataset" do
                    dataset_open("text", "somefile", mode: "w") do |f|
                        f.write("content")
                    end
                    assert_equal "content", @dataset.read("text", "somefile")
                    if compress?
                        # Make sure the decompressed file is in the right place if there
                        # is compression
                        assert_equal "content",
                                     (@dataset.cache_path / "text" / "somefile").read
                    end
                end

                it "handles files in the root folder" do
                    dataset_open("text", "somefile", mode: "w") { |f| f.write("content") }
                    assert_equal "content", @dataset.read("text", "somefile")
                end
            end

            describe "#glob" do
                before do
                    (dataset_path + "test").mkpath
                end

                it "finds an uncompressed file" do
                    dataset_open("test", "ab", mode: "w", compress: false) { |io| }
                    expected = dataset_pathname("test", "ab", compress: false)
                    assert_equal [expected], @dataset.glob("test", "a?").to_a
                end

                it "finds a compressed file" do
                    dataset_open("test", "ab", mode: "w", compress: true) { |io| }
                    expected = dataset_pathname("test", "ab", compress: true)
                    assert_equal [expected], @dataset.glob("test", "a?").to_a
                end

                it "returns only once a path when using globs at the end of the path" do
                    dataset_open("test", "ab", mode: "w", compress: true) { |io| }
                    dataset_open("test", "bb", mode: "w", compress: false) { |io| }
                    expected_a = dataset_pathname("test", "ab", compress: true)
                    expected_b = dataset_pathname("test", "bb", compress: false)

                    assert_equal Set[expected_a, expected_b],
                                 @dataset.glob("test", "*").to_set
                end
            end

            describe "#digest_from_path" do
                it "returns the path's base name if it is a valid SHA256 digest" do
                    digest = Digest::SHA256.hexdigest("TEST")
                    path = root_path + digest
                    path.mkpath
                    dataset = Dataset.new(path)
                    assert_equal digest, dataset.digest_from_path
                end
                it "raises InvalidPath if the path's base name is not looking like a valid SHA256" do
                    path = root_path + "INVALID"
                    path.mkpath
                    dataset = Dataset.new(path)
                    assert_raises(Dataset::InvalidPath) do
                        dataset.digest_from_path
                    end
                end
            end

            describe "#each_important_file" do
                it "lists the full paths to the pocolog and roby files" do
                    files = dataset.each_important_file.to_set
                    expected = [
                        dataset_pathname("roby-events.0.log"),
                        dataset_pathname("pocolog", "task0::port.0.log")
                    ].to_set
                    assert_equal expected, files
                end
            end

            describe "#compute_dataset_identity_from_files" do
                it "returns a list of entries with full path, size and sha256 digest" do
                    roby_path = dataset_path + "roby-events.0.log"
                    roby_data = dataset_open("roby-events.0.log", mode: "r", &:read)
                    roby_digest = Digest::SHA256.hexdigest(roby_data)
                    pocolog_path = dataset_path + "pocolog" + "task0::port.0.log"
                    pocolog_data =
                        dataset_open("pocolog", "task0::port.0.log", mode: "r", &:read)
                    pocolog_digest = Digest::SHA256.hexdigest(
                        pocolog_data[Pocolog::Format::Current::PROLOGUE_SIZE..-1]
                    )
                    expected = Set[
                        Dataset::IdentityEntry.new(
                            roby_path, roby_data.size, roby_digest
                        ),
                        Dataset::IdentityEntry.new(
                            pocolog_path, pocolog_data.size, pocolog_digest
                        )
                    ]
                    assert_equal expected,
                                 dataset.compute_dataset_identity_from_files.to_set
                end
            end

            it "saves and loads the identity information in the dataset" do
                dataset.write_dataset_identity_to_metadata_file
                assert_equal dataset.compute_dataset_identity_from_files.to_set,
                             dataset.read_dataset_identity_from_metadata_file.to_set
            end

            describe "#write_dataset_identity_to_metadata_file" do
                it "validates that the provided identity entries have paths within the dataset" do
                    entry = Dataset::IdentityEntry.new(Pathname.new("/"), 10, Digest::SHA256.hexdigest(""))
                    assert_raises(Dataset::InvalidIdentityMetadata) do
                        dataset.write_dataset_identity_to_metadata_file([entry])
                    end
                end
                it "validates that the provided identity entries have sizes that are integers" do
                    entry = Dataset::IdentityEntry.new(dataset_path + "file", "not_a_number", Digest::SHA256.hexdigest(""))
                    assert_raises(Dataset::InvalidIdentityMetadata) do
                        dataset.write_dataset_identity_to_metadata_file([entry])
                    end
                end
                it "validates that the provided identity entries have sizes that are positive" do
                    entry = Dataset::IdentityEntry.new(dataset_path + "file", -20, Digest::SHA256.hexdigest(""))
                    assert_raises(Dataset::InvalidIdentityMetadata) do
                        dataset.write_dataset_identity_to_metadata_file([entry])
                    end
                end
                it "validates that the provided identity entries have valid-looking sha256 digests" do
                    entry = Dataset::IdentityEntry.new(dataset_path + "file", 10, "invalid_digest")
                    assert_raises(Dataset::InvalidIdentityMetadata) do
                        dataset.write_dataset_identity_to_metadata_file([entry])
                    end
                end
                it "saves the result to the identity file" do
                    file_digest = Digest::SHA256.hexdigest("file")
                    dataset_digest = Digest::SHA256.hexdigest("dataset")
                    entry = Dataset::IdentityEntry.new(dataset_path + "file", 10, file_digest)
                    flexmock(dataset).should_receive(:compute_dataset_digest).with([entry]).and_return(dataset_digest)
                    dataset.write_dataset_identity_to_metadata_file([entry])
                    data = YAML.load((dataset_path + Dataset::BASENAME_IDENTITY_METADATA).read)
                    expected = Hash["layout_version" => Dataset::LAYOUT_VERSION, "sha2" => dataset_digest,
                                    "identity" => [Hash["sha2" => file_digest, "size" => 10, "path" => "file"]]]
                    assert_equal expected, data
                end
            end

            describe "#read_dataset_identity_from_metadata_file" do
                def write_metadata(
                    overrides = {}, layout_version: Dataset::LAYOUT_VERSION
                )
                    metadata = {
                        "path" => "test",
                        "size" => 10,
                        "sha2" => Digest::SHA2.hexdigest("")
                    }.merge(overrides)

                    (dataset_path + Dataset::BASENAME_IDENTITY_METADATA).open("w") do |io|
                        io.write YAML.dump(
                            { "layout_version" => layout_version,
                              "identity" => [metadata] }
                        )
                    end
                    metadata
                end

                it "raises InvalidLayoutVersion if there is a mismatch in the layout version" do
                    write_metadata(layout_version: Dataset::LAYOUT_VERSION - 1)
                    assert_raises(Dataset::InvalidLayoutVersion) do
                        dataset.read_dataset_identity_from_metadata_file
                    end
                end
                it "sets the entry's path to the file's absolute path" do
                    write_metadata({ "path" => "test" })
                    entry = dataset.read_dataset_identity_from_metadata_file.first
                    assert_equal (dataset_path + "test"), entry.path
                end
                it "validates that the paths are within the dataset" do
                    write_metadata({ "path" => "../test" })
                    assert_raises(Dataset::InvalidIdentityMetadata) do
                        dataset.read_dataset_identity_from_metadata_file
                    end
                end
                it "sets the entry's size" do
                    write_metadata({ "size" => 20 })
                    entry = dataset.read_dataset_identity_from_metadata_file.first
                    assert_equal 20, entry.size
                end
                it "sets the entry's size" do
                    write_metadata({ "sha2" => Digest::SHA2.hexdigest("test") })
                    entry = dataset.read_dataset_identity_from_metadata_file.first
                    assert_equal Digest::SHA2.hexdigest("test"), entry.sha2
                end
                it "validates that the file's has an 'identity' field" do
                    (dataset_path + Dataset::BASENAME_IDENTITY_METADATA).open("w") do |io|
                        io.write YAML.dump(Hash["layout_version" => Dataset::LAYOUT_VERSION])
                    end
                    e = assert_raises(Dataset::InvalidIdentityMetadata) do
                        dataset.read_dataset_identity_from_metadata_file
                    end
                    assert_match(/no 'identity' field/, e.message)
                end
                it "validates that the file's 'identity' field is an array" do
                    (dataset_path + Dataset::BASENAME_IDENTITY_METADATA).open("w") do |io|
                        io.write YAML.dump(Hash["layout_version" => Dataset::LAYOUT_VERSION, "identity" => {}])
                    end
                    e = assert_raises(Dataset::InvalidIdentityMetadata) do
                        dataset.read_dataset_identity_from_metadata_file
                    end
                    assert_match(/the 'identity' field.*is not an array/, e.message)
                end
                it "validates that the 'path' field contains a string" do
                    write_metadata({ "path" => 10 })
                    assert_raises(Dataset::InvalidIdentityMetadata) do
                        dataset.read_dataset_identity_from_metadata_file
                    end
                end
                it "validates that the 'size' field is an integer" do
                    write_metadata({ "size" => "not_a_number" })
                    assert_raises(Dataset::InvalidIdentityMetadata) do
                        dataset.read_dataset_identity_from_metadata_file
                    end
                end
                it "validates that the 'sha2' field contains a string" do
                    write_metadata({ "sha2" => 10 })
                    assert_raises(Dataset::InvalidIdentityMetadata) do
                        dataset.read_dataset_identity_from_metadata_file
                    end
                end
                it "validates that the 'path' field contains a valid hash" do
                    write_metadata({ "sha2" => "aerpojapoj" })
                    assert_raises(Dataset::InvalidIdentityMetadata) do
                        dataset.read_dataset_identity_from_metadata_file
                    end
                end
            end
            describe "compute_dataset_digest" do
                before do
                    dataset.write_dataset_identity_to_metadata_file
                end
                it "computes a sha2 hash" do
                    DatasetIdentity.validate_encoded_sha2(dataset.compute_dataset_digest)
                end
                it "is sensitive only to the file's relative paths" do
                    digest = dataset.compute_dataset_digest
                    FileUtils.mv dataset_path, (root_path + "moved_dataset")
                    assert_equal digest, Dataset.new(root_path + "moved_dataset").compute_dataset_digest
                end
                it "computes the same hash with the same input" do
                    assert_equal dataset.compute_dataset_digest, dataset.compute_dataset_digest
                end
                it "is not sensitive to an entry being compressed or not" do
                    entries = dataset.compute_dataset_identity_from_files
                    digest = dataset.compute_dataset_digest(entries)

                    path = entries[0].path
                    compressed_path = path.dirname + "#{path.basename}.zst"
                    if compress?
                        Syskit::Log.decompress(compressed_path, path)
                        compressed_path.unlink
                    else
                        Syskit::Log.compress(path, compressed_path)
                        path.unlink
                    end

                    new_entries = dataset.compute_dataset_identity_from_files
                    assert_equal digest, dataset.compute_dataset_digest(new_entries)
                end
                it "changes if the size of one of the files change" do
                    entries = dataset.compute_dataset_identity_from_files
                    entries[0].size += 10
                    refute_equal dataset.compute_dataset_digest,
                                 dataset.compute_dataset_digest(entries)
                end
                it "changes if the sha2 of one of the files change" do
                    entries = dataset.compute_dataset_identity_from_files
                    entries[0].sha2[10] = "0"
                    refute_equal dataset.compute_dataset_digest,
                                 dataset.compute_dataset_digest(entries)
                end
                it "changes if a new entry is added" do
                    entries = dataset.compute_dataset_identity_from_files
                    entries << Dataset::IdentityEntry.new(
                        root_path + "new_file", 10, Digest::SHA2.hexdigest("test")
                    )
                    refute_equal dataset.compute_dataset_digest,
                                 dataset.compute_dataset_digest(entries)
                end
                it "changes if an entry is removed" do
                    entries = dataset.compute_dataset_identity_from_files
                    entries.pop
                    refute_equal dataset.compute_dataset_digest,
                                 dataset.compute_dataset_digest(entries)
                end
                it "is not sensitive to the identity entries order" do
                    entries = dataset.compute_dataset_identity_from_files
                    entries = [entries[1], entries[0]]
                    assert_equal dataset.compute_dataset_digest,
                                 dataset.compute_dataset_digest(entries)
                end
            end
            describe "weak_validate_identity_metadata" do
                before do
                    dataset.write_dataset_identity_to_metadata_file
                end
                it "passes if the metadata and dataset match" do
                    dataset.weak_validate_identity_metadata
                end
                it "raises if a file is missing on disk" do
                    dataset_pathname("roby-events.0.log").unlink
                    assert_raises(Dataset::InvalidIdentityMetadata) do
                        dataset.weak_validate_identity_metadata
                    end
                end
                it "raises if a new pocolog log file is added on disk" do
                    dataset_open("pocolog", "some::Task.0.log", mode: "w").close
                    assert_raises(Dataset::InvalidIdentityMetadata) do
                        dataset.weak_validate_identity_metadata
                    end
                end
                it "raises if a new Roby log file is added on disk" do
                    dataset_open("roby-events.1.log", mode: "w").close
                    assert_raises(Dataset::InvalidIdentityMetadata) do
                        dataset.weak_validate_identity_metadata
                    end
                end
            end

            describe "validate_identity_metadata" do
                before do
                    dataset.write_dataset_identity_to_metadata_file
                end
                it "passes if the metadata and dataset match" do
                    dataset.validate_identity_metadata
                end
                it "raises if a file is missing on disk" do
                    dataset_pathname("roby-events.0.log").unlink
                    assert_raises(Dataset::InvalidIdentityMetadata) do
                        dataset.validate_identity_metadata
                    end
                end
                it "raises if a new pocolog log file is added on disk" do
                    dataset_open("pocolog", "some::Task.0.log", mode: "w").close
                    assert_raises(Dataset::InvalidIdentityMetadata) do
                        dataset.validate_identity_metadata
                    end
                end
                it "raises if a new Roby log file is added on disk" do
                    dataset_open("roby-events.1.log", mode: "w").close
                    assert_raises(Dataset::InvalidIdentityMetadata) do
                        dataset.validate_identity_metadata
                    end
                end
                it "raises if a file size mismatches" do
                    dataset_open("roby-events.0.log", mode: "a") { |io| io.write("10") }
                    assert_raises(Dataset::InvalidIdentityMetadata) do
                        dataset.validate_identity_metadata
                    end
                end
                it "raises if the contents of a file changed" do
                    dataset_open("roby-events.0.log", mode: "w") { |io| io.write "ROBA" }
                    assert_raises(Dataset::InvalidIdentityMetadata) do
                        dataset.validate_identity_metadata
                    end
                end
            end

            describe "#metadata_reset" do
                before do
                    (dataset_path + Dataset::BASENAME_METADATA).open("w") do |io|
                        YAML.dump(Hash["test" => [10]], io)
                    end
                end
                it "empties the metadata" do
                    dataset.metadata
                    dataset.metadata_reset
                    assert_equal({}, dataset.metadata)
                end
                it "does not cause a read from disk if called first" do
                    flexmock(dataset).should_receive(:metadata_read_from_file).never
                    dataset.metadata_reset
                    dataset.metadata
                end
            end

            describe "#metadata" do
                it "loads the data from file" do
                    (dataset_path + Dataset::BASENAME_METADATA).open("w") do |io|
                        YAML.dump(Hash["test" => [10]], io)
                    end
                    assert_equal Hash["test" => Set[10]], dataset.metadata
                end
                it "sets the metadata to an empty hash if there is no file" do
                    assert_equal({}, dataset.metadata)
                end
                it "loads the metadata only once" do
                    metadata_hash = dataset.metadata
                    assert_same metadata_hash, dataset.metadata
                end
            end

            describe "#metadata_set" do
                it "creates a new key->values mapping" do
                    dataset.metadata_set("test", 10, 20)
                    assert_equal Hash["test" => Set[10, 20]], dataset.metadata
                end
                it "resets existing values with the new ones" do
                    dataset.metadata_add("test", 10, 20)
                    dataset.metadata_set("test", 30, 40)
                    assert_equal Hash["test" => Set[30, 40]], dataset.metadata
                end
            end

            describe "#metadata_add" do
                it "creates a new key->values mapping" do
                    dataset.metadata_add("test", 10, 20)
                    assert_equal Hash["test" => Set[10, 20]], dataset.metadata
                end
                it "merges new values to existing ones" do
                    dataset.metadata_add("test", 10, 20)
                    dataset.metadata_add("test", 10, 30)
                    assert_equal Hash["test" => Set[10, 20, 30]], dataset.metadata
                end
            end

            describe "#metadata?" do
                it "returns true if the key exists" do
                    dataset.metadata_add "test", 10
                    assert dataset.metadata?("test")
                end
                it "returns false if the key does not exist" do
                    refute dataset.metadata?("test")
                end
            end

            describe "#metadata_get" do
                it "returns the entry's set" do
                    dataset.metadata_add "test", 10
                    assert_equal Set[10], dataset.metadata_get("test")
                end
                it "returns nil if the key does not exist" do
                    assert_nil dataset.metadata_get("test")
                end
            end

            describe "#metadata_fetch" do
                it "returns a single value" do
                    dataset.metadata_add "test", 10
                    assert_equal 10, dataset.metadata_fetch("test")
                end
                it "raises ArgumentError if more than one default value is given" do
                    assert_raises(ArgumentError) do
                        dataset.metadata_fetch("test", 10, 20)
                    end
                end
                it "raises NoValue if there are none" do
                    assert_raises(Dataset::NoValue) do
                        dataset.metadata_fetch("test")
                    end
                end
                it "raises MultipleValues if there is more than one" do
                    dataset.metadata_add "test", 10, 20
                    assert_raises(Dataset::MultipleValues) do
                        dataset.metadata_fetch("test")
                    end
                end
                it "returns the default if there is no value for the key" do
                    assert_equal 10, dataset.metadata_fetch("test", 10)
                end
            end

            describe "#metadata_fetch_all" do
                it "returns all values for the key" do
                    dataset.metadata_add "test", 10, 20
                    assert_equal Set[10, 20], dataset.metadata_fetch_all("test")
                end
                it "raises NoValue if there are none and no defaults are given" do
                    assert_raises(Dataset::NoValue) do
                        dataset.metadata_fetch_all("test")
                    end
                end
                it "returns the default if there is no value for the key" do
                    assert_equal Set[10, 20], dataset.metadata_fetch_all("test", 10, 20)
                end
            end

            describe "#metadata_write_to_file" do
                it "writes an empty metadata hash if there is no metadata" do
                    dataset.metadata_write_to_file
                    assert_equal Hash[],
                                 YAML.load((dataset_path + Dataset::BASENAME_METADATA).read)
                end
                it "writes the metadata to file" do
                    dataset.metadata_add "test", 10, 20
                    dataset.metadata_write_to_file
                    assert_equal Hash["test" => [10, 20]],
                                 YAML.load((dataset_path + Dataset::BASENAME_METADATA).read)
                end
            end

            describe "#each_pocolog_path" do
                it "enumerates the pocolog files in the dataset" do
                    paths = dataset.each_pocolog_path.to_a
                    assert_equal [logfile_pathname("task0::port.0.log")], paths
                end
            end

            describe "#each_pocolog_stream" do
                it "expects the pocolog cache files in the dataset's cache directory" do
                    cache_path.mkpath
                    open_logfile "task0::port.0.log", index_dir: (cache_path + "pocolog").to_s
                    flexmock(Pocolog::Logfiles)
                        .new_instances
                        .should_receive(:rebuild_and_load_index)
                        .never
                    streams = dataset.each_pocolog_stream.to_a
                    assert_equal ["test"], streams.map(&:name)
                end
            end

            describe "#read_lazy_data_stream" do
                attr_reader :base_time, :double_t
                before do
                    @base_time = Time.at(342983, 3219)
                    registry = Typelib::Registry.new
                    @double_t = registry.create_numeric "/double", 8, :float
                    create_logfile "task0::port.0.log" do
                        create_logfile_stream "test",
                                              metadata: Hash["rock_task_name" => "task0", "rock_task_object_name" => "port"]
                        write_logfile_sample base_time, base_time + 10, 1
                        write_logfile_sample base_time + 1, base_time + 20, 2
                    end
                    create_logfile "task0::other.0.log" do
                        create_logfile_stream "other_test",
                                              metadata: Hash["rock_task_name" => "task0", "rock_task_object_name" => "other"],
                                              type: double_t
                        write_logfile_sample base_time + 100, base_time + 300, 3
                    end
                    cache_path.mkpath
                    open_logfile "task0::port.0.log",
                                 index_dir: (cache_path + "pocolog").to_s
                    open_logfile "task0::other.0.log",
                                 index_dir: (cache_path + "pocolog").to_s
                end

                it "loads stream information and returns LazyDataStream objects" do
                    streams = dataset.read_lazy_data_streams.sort_by(&:name).reverse
                    assert_equal %w[test other_test], streams.map(&:name)
                    assert_equal [int32_t, double_t], streams.map(&:type)
                    assert_equal [Hash["rock_task_name" => "task0", "rock_task_object_name" => "port"],
                                  Hash["rock_task_name" => "task0", "rock_task_object_name" => "other"]],
                                 streams.map(&:metadata)
                    assert_equal [[base_time, base_time + 1], [base_time + 100, base_time + 100]],
                                 streams.map(&:interval_rt)
                    assert_equal [[base_time + 10, base_time + 20], [base_time + 300, base_time + 300]],
                                 streams.map(&:interval_lg)
                    assert_equal [2, 1], streams.map(&:size)
                end

                it "sets up the lazy data stream to load the actual stream properly" do
                    lazy_streams = dataset.read_lazy_data_streams
                    flexmock(Pocolog::Logfiles)
                        .new_instances
                        .should_receive(:rebuild_and_load_index).never
                    streams = lazy_streams.map(&:syskit_eager_load).sort_by(&:name)
                                          .reverse
                    assert_equal %w[test other_test], streams.map(&:name)
                    assert_equal [int32_t, double_t], streams.map(&:type)
                    assert_equal [Hash["rock_task_name" => "task0", "rock_task_object_name" => "port"],
                                  Hash["rock_task_name" => "task0", "rock_task_object_name" => "other"]],
                                 streams.map(&:metadata)
                    assert_equal [[base_time, base_time + 1], [base_time + 100, base_time + 100]],
                                 streams.map(&:interval_rt)
                    assert_equal [[base_time + 10, base_time + 20], [base_time + 300, base_time + 300]],
                                 streams.map(&:interval_lg)
                    assert_equal [2, 1], streams.map(&:size)
                end
            end

            describe "#compute_timestamp" do
                it "uses the roby time when available" do
                    ds = Dataset.new(@dataset_path)
                    ds.metadata_set "roby:time", "20211214-1115"
                    assert_equal Time.utc(2021, 12, 14, 11, 15).tv_sec,
                                 ds.compute_timestamp
                end

                it "handles leading zeroes in some of the fields" do
                    ds = Dataset.new(@dataset_path)
                    ds.metadata_set "roby:time", "20210704-0105"
                    assert_equal Time.utc(2021, 7, 4, 1, 5).tv_sec,
                                 ds.compute_timestamp
                end

                it "uses the smallest roby:time if there is more than one" do
                    ds = Dataset.new(@dataset_path)
                    ds.metadata_set(
                        "roby:time",
                        "20210704-1205",
                        "20210705-1215",
                        "20210704-0105",
                        "20210704-0115"
                    )
                    assert_equal Time.utc(2021, 7, 4, 1, 5).tv_sec,
                                 ds.compute_timestamp
                end
            end

            describe "#upgrade_value_to" do
                it "upgrades a value to a target given as type" do
                    cxx = Typelib::CXXRegistry.new
                    int32 = cxx.get("/int32_t").from_ruby(32)
                    int64_t = cxx.get("/int64_t")
                    result = @dataset.upgrade_value_to(int32, int64_t)
                    assert_equal 32, result.to_ruby
                end

                it "upgrades a value to a target given as value" do
                    cxx = Typelib::CXXRegistry.new
                    int32 = cxx.get("/int32_t").from_ruby(32)
                    int64 = cxx.get("/int64_t").new
                    result = @dataset.upgrade_value_to(int32, int64)
                    assert_same int64, result
                    assert_equal 32, result.to_ruby
                end

                it "uses the custom converters registered on the datastore" do
                    cxx = Typelib::CXXRegistry.new
                    int32_t = cxx.get("/int32_t")
                    int64_t = cxx.get("/int64_t")
                    @store.upgrade_converter_registry
                          .add(Time.at(10), int32_t, int64_t) do |to, from|
                              int64 = Typelib.from_ruby(from.to_ruby * 2, int64_t)
                              Typelib.copy(to, int64)
                          end

                    result = @dataset.upgrade_value_to(
                        int32_t.from_ruby(32), int64_t, reference_time: Time.at(9)
                    )
                    assert_equal 64, result.to_ruby
                end

                it "uses the given reference time to build the conversion" do
                    cxx = Typelib::CXXRegistry.new
                    int32_t = cxx.get("/int32_t")
                    int64_t = cxx.get("/int64_t")
                    @store.upgrade_converter_registry
                          .add(Time.at(10), int32_t, int64_t) do |to, from|
                              int64 = Typelib.from_ruby(from.to_ruby * 2, int64_t)
                              Typelib.copy(to, int64)
                          end

                    result = @dataset.upgrade_value_to(
                        int32_t.from_ruby(32), int64_t, reference_time: Time.at(11)
                    )
                    assert_equal 32, result.to_ruby
                end
            end
        end
    end
end
