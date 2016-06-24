require 'test_helper'
require 'tmpdir'

module Syskit::Pocolog
    describe Dataset do
        attr_reader :root_path, :dataset, :dataset_path
        attr_reader :roby_digest, :pocolog_digest

        def dataset_pathname(*names)
            dataset_path + File.join(*names)
        end

        before do
            @root_path = Pathname.new(Dir.mktmpdir)
            @dataset_path = root_path + 'dataset'
            (dataset_path + 'pocolog').mkpath
            (dataset_path + 'text').mkpath
            (dataset_path + 'ignored').mkpath
            @dataset = Dataset.new(dataset_path)

            create_logfile 'test.0.log' do
                create_logfile_stream 'test', 
                    metadata: Hash['rock_task_name' => 'task0', 'rock_task_object_name' => 'port']
            end
            FileUtils.mv logfile_pathname('test.0.log'), dataset_pathname('pocolog', 'task0::port.0.log')
            FileUtils.touch dataset_pathname('text', 'test.txt')
            dataset_pathname('roby-events.log').open('w') { |io| io.write "ROBY" }
            FileUtils.touch dataset_pathname('ignored', 'not_recognized_file')
            dataset_pathname('ignored', 'not_recognized_dir').mkpath
            FileUtils.touch dataset_pathname('ignored', 'not_recognized_dir', 'test')
        end
        after do
            root_path.rmtree
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
                    dataset_pathname('roby-events.log'),
                    dataset_pathname('pocolog', 'task0::port.0.log')].to_set
                assert_equal expected, files
            end
        end

        describe "#validate_encoded_sha2" do
            attr_reader :sha2
            before do
                @sha2 = Digest::SHA2.hexdigest("TEST")
            end
            it "raises if the string is too short" do
                assert_raises(Dataset::InvalidDigest) do
                    dataset.validate_encoded_sha2(sha2[0..-2])
                end
            end
            it "raises if the string is too long" do
                assert_raises(Dataset::InvalidDigest) do
                    dataset.validate_encoded_sha2(sha2 + " ")
                end
            end
            it "raises if the string contains invalid characters for base64" do
                sha2[3, 1] = '_'
                assert_raises(Dataset::InvalidDigest) do
                    dataset.validate_encoded_sha2(sha2)
                end
            end
            it "returns the digest unmodified if it is valid" do
                assert_equal sha2, dataset.validate_encoded_sha2(sha2)
            end
        end

        describe "#compute_dataset_identity_from_files" do
            it "returns a list of entries with full path, size and sha256 digest" do
                roby_path = dataset_pathname('roby-events.log')
                roby_digest = Digest::SHA256.hexdigest(roby_path.read)
                pocolog_path = dataset_pathname('pocolog', 'task0::port.0.log')
                pocolog_digest = Digest::SHA256.hexdigest(
                    pocolog_path.read[Pocolog::Format::Current::PROLOGUE_SIZE..-1])
                expected = Set[
                    Dataset::IdentityEntry.new(roby_path, roby_path.size, roby_digest),
                    Dataset::IdentityEntry.new(pocolog_path, pocolog_path.size, pocolog_digest)]
                assert_equal expected, dataset.compute_dataset_identity_from_files.to_set
            end
        end

        it "saves and loads the identity information in the dataset" do
            dataset.write_dataset_identity_to_metadata_file
            assert_equal dataset.compute_dataset_identity_from_files.to_set,
                dataset.read_dataset_identity_from_metadata_file.to_set
        end

        describe "#write_dataset_identity_to_metadata_file" do
            it "validates that the provided identity entries have paths within the dataset" do
                entry = Dataset::IdentityEntry.new(Pathname.new('/'), 10, Digest::SHA256.hexdigest(''))
                assert_raises(Dataset::InvalidIdentityMetadata) do
                    dataset.write_dataset_identity_to_metadata_file([entry])
                end
            end
            it "validates that the provided identity entries have sizes that are integers" do
                entry = Dataset::IdentityEntry.new(dataset_path + "file", 'not_a_number', Digest::SHA256.hexdigest(''))
                assert_raises(Dataset::InvalidIdentityMetadata) do
                    dataset.write_dataset_identity_to_metadata_file([entry])
                end
            end
            it "validates that the provided identity entries have sizes that are positive" do
                entry = Dataset::IdentityEntry.new(dataset_path + "file", -20, Digest::SHA256.hexdigest(''))
                assert_raises(Dataset::InvalidIdentityMetadata) do
                    dataset.write_dataset_identity_to_metadata_file([entry])
                end
            end
            it "validates that the provided identity entries have valid-looking sha256 digests" do
                entry = Dataset::IdentityEntry.new(dataset_path + "file", 10, 'invalid_digest')
                assert_raises(Dataset::InvalidIdentityMetadata) do
                    dataset.write_dataset_identity_to_metadata_file([entry])
                end
            end
            it "saves the result to the identity file" do
                file_digest = Digest::SHA256.hexdigest('file')
                dataset_digest = Digest::SHA256.hexdigest('dataset')
                entry = Dataset::IdentityEntry.new(dataset_path + "file", 10, file_digest)
                flexmock(dataset).should_receive(:compute_dataset_digest).with([entry]).and_return(dataset_digest)
                dataset.write_dataset_identity_to_metadata_file([entry])
                data = YAML.load((dataset_path + Dataset::BASENAME_IDENTITY_METADATA).read)
                expected = Hash['layout_version' => Dataset::LAYOUT_VERSION, 'sha2' => dataset_digest,
                                'identity' => [Hash['sha2' => file_digest, 'size' => 10, 'path' => 'file']]]
                assert_equal expected, data
            end
        end

        describe "#read_dataset_identity_from_metadata_file" do
            def write_metadata(overrides = Hash.new, layout_version: Dataset::LAYOUT_VERSION)
                metadata = Hash[
                    'path' => 'test',
                     'size' => 10,
                     'sha2' => Digest::SHA2.hexdigest('')].merge(overrides)
                (dataset_path + Dataset::BASENAME_IDENTITY_METADATA).open('w') do |io|
                    io.write YAML.dump(Hash['layout_version' => layout_version, 'identity' => [metadata]])
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
                write_metadata('path' => 'test')
                entry = dataset.read_dataset_identity_from_metadata_file.first
                assert_equal (dataset_path + 'test'), entry.path
            end
            it "validates that the paths are within the dataset" do
                write_metadata('path' => '../test')
                assert_raises(Dataset::InvalidIdentityMetadata) do
                    dataset.read_dataset_identity_from_metadata_file
                end
            end
            it "sets the entry's size" do
                write_metadata('size' => 20)
                entry = dataset.read_dataset_identity_from_metadata_file.first
                assert_equal 20, entry.size
            end
            it "sets the entry's size" do
                write_metadata('sha2' => Digest::SHA2.hexdigest('test'))
                entry = dataset.read_dataset_identity_from_metadata_file.first
                assert_equal Digest::SHA2.hexdigest('test'), entry.sha2
            end
            it "validates that the file's has an 'identity' field" do
                (dataset_path + Dataset::BASENAME_IDENTITY_METADATA).open('w') do |io|
                    io.write YAML.dump(Hash[])
                end
                assert_raises(Dataset::InvalidIdentityMetadata) do
                    dataset.read_dataset_identity_from_metadata_file
                end
            end
            it "validates that the file's 'identity' field is an array" do
                (dataset_path + Dataset::BASENAME_IDENTITY_METADATA).open('w') do |io|
                    io.write YAML.dump(Hash['identity' => Hash.new])
                end
                assert_raises(Dataset::InvalidIdentityMetadata) do
                    dataset.read_dataset_identity_from_metadata_file
                end
            end
            it "validates that the 'path' field contains a string" do
                write_metadata('path' => 10)
                assert_raises(Dataset::InvalidIdentityMetadata) do
                    dataset.read_dataset_identity_from_metadata_file
                end
            end
            it "validates that the 'size' field is an integer" do
                write_metadata('size' => 'not_a_number')
                assert_raises(Dataset::InvalidIdentityMetadata) do
                    dataset.read_dataset_identity_from_metadata_file
                end
            end
            it "validates that the 'sha2' field contains a string" do
                write_metadata('sha2' => 10)
                assert_raises(Dataset::InvalidIdentityMetadata) do
                    dataset.read_dataset_identity_from_metadata_file
                end
            end
            it "validates that the 'path' field contains a valid hash" do
                write_metadata('sha2' => 'aerpojapoj')
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
                dataset.validate_encoded_sha2(dataset.compute_dataset_digest)
            end
            it "is sensitive only to the file's relative paths" do
                digest = dataset.compute_dataset_digest
                FileUtils.mv dataset_path, (root_path + "moved_dataset")
                assert_equal digest, Dataset.new(root_path + "moved_dataset").compute_dataset_digest
            end
            it "computes the same hash with the same input" do
                assert_equal dataset.compute_dataset_digest, dataset.compute_dataset_digest
            end
            it "changes if the size of one of the files change" do
                entries = dataset.compute_dataset_identity_from_files
                entries[0].size += 10
                refute_equal dataset.compute_dataset_digest,
                    dataset.compute_dataset_digest(entries)
            end
            it "changes if the sha2 of one of the files change" do
                entries = dataset.compute_dataset_identity_from_files
                entries[0].sha2[10] = '0'
                refute_equal dataset.compute_dataset_digest,
                    dataset.compute_dataset_digest(entries)
            end
            it "changes if a new entry is added" do
                entries = dataset.compute_dataset_identity_from_files
                entries << Dataset::IdentityEntry.new(
                    root_path + 'new_file', 10, Digest::SHA2.hexdigest('test'))
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
                dataset_pathname("roby-events.log").unlink
                assert_raises(Dataset::InvalidIdentityMetadata) do
                    dataset.weak_validate_identity_metadata
                end
            end
            it "raises if a new important file is added on disk" do
                FileUtils.touch dataset_pathname("test-events.log")
                assert_raises(Dataset::InvalidIdentityMetadata) do
                    dataset.weak_validate_identity_metadata
                end
            end
            it "raises if a file size mismatches" do
                dataset_pathname("roby-events.log").open('a') { |io| io.write('10') }
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
                dataset_pathname("roby-events.log").unlink
                assert_raises(Dataset::InvalidIdentityMetadata) do
                    dataset.validate_identity_metadata
                end
            end
            it "raises if a new important file is added on disk" do
                FileUtils.touch dataset_pathname("test-events.log")
                assert_raises(Dataset::InvalidIdentityMetadata) do
                    dataset.validate_identity_metadata
                end
            end
            it "raises if a file size mismatches" do
                dataset_pathname("roby-events.log").open('a') { |io| io.write('10') }
                assert_raises(Dataset::InvalidIdentityMetadata) do
                    dataset.validate_identity_metadata
                end
            end
            it "raises if the contents of a file changed" do
                dataset_pathname("roby-events.log").open('a') { |io| io.seek(5); io.write('0') }
                assert_raises(Dataset::InvalidIdentityMetadata) do
                    dataset.validate_identity_metadata
                end
            end
        end

        describe "#metadata_reset" do
            before do
                (dataset_path + Dataset::BASENAME_METADATA).open('w') do |io|
                    YAML.dump(Hash['test' => [10]], io)
                end
            end
            it "empties the metadata" do
                dataset.metadata
                dataset.metadata_reset
                assert_equal Hash.new, dataset.metadata
            end
            it "does not cause a read from disk if called first" do
                flexmock(dataset).should_receive(:metadata_read_from_file).never
                dataset.metadata_reset
                dataset.metadata
            end
        end

        describe "#metadata" do
            it "loads the data from file" do
                (dataset_path + Dataset::BASENAME_METADATA).open('w') do |io|
                    YAML.dump(Hash['test' => [10]], io)
                end
                assert_equal Hash['test' => Set[10]], dataset.metadata
            end
            it "sets the metadata to an empty hash if there is no file" do
                assert_equal Hash.new, dataset.metadata
            end
            it "loads the metadata only once" do
                metadata_hash = dataset.metadata
                assert_same metadata_hash, dataset.metadata
            end
        end

        describe "#metadata_add" do
            it "creates a new key->values mapping" do
                dataset.metadata_add("test", 10, 20)
                assert_equal Hash['test' => Set[10, 20]], dataset.metadata
            end
            it "merges new values to existing ones" do
                dataset.metadata_add("test", 10, 20)
                dataset.metadata_add("test", 10, 30)
                assert_equal Hash['test' => Set[10, 20, 30]], dataset.metadata
            end
        end

        describe "#metadata_fetch" do
            it "returns a single value" do
                dataset.metadata_add 'test', 10
                assert_equal 10, dataset.metadata_fetch('test')
            end
            it "raises ArgumentError if more than one default value is given" do
                assert_raises(ArgumentError) do
                    dataset.metadata_fetch('test', 10, 20)
                end
            end
            it "raises NoValue if there are none" do
                assert_raises(Dataset::NoValue) do
                    dataset.metadata_fetch('test')
                end
            end
            it "raises MultipleValues if there is more than one" do
                dataset.metadata_add 'test', 10, 20
                assert_raises(Dataset::MultipleValues) do
                    dataset.metadata_fetch('test')
                end
            end
            it "returns the default if there is no value for the key" do
                assert_equal 10, dataset.metadata_fetch('test', 10)
            end
        end

        describe "#metadata_fetch_all" do
            it "returns all values for the key" do
                dataset.metadata_add 'test', 10, 20
                assert_equal Set[10, 20], dataset.metadata_fetch_all('test')
            end
            it "raises NoValue if there are none and no defaults are given" do
                assert_raises(Dataset::NoValue) do
                    dataset.metadata_fetch_all('test')
                end
            end
            it "returns the default if there is no value for the key" do
                assert_equal Set[10, 20], dataset.metadata_fetch_all('test', 10, 20)
            end
        end

        describe "#metadata_write_to_file" do
            it "writes an empty metadata hash if there is no metadata" do
                dataset.metadata_write_to_file
                assert_equal Hash[],
                    YAML.load((dataset_path + Dataset::BASENAME_METADATA).read)
            end
            it "writes the metadata to file" do
                dataset.metadata_add 'test', 10, 20
                dataset.metadata_write_to_file
                assert_equal Hash['test' => [10, 20]],
                    YAML.load((dataset_path + Dataset::BASENAME_METADATA).read)
            end
        end
    end
end

