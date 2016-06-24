module Syskit::Pocolog
    # A representation of a set of streams and their metadata
    #
    # If one ran 'syskit pocolog normalize', this loads using a generated
    # metadata file, and loads actual data indexes on-demand. Otherwise, it
    # loads the files to extract the metadata
    class Dataset
        include Logger::Hierarchy

        LAYOUT_VERSION = 1

        class InvalidPath < ArgumentError; end
        class InvalidDigest < ArgumentError; end
        class InvalidIdentityMetadata < ArgumentError; end
        class InvalidLayoutVersion < InvalidIdentityMetadata; end

        class MultipleValues < ArgumentError; end
        class NoValue < ArgumentError; end

        # The way we encode digests into strings
        #
        # Make sure you change {ENCODED_DIGEST_LENGTH} and
        # {validate_encoded_sha2} if you change this
        DIGEST_ENCODING_METHOD = :hexdigest

        # Length in characters of the digests once encoded in text form
        #
        # We're encoding a sha256 digest in hex, so that's 64 characters
        ENCODED_DIGEST_LENGTH = 64

        # The basename of the file that contains identifying metadata
        #
        # @see write_identifying_metadata
        BASENAME_IDENTITY_METADATA = "syskit-dataset.yml"

        # The basename of the file that contains identifying metadata
        #
        # @see write_identifying_metadata
        BASENAME_METADATA = "syskit-metadata.yml"

        IdentityEntry = Struct.new :path, :size, :sha2

        # The path to the dataset
        #
        # @return [Pathname]
        attr_reader :dataset_dir

        def initialize(path)
            @dataset_dir = path.realpath
            @metadata = nil
        end

        # Return the digest from the dataset's path
        #
        # @param [Pathname] path the dataset path
        # @raise [InvalidPath] if the path's dirname does not match a digest
        #   format
        def digest_from_path
            digest = dataset_dir.basename.to_s
            begin validate_encoded_sha2(digest)
            rescue InvalidDigest => e
                raise InvalidPath, "#{dataset_dir}'s name does not look like a valid digest: #{e.message}"
            end
            digest
        end

        # Computes the dataset identity by reading the files
        #
        # The result is suitable to call e.g. {#write_identifying_metadata}
        #
        # @return [Hash<Pathname,(String,Integer)>]
        def compute_dataset_identity_from_files
            each_important_file.map do |path|
                sha2 = path.open do |io|
                    if path.dirname.basename.to_s == 'pocolog' # Pocolog files do not hash their prologue
                        io.seek(Pocolog::Format::Current::PROLOGUE_SIZE)
                    end
                    compute_file_sha2(io)
                end
                IdentityEntry.new(path, path.size, sha2)
            end
        end

        # Validate that the argument looks like a valid sha2 digest encoded
        # with {DIGEST_ENCODING_METHOD}
        def validate_encoded_sha2(sha2)
            if sha2.length != ENCODED_DIGEST_LENGTH
                raise InvalidDigest, "#{sha2} does not look like a valid SHA2 digest encoded with #{DIGEST_ENCODING_METHOD}. Expected #{ENCODED_DIGEST_LENGTH} characters but got #{sha2.length}"
            elsif sha2 !~ /^[0-9a-f]+$/
                raise InvalidDigest, "#{sha2} does not look like a valid SHA2 digest encoded with #{DIGEST_ENCODING_METHOD}. Expected characters in 0-9a-zA-Z+/"
            end
            sha2
        end

        # Load the dataset identity information from the metadata file 
        #
        # It does sanity checks on the loaded data, but does not compare it
        # against the actual data on disk
        #
        # @return [Hash<Pathname,(String,Integer)>]
        def read_dataset_identity_from_metadata_file
            metadata_path = (dataset_dir + BASENAME_IDENTITY_METADATA)
            identity_metadata = (YAML.load(metadata_path.read) || Hash.new)
            if identity_metadata['layout_version'] != LAYOUT_VERSION
                raise InvalidLayoutVersion, "layout version in #{dataset_dir} is #{identity_metadata['layout_version']}, expected #{LAYOUT_VERSION}"
            end
            digests = identity_metadata['identity']
            if !digests
                raise InvalidIdentityMetadata, "no 'identity' field in #{metadata_path}"
            elsif !digests.respond_to?(:to_ary)
                raise InvalidIdentityMetadata, "the 'identity' field in #{metadata_path} is not an array"
            end
            digests = digests.map do |path_info|
                if !path_info['path'].respond_to?(:to_str)
                    raise InvalidIdentityMetadata, "found non-string value for field 'path' in #{metadata_path}"
                elsif !path_info['size'].kind_of?(Integer)
                    raise InvalidIdentityMetadata, "found non-integral value for field 'size' in #{metadata_path}"
                elsif !path_info['sha2'].respond_to?(:to_str)
                    raise InvalidIdentityMetadata, "found non-string value for field 'sha2' in #{metadata_path}"
                end

                begin
                    validate_encoded_sha2(path_info['sha2'])
                rescue InvalidDigest => e
                    raise InvalidIdentityMetadata, "value of field 'sha2' in #{metadata_path} does not look like a valid SHA2 digest: #{e.message}"
                end

                path = Pathname.new(path_info['path'].to_str)
                if path.each_filename.find { |p| p == '..' }
                    raise InvalidIdentityMetadata, "found path #{path} not within the dataset"
                end
                IdentityEntry.new(dataset_dir + path,
                                  Integer(path_info['size']),
                                  path_info['sha2'].to_str)
            end
            digests
        end

        # @api private
        #
        # Compute the encoded SHA2 digest of a file
        def compute_file_sha2(io)
            digest = Digest::SHA256.new
            while block = io.read(1024 * 1024)
                digest.update(block)
            end
            digest.send(DIGEST_ENCODING_METHOD)
        end

        # Compute a dataset digest based on the identity metadata
        #
        # It really only computes the data from the data in the metadata file,
        # but does not validate it against the data on-disk
        def compute_dataset_digest(dataset_identity = read_dataset_identity_from_metadata_file)
            dataset_digest_data = dataset_identity.map do |entry|
                path = entry.path.relative_path_from(dataset_dir).to_s
                [path, entry.size, entry.sha2]
            end
            dataset_digest_data = dataset_digest_data.
                sort_by { |path, _| path }.
                map do |path, size, sha2|
                    "#{sha2} #{size} #{path}"
                end.
                join("\n")
            Digest::SHA256.send(DIGEST_ENCODING_METHOD, dataset_digest_data)
        end

        # Enumerate the file's in a dataset that are considered 'important',
        # that is are part of the dataset's identity
        def each_important_file
            return enum_for(__method__) if !block_given?
            Pathname.glob(dataset_dir + "pocolog" + "*.*.log") do |path|
                yield(path)
            end
            Pathname.glob(dataset_dir + "*-events.log") do |path|
                yield(path)
            end
        end

        # Fully validate the dataset's identity metadata
        def validate_identity_metadata
            precomputed = read_dataset_identity_from_metadata_file.inject(Hash.new) do |h, entry|
                h.merge!(entry.path => entry)
            end
            actual      = compute_dataset_identity_from_files

            actual.each do |entry|
                if metadata_entry = precomputed.delete(entry.path)
                    if metadata_entry != entry
                        raise InvalidIdentityMetadata, "metadata mismatch between metadata file (#{metadata_entry.to_h}) and state on-disk (#{entry.to_h})"
                    end
                else
                    raise InvalidIdentityMetadata, "#{entry.path} is present on disk and missing in the metadata file"
                end
            end

            if !precomputed.empty?
                raise InvalidIdentityMetadata, "#{precomputed.size} files are listed in the dataset identity metadata, but are not present on disk: #{precomputed.keys.map(&:to_s).join(", ")}"
            end
        end

        # Fast validation of the dataset's identity information
        #
        # It does all the checks possible, short of actually recomputing the
        # file's digests
        def weak_validate_identity_metadata(dataset_identity = read_dataset_identity_from_metadata_file)
            # Verify the identity's format itself
            dataset_identity.each do |entry|
                Integer(entry.size)
                validate_encoded_sha2(entry.sha2)
            end

            important_files = each_important_file.inject(Hash.new) do |h, path|
                h.merge!(path => path.size)
            end

            dataset_identity.each do |entry|
                actual_size = important_files.delete(entry.path)
                if !actual_size
                    raise InvalidIdentityMetadata, "file #{entry.path} is listed in the identity metadata, but is not present on disk"
                elsif actual_size != entry.size
                    raise InvalidIdentityMetadata, "file #{entry.size} is listed in the identity metadata with a size of #{entry.size} bytes, but the file present on disk has a size of #{actual_size}"
                end
            end

            if !important_files.empty?
                raise InvalidIdentityMetadata, "#{important_files.size} important files are present on disk but are not listed in the identity metadata: #{important_files.keys.sort.join(", ")}"
            end
        end

        # Write the dataset's static metadata
        #
        # This is the metadata that is used to identify and verify the integrity
        # of the dataset
        def write_dataset_identity_to_metadata_file(dataset_identity = compute_dataset_identity_from_files)
            dataset_digest = compute_dataset_digest(dataset_identity)
            dataset_identity = dataset_identity.map do |entry|
                relative_path = entry.path.relative_path_from(dataset_dir)
                if relative_path.each_filename.find { |p| p == ".." }
                    raise InvalidIdentityMetadata, "found path #{entry.path} not within the dataset"
                end
                size = begin Integer(entry.size)
                       rescue ArgumentError => e
                           raise InvalidIdentityMetadata, "#{entry.size} is not a valid file size"
                       end
                if size < 0
                    raise InvalidIdentityMetadata, "#{entry.size} is not a valid file size"
                end
                sha2 = begin validate_encoded_sha2(entry.sha2)
                       rescue InvalidDigest
                           raise InvalidIdentityMetadata, "#{entry.sha2} is not a valid digest"
                       end

                Hash['path' => relative_path.to_s,
                     'sha2' => sha2,
                     'size' => size]
            end

            metadata = Hash[
                'layout_version' => LAYOUT_VERSION,
                'sha2' => dataset_digest,
                'identity' => dataset_identity
            ]
            (dataset_dir + BASENAME_IDENTITY_METADATA).open('w') do |io|
                YAML.dump metadata, io
            end
        end

        # Reset all metadata associated with this dataset
        def metadata_reset
            @metadata = Hash.new
        end

        # Add a new metadata value
        def metadata_add(key, *values)
            metadata.merge!(key => values.to_set) do |k, v1, v2|
                v1.merge(v2)
            end
        end

        # Get a single metadata value
        #
        # @param [String] key
        # @raise MultipleValues if there is more than one value associated with
        #   the key
        # @raise NoValue if there are no value associated with the key and the
        #   default value argument is not provided
        def metadata_fetch(key, *default_value)
            default_values = if default_value.empty?
                                 []
                             elsif default_value.size == 1
                                 [default_value]
                             else
                                 raise ArgumentError, "expected zero or one default value, got #{default_value.size}"
                             end

            value = metadata.fetch(key, *default_values)
            if value.size > 1
                raise MultipleValues, "multiple values found for #{key}. Use metadata_fetch_all"
            end
            value.first
        rescue KeyError
            raise NoValue, "no value found for key #{key}"
        end

        # Get all metadata values associated with a key
        #
        # @param [String] key
        # @raise NoValue if there are no value associated with the key and the
        #   default value argument is not provided
        #
        # @see
        def metadata_fetch_all(key, *default_values)
            default_values = if default_values.empty?
                                 []
                             else
                                 [default_values.to_set]
                             end
            metadata.fetch(key, *default_values)
        rescue KeyError
            raise NoValue, "no value found for key #{key}"
        end

        # Returns the dataset's metadata
        #
        # It is lazily loaded, i.e. loaded only the first time this method
        # is called
        def metadata
            return @metadata if @metadata

            path = (dataset_dir + BASENAME_METADATA)
            if path.exist?
                metadata_read_from_file
            else
                @metadata = Hash.new
            end
        end

        # Re-read the metadata from file, resetting the current metadata
        def metadata_read_from_file
            loaded = YAML.load((dataset_dir + BASENAME_METADATA).read)
            @metadata = loaded.inject(Hash.new) do |h, (k, v) |
                h.merge!(k => v.to_set)
            end
        end

        # Write this dataset's metadata to disk
        #
        # It is written in the root of the dataset, as {BASENAME_METADATA}
        def metadata_write_to_file
            dumped = metadata.inject(Hash.new) do |h, (k, v)|
                h.merge!(k => v.to_a)
            end
            (dataset_dir + BASENAME_METADATA).open('w') do |io|
                YAML.dump(dumped, io)
            end
        end
    end
end

