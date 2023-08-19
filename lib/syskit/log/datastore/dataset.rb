# frozen_string_literal: true

module Syskit::Log
    class Datastore
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
            # @see write_dataset_identity_to_metadata_file
            BASENAME_IDENTITY_METADATA = "syskit-dataset.yml"

            # The basename of the file that contains identifying metadata
            #
            # @see write_dataset_identity_to_metadata_file
            BASENAME_METADATA = "syskit-metadata.yml"

            IdentityEntry = Struct.new :path, :size, :sha2

            # The path to the dataset
            #
            # @return [Pathname]
            attr_reader :dataset_path

            # The path to the index cache
            #
            # @return [Pathname]
            attr_reader :cache_path

            # If this dataset is part of a store, the digest in the store
            attr_reader :digest

            def initialize(path, digest: nil, cache: path)
                @digest = digest
                @dataset_path = path.realpath
                @cache_path = cache
                @metadata = nil
                @lazy_data_streams = nil
            end

            def timestamp
                begin
                    tv_sec = metadata_fetch("timestamp")
                rescue NoValue
                    tv_sec = compute_timestamp
                    metadata_set "timestamp", tv_sec
                end

                Time.at(tv_sec)
            end

            # @api private
            #
            # Compute a timestamp representative of this dataset
            def compute_timestamp
                roby_time = metadata_fetch_all("roby:time").min
                year = roby_time[0, 4]
                month = roby_time[4, 2]
                day = roby_time[6, 2]
                hh = roby_time[9, 2]
                mm = roby_time[11, 2]
                Time.utc(*[year, month, day, hh, mm].map { |v| Integer(v, 10) }).tv_sec
            rescue NoValue
                pocolog_timestamp =
                    each_pocolog_lazy_stream
                    .map { |stream| stream.interval_lg[0] }
                    .compact.min

                pocolog_timestamp&.tv_sec || 0
            end

            # Whether there is a dataset at this path
            def self.dataset?(path)
                (path + BASENAME_IDENTITY_METADATA).exist?
            end

            # @overload digest(string)
            #   Computes the digest of a string
            #
            # @overload digest
            #   Returns a Digest object that can be used to digest data
            def self.digest(string = nil)
                digest = Digest::SHA256.new
                digest.update(string) if string
                digest
            end

            # @overload string_digest(digest)
            #   Computes the string representation of a digest
            #
            # @overload string_digest(string)
            #   Computes the string representation of a string's digest
            def self.string_digest(object)
                object = digest(object) if object.respond_to?(:to_str)
                object.send(DIGEST_ENCODING_METHOD)
            end

            # Return the digest from the dataset's path
            #
            # @param [Pathname] path the dataset path
            # @raise [InvalidPath] if the path's dirname does not match a digest
            #   format
            def digest_from_path
                digest = dataset_path.basename.to_s
                begin
                    self.class.validate_encoded_sha2(digest)
                rescue InvalidDigest => e
                    raise InvalidPath,
                          "#{dataset_path}'s name does not look like a valid "\
                          "digest: #{e.message}"
                end
                digest
            end

            # Computes the dataset identity by reading the files
            #
            # The result is suitable to call e.g.
            # {#write_dataset_identity_to_metadata_file}
            #
            # @return [Array<IdentityEntry>]
            def compute_dataset_identity_from_files
                each_important_file.map do |path|
                    compute_file_identity(path)
                end
            end

            # @api private
            #
            # Compute the identity of a single file
            #
            # @param [Pathname] path
            # @return [IdentityEntry]
            def compute_file_identity(path)
                Syskit::Log.open_in_stream(path) do |io|
                    # Pocolog files do not hash their prologue
                    if path.dirname.basename.to_s == "pocolog"
                        io.seek(Pocolog::Format::Current::PROLOGUE_SIZE)
                    end
                    sha2 = compute_file_sha2(io)

                    IdentityEntry.new(path, io.tell, sha2)
                end
            end

            def self.validate_encoded_short_digest(digest)
                if digest.length > ENCODED_DIGEST_LENGTH
                    raise InvalidDigest,
                          "#{digest} does not look like a valid SHA2 short digest "\
                          "encoded with #{DIGEST_ENCODING_METHOD}. Expected at most "\
                          "#{ENCODED_DIGEST_LENGTH} characters but got #{digest.length}"
                elsif digest !~ /^[0-9a-f]+$/
                    raise InvalidDigest,
                          "#{digest} does not look like a valid SHA2 digest encoded "\
                          "with #{DIGEST_ENCODING_METHOD}. "\
                          "Expected characters in 0-9a-zA-Z+"
                end
                digest
            end

            # Validate that the given digest is a valid dataset ID
            #
            # See {valid_encoded_digest?} to for a true/false check
            #
            # @param [String]
            # @raise [InvalidDigest]
            def self.validate_encoded_digest(digest)
                validate_encoded_sha2(digest)
            end

            # @api private
            #
            # Implementation of {.validate_encoded_digest} for SHA2 hashes
            def self.validate_encoded_sha2(sha2)
                if sha2.length != ENCODED_DIGEST_LENGTH
                    raise InvalidDigest,
                          "#{sha2} does not look like a valid SHA2 digest encoded "\
                          "with #{DIGEST_ENCODING_METHOD}. Expected "\
                          "#{ENCODED_DIGEST_LENGTH} characters but got #{sha2.length}"
                elsif sha2 !~ /^[0-9a-f]+$/
                    raise InvalidDigest,
                          "#{sha2} does not look like a valid SHA2 digest encoded "\
                          "with #{DIGEST_ENCODING_METHOD}. "\
                          "Expected characters in 0-9a-zA-Z+/"
                end
                sha2
            end

            # Checks if the given digest is a valid dataset ID
            #
            # @see validate_encoded_digest
            def self.valid_encoded_digest?(digest)
                valid_encoded_sha2?(digest)
            end

            # @api private
            #
            # Implementation of {.valid_encoded_digest?} for SHA2 hashes
            def self.valid_encoded_sha2?(sha2)
                sha2.length == ENCODED_DIGEST_LENGTH &&
                    /^[0-9a-f]+$/.match?(sha2)
            end

            # Return the path to the file containing identity metadata
            def identity_metadata_path
                dataset_path + BASENAME_IDENTITY_METADATA
            end

            # Load the dataset identity information from the metadata file
            #
            # It does sanity checks on the loaded data, but does not compare it
            # against the actual data on disk
            #
            # @return [Array<IdentityEntry>]
            def read_dataset_identity_from_metadata_file(
                metadata_path = identity_metadata_path
            )
                identity_metadata = (YAML.safe_load(metadata_path.read) || {})
                if identity_metadata["layout_version"] != LAYOUT_VERSION
                    raise InvalidLayoutVersion,
                          "layout version in #{dataset_path} is "\
                          "#{identity_metadata['layout_version']}, "\
                          "expected #{LAYOUT_VERSION}"
                end
                digests = identity_metadata["identity"]
                if !digests
                    raise InvalidIdentityMetadata,
                          "no 'identity' field in #{metadata_path}"
                elsif !digests.respond_to?(:to_ary)
                    raise InvalidIdentityMetadata,
                          "the 'identity' field in #{metadata_path} is not an array"
                end
                digests = digests.map do |path_info|
                    if !path_info["path"].respond_to?(:to_str)
                        raise InvalidIdentityMetadata,
                              "found non-string value for field 'path' "\
                              "in #{metadata_path}"
                    elsif !path_info["size"].kind_of?(Integer)
                        raise InvalidIdentityMetadata,
                              "found non-integral value for field 'size' "\
                              "in #{metadata_path}"
                    elsif !path_info["sha2"].respond_to?(:to_str)
                        raise InvalidIdentityMetadata,
                              "found non-string value for field 'sha2' "\
                              "in #{metadata_path}"
                    end

                    begin
                        self.class.validate_encoded_sha2(path_info["sha2"])
                    rescue InvalidDigest => e
                        raise InvalidIdentityMetadata,
                              "value of field 'sha2' in #{metadata_path} does "\
                              "not look like a valid SHA2 digest: #{e.message}"
                    end

                    path = Pathname.new(path_info["path"].to_str)
                    if path.each_filename.find { |p| p == ".." }
                        raise InvalidIdentityMetadata,
                              "found path #{path} not within the dataset"
                    end
                    IdentityEntry.new(
                        dataset_path + path, Integer(path_info["size"]),
                        path_info["sha2"].to_str
                    )
                end
                digests
            end

            # @api private
            #
            # Compute the encoded SHA2 digest of a file
            def compute_file_sha2(io)
                digest = Dataset.digest
                while (block = io.read(1024 * 1024))
                    digest.update(block)
                end
                Dataset.string_digest(digest)
            end

            # Compute a dataset digest based on the identity metadata
            #
            # It really only computes the data from the data in the metadata file,
            # but does not validate it against the data on-disk
            def compute_dataset_digest(
                dataset_identity = read_dataset_identity_from_metadata_file
            )
                dataset_digest_data = dataset_identity.map do |entry|
                    path = entry.path.relative_path_from(dataset_path).to_s
                    [path, entry.size, entry.sha2]
                end
                dataset_digest_data =
                    dataset_digest_data
                    .sort_by { |path, _| path }
                    .map { |path, size, sha2| "#{sha2} #{size} #{path}" }
                    .join('\n')
                Dataset.string_digest(dataset_digest_data)
            end

            # Enumerate the file's in a dataset that are considered 'important',
            # that is are part of the dataset's identity
            def each_important_file
                return enum_for(__method__) unless block_given?

                Pathname.glob(dataset_path + "pocolog" + "*.*.log") do |path|
                    yield(path)
                end

                Pathname.glob(dataset_path + "roby-events.*.log") do |path|
                    yield(path)
                end
            end

            # Fully validate the dataset's identity metadata
            def validate_identity_metadata
                precomputed = read_dataset_identity_from_metadata_file
                              .inject({}) { |h, entry| h.merge!(entry.path => entry) }
                actual = compute_dataset_identity_from_files

                actual.each do |entry|
                    unless (metadata_entry = precomputed.delete(entry.path))
                        raise InvalidIdentityMetadata,
                              "#{entry.path} is present on disk and "\
                              "missing in the metadata file"
                    end

                    if metadata_entry != entry
                        raise InvalidIdentityMetadata,
                              "metadata mismatch between metadata file "\
                              "(#{metadata_entry.to_h}) and state on-disk "\
                              "(#{entry.to_h})"
                    end
                end

                return if precomputed.empty?

                raise InvalidIdentityMetadata,
                      "#{precomputed.size} files are listed in the dataset "\
                      "identity metadata, but are not present on disk: "\
                      "#{precomputed.keys.map(&:to_s).join(', ')}"
            end

            # Fast validation of the dataset's identity information
            #
            # It does all the checks possible, short of actually recomputing the
            # file's digests
            def weak_validate_identity_metadata(
                dataset_identity = read_dataset_identity_from_metadata_file
            )
                # Verify the identity's format itself
                dataset_identity.each do |entry|
                    Integer(entry.size)
                    self.class.validate_encoded_sha2(entry.sha2)
                end

                important_files = each_important_file.to_set
                dataset_identity.each do |entry|
                    next if important_files.delete?(entry.path)

                    raise InvalidIdentityMetadata,
                          "file #{entry.path} is listed in the identity "\
                          "metadata, but is not present on disk. Try "\
                          "`syskit ds repair`"
                end

                return if important_files.empty?

                raise InvalidIdentityMetadata,
                      "#{important_files.size} important files are present on disk "\
                      "but are not listed in the identity metadata: "\
                      "#{important_files.to_a.sort.join(', ')}"
            end

            # Write the dataset's static metadata
            #
            # This is the metadata that is used to identify and verify the integrity
            # of the dataset
            def write_dataset_identity_to_metadata_file(
                dataset_identity = compute_dataset_identity_from_files
            )
                dataset_digest = compute_dataset_digest(dataset_identity)
                @digest = dataset_digest
                metadata_set "digest", dataset_digest

                dataset_identity = dataset_identity.map do |entry|
                    relative_path = entry.path.relative_path_from(dataset_path)
                    if relative_path.each_filename.find { |p| p == ".." }
                        raise InvalidIdentityMetadata,
                              "found path #{entry.path} not within the dataset"
                    end
                    size = begin Integer(entry.size)
                           rescue ArgumentError
                               raise InvalidIdentityMetadata,
                                     "#{entry.size} is not a valid file size"
                           end
                    if size < 0
                        raise InvalidIdentityMetadata,
                              "#{entry.size} is not a valid file size"
                    end
                    sha2 = begin self.class.validate_encoded_sha2(entry.sha2)
                           rescue InvalidDigest
                               raise InvalidIdentityMetadata,
                                     "#{entry.sha2} is not a valid digest"
                           end

                    Hash["path" => relative_path.to_s,
                         "sha2" => sha2,
                         "size" => size]
                end

                metadata = Hash[
                    "layout_version" => LAYOUT_VERSION,
                    "sha2" => dataset_digest,
                    "identity" => dataset_identity
                ]
                (dataset_path + BASENAME_IDENTITY_METADATA).open("w") do |io|
                    YAML.dump metadata, io
                end
                @digest = dataset_digest
            end

            # Reset all metadata associated with this dataset
            def metadata_reset
                @metadata = {}
            end

            # Delete a metadata entry
            def metadata_delete(key)
                metadata.delete(key)
            end

            # Resets a metadata value
            def metadata_set(key, *values)
                metadata[key] = Set[*values]
            end

            # Add a new metadata value
            def metadata_add(key, *values)
                metadata.merge!(key => values.to_set) do |_, v1, v2|
                    v1.merge(v2)
                end
            end

            # Whether there is a given metadata entry
            def metadata?(key)
                metadata.key?(key)
            end

            # Return a metadata entry
            #
            # @return [Set,nil] the entry contents, or nil if there is no such entry
            def metadata_get(key)
                metadata[key]
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
                                     raise ArgumentError,
                                           "expected zero or one default value, "\
                                           "got #{default_value.size}"
                                 end

                value = metadata.fetch(key, *default_values)
                if value.size > 1
                    raise MultipleValues,
                          "multiple values found for #{key}. Use metadata_fetch_all"
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

            # Path to the (optional) metadata file
            #
            # @return [Pathname]
            def metadata_path
                dataset_path + BASENAME_METADATA
            end

            # Returns the dataset's metadata
            #
            # It is lazily loaded, i.e. loaded only the first time this method
            # is called
            def metadata
                return @metadata if @metadata

                if metadata_path.exist?
                    metadata_read_from_file
                else
                    @metadata = {}
                end
            end

            # Re-read the metadata from file, resetting the current metadata
            def metadata_read_from_file
                loaded = YAML.safe_load(metadata_path.read)
                @metadata = loaded.inject({}) do |h, (k, v)|
                    h.merge!(k => v.to_set)
                end
                @metadata.merge!("digest" => Set[digest]) if digest
                @metadata
            end

            # Write this dataset's metadata to disk
            #
            # It is written in the root of the dataset, as {BASENAME_METADATA}
            def metadata_write_to_file
                dumped = metadata.inject({}) do |h, (k, v)|
                    h.merge!(k => v.to_a)
                end
                metadata_path.open("w") { |io| YAML.dump(dumped, io) }
            end

            def pocolog_path(name)
                path = dataset_path + "pocolog" + "#{name}.0.log"
                unless path.exist?
                    raise ArgumentError,
                          "no pocolog file for stream #{name} (expected #{path})"
                end

                path
            end

            def each_pocolog_path
                return enum_for(__method__) unless block_given?

                Pathname.glob(dataset_path + "pocolog" + "*.log") do |logfile_path|
                    yield(logfile_path)
                end
            end

            # Enumerate the pocolog streams available in this dataset
            #
            # @yieldparam [Pocolog::Datastream] stream
            # @see each_pocolog_lazy_stream
            def each_pocolog_stream
                return enum_for(__method__) unless block_given?

                pocolog_index_dir = (cache_path + "pocolog").to_s
                each_pocolog_path do |logfile_path|
                    logfile = Pocolog::Logfiles.open(
                        logfile_path, index_dir: pocolog_index_dir, silent: true
                    )
                    yield(logfile.streams.first)
                end
            end

            # @api private
            #
            # Load lazy data stream information from disk
            def read_lazy_data_streams
                pocolog_index_dir = (cache_path + "pocolog").to_s
                Pathname.enum_for(:glob, dataset_path + "pocolog" + "*.log").map do |logfile_path|
                    index_path = Pocolog::Logfiles.default_index_filename(
                        logfile_path.to_s, index_dir: pocolog_index_dir.to_s
                    )
                    index_path = Pathname.new(index_path)
                    logfile_path.open do |file_io|
                        index_path.open do |index_io|
                            stream_info =
                                Pocolog::Format::Current
                                .read_minimal_info(index_io, file_io)
                            stream_block, index_stream_info = stream_info.first

                            interval_rt = index_stream_info.interval_rt.map do |t|
                                Pocolog::StreamIndex.time_from_internal(t, 0)
                            end
                            interval_lg = index_stream_info.interval_lg.map do |t|
                                Pocolog::StreamIndex.time_from_internal(t, 0)
                            end

                            LazyDataStream.new(
                                logfile_path,
                                pocolog_index_dir,
                                stream_block.name,
                                stream_block.type,
                                stream_block.metadata,
                                interval_rt,
                                interval_lg,
                                index_stream_info.stream_size
                            )
                        end
                    end
                end
            end

            # Whether there is actually something in this dataset
            def empty?
                interval_lg.empty?
            end

            # Return the logical time interval of the whole dataset
            #
            # That is, the min and max time of all streams in the set
            #
            # @return [(Time,Time)]
            def interval_lg
                return @interval_lg if @interval_lg

                streams = each_pocolog_lazy_stream
                          .map(&:interval_lg)
                          .reject(&:empty?)

                @interval_lg =
                    if streams.empty?
                        []
                    else
                        interval_start, interval_end = streams.transpose
                        [interval_start.min, interval_end.max]
                    end
            end

            # Enumerate the pocolog streams available in this dataset, without
            # loading them
            #
            # It relies on an index built by the datastore
            #
            # @yieldparam [LazyDataStream] stream
            # @see each_pocolog_stream
            def each_pocolog_lazy_stream(&block)
                return enum_for(__method__) unless block_given?

                (@lazy_data_streams ||= read_lazy_data_streams).each(&block)
            end

            # Enumerate the paths to the Roby logs available in this dataset
            #
            # @yieldparam [Pathname] path to the roby log
            def each_roby_log_path(&block)
                Syskit::Log.logfiles_in_dir(dataset_path).each(&block)
            end

            # Path to Roby's own index file for the given roby log
            #
            # @param [Pathname] roby_log_path
            def roby_index_path(roby_log_path)
                cache_path + roby_log_path.basename.sub_ext(".idx")
            end

            # Return an object that allows to query the set of data streams in
            # this dataset
            #
            # @return [Streams]
            def streams
                Streams.new(each_pocolog_lazy_stream.to_a)
            end

            # Access to the Roby log information, such as tasks or events
            #
            # @return [RobySQLIndex::Accessors::Root]
            def roby
                RobySQLIndex::Accessors::Root.new(roby_sql_index)
            end

            # Path to the Roby SQL index
            #
            # @return [Pathname]
            def roby_sql_index_path
                cache_path + "roby.sql"
            end

            # The Roby SQL index
            #
            # @return [RobySQLIndex]
            def roby_sql_index
                RobySQLIndex::Index.open(roby_sql_index_path, dataset: self)
            end

            # Enumerate the streams per task
            #
            # @yieldparam [TaskStreams]
            #
            # @param (see Streams#each_task)
            def each_task(
                load_models: false,
                skip_tasks_without_models: false,
                raise_on_missing_task_models: false,
                loader: Roby.app.default_loader, &block
            )
                streams.each_task(
                    load_models: load_models,
                    skip_tasks_without_models: skip_tasks_without_models,
                    raise_on_missing_task_models: raise_on_missing_task_models,
                    loader: loader,
                    &block
                )
            end
        end
    end
end
