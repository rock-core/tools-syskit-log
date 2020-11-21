module Syskit::Log
    # Functionality related to building and using data stores
    #
    # Note that requiring syskit/log only loads the 'using datastores' APIs.
    # You need to require the functionality specific files in
    # syskit/log/datastore
    #
    # A store for normalized datasets
    class Datastore
        extend Logger::Hierarchy

        # The store's path on disk
        #
        # @return [Pathname]
        attr_reader :datastore_path

        def initialize(datastore_path)
            @datastore_path = datastore_path.realpath
        end

        # Whether there is a default datastore defined
        #
        # The default datastore is defined through the SYSKIT_LOG_STORE
        # environment variable
        def self.default_defined?
            ENV["SYSKIT_LOG_STORE"]
        end

        # The default datastore
        #
        # The default datastore is defined through the SYSKIT_LOG_STORE
        # environment variable. This raises if the environment variable is
        # not defined
        def self.default
            raise ArgumentError, "SYSKIT_LOG_STORE is not set" unless default_defined?

            new(Pathname(ENV["SYSKIT_LOG_STORE"]))
        end

        # Setup a directory structure for the given path to be a valid datastore
        def self.create(datastore_path)
            datastore_path.mkpath
            (datastore_path + "core").mkpath
            (datastore_path + "cache").mkpath
            (datastore_path + "incoming").mkpath
            Datastore.new(datastore_path)
        end

        class AmbiguousShortDigest < ArgumentError; end

        # Finds the dataset that matches the given shortened digest
        def find_dataset_from_short_digest(digest)
            datasets = each_dataset_digest.find_all do |on_disk_digest|
                on_disk_digest.start_with?(digest)
            end
            if datasets.size > 1
                raise AmbiguousShortDigest, "#{digest} is ambiguous, it matches #{datasets.join(", ")}"
            elsif !datasets.empty?
                get(datasets.first)
            end
        end

        # Returns the short digest for the given dataset, or the full digest if
        # shortening creates a collision
        def short_digest(dataset, size: 10)
            short = dataset.digest[0, size]
            begin
                find_dataset_from_short_digest(short)
                short
            rescue AmbiguousShortDigest
                dataset.digest
            end
        end

        # Whether a dataset with the given ID exists
        def has?(digest)
            core_path_of(digest).exist?
        end

        # Enumerate the store's datasets
        def each_dataset_digest
            return enum_for(__method__) if !block_given?
            core_path = (datastore_path + "core")
            core_path.each_entry do |dataset_path|
                if Dataset.dataset?(core_path + dataset_path)
                    yield(dataset_path.to_s)
                end
            end
        end

        # Enumerate the store's datasets
        def each_dataset
            return enum_for(__method__) if !block_given?
            each_dataset_digest do |digest|
                yield(get(digest))
            end
        end

        # Remove an existing dataset
        def delete(digest)
            core_path_of(digest).rmtree
            if cache_path_of(digest).exist?
                cache_path_of(digest).rmtree
            end
        end

        # The full path to a dataset
        #
        # The dataset itself is not guaranteed to exist
        def core_path_of(digest)
            datastore_path + "core" + digest
        end

        # The full path to a dataset
        #
        # The dataset itself is not guaranteed to exist
        def cache_path_of(digest)
            datastore_path + "cache" + digest
        end

        # Enumerate the datasets matching this query
        def find(metadata)
            matches = find_all(metadata)
            if matches.size > 1
                raise ArgumentError,
                      "more than one matching dataset, use #find_all instead"
            else
                matches.first
            end
        end

        # Enumerate the datasets matching this query
        def find_all(metadata)
            each_dataset.find_all do |ds|
                metadata.all? do |key, values|
                    values = Array(values).to_set
                    (values - (ds.metadata[key] || Set.new)).empty?
                end
            end
        end

        # Get an existing dataset
        def get(digest)
            unless has?(digest)
                # Try to see if digest is a short digest
                if (dataset = find_dataset_from_short_digest(digest))
                    return dataset
                end

                raise ArgumentError,
                      "no dataset with digest #{digest} exist"
            end

            dataset = Dataset.new(
                core_path_of(digest),
                digest: digest, cache: cache_path_of(digest)
            )
            dataset.weak_validate_identity_metadata
            dataset.metadata
            dataset
        end

        # @api private
        #
        # Create a working directory in the incoming dir of the data store and
        # yield
        #
        # The created dir is deleted if it still exists after the block
        # returned. This ensures that no incoming leftovers are kept if an
        # opeartion fails
        def in_incoming(keep: false)
            incoming_dir = (datastore_path + "incoming")
            incoming_dir.mkpath

            i = 0
            begin
                while (import_dir = (incoming_dir + i.to_s)).exist?
                    i += 1
                end
                import_dir.mkdir
            rescue Errno::EEXIST
                i += 1
                retry
            end

            begin
                core_path = import_dir + "core"
                cache_path = import_dir + "cache"
                core_path.mkdir
                cache_path.mkdir
                yield(import_dir + "core", import_dir + "cache")
            ensure
                if !keep && import_dir.exist?
                    import_dir.rmtree
                end
            end
        end

    end
end

require "syskit/log/datastore/dataset"
