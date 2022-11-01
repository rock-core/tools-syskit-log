# frozen_string_literal: true

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
        #
        # @param (see #get)
        def find_dataset_from_short_digest(digest, **get_arguments)
            datasets = each_dataset_digest(redirects: true).find_all do |on_disk_digest|
                on_disk_digest.start_with?(digest)
            end
            if datasets.size > 1
                raise AmbiguousShortDigest,
                      "#{digest} is ambiguous, it matches #{datasets.join(", ")}"
            elsif !datasets.empty?
                get(datasets.first, **get_arguments)
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
        def each_dataset_digest(redirects: false)
            return enum_for(__method__, redirects: redirects) unless block_given?

            core_path = (datastore_path + "core")
            core_path.each_entry do |dataset_path|
                full_path = core_path + dataset_path
                valid_dataset =
                    Dataset.dataset?(full_path) ||
                    (redirects && self.class.redirect?(full_path))

                yield(dataset_path.to_s) if valid_dataset
            end
        end

        # Test whether the file at the given path is a redirect file
        def self.redirect?(path)
            return unless path.file?

            YAML.safe_load(path.read).key?("to")
        rescue Psych::SyntaxError # rubocop:disable Lint/SuppressedException
        end

        # Enumerate the store's datasets
        #
        # @param (see #get)
        # @yieldparam [Dataset] dataset
        def each_dataset(**get_arguments)
            return enum_for(__method__, **get_arguments) unless block_given?

            each_dataset_digest(redirects: false) do |digest|
                yield(get(digest, **get_arguments))
            end
        end

        # Remove an existing dataset
        def delete(digest)
            cache_path_of(digest).rmtree if cache_path_of(digest).exist?
            core_path_of(digest).rmtree
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
        #
        # @return [Array<Dataset>]
        def find_all(metadata, **get_arguments)
            metadata = metadata.transform_keys(&:to_s)
            metadata = metadata.transform_values { |v| Array(v).to_set }
            each_dataset(**get_arguments).find_all do |ds|
                metadata.all? do |key, values|
                    (values - (ds.metadata[key] || Set.new)).empty?
                end
            end
        end

        # Get an existing dataset
        #
        # @param [Symbol] validate validate the dataset information. If :weak
        #   (the default), it performs fast but limited checks. Set to :full
        #   for more extensive checks, and false to disable the checks altogether
        # @param [Boolean] preload_metadata load the metadata information
        # @return [Dataset]
        def get(digest, validate: :weak, preload_metadata: true)
            unless has?(digest)
                # Allow the user to give us a short digest here as well
                return from_short_digest(
                    digest, validate: validate, preload_metadata: preload_metadata
                )
            end

            digest = resolve_redirect(digest)

            dataset = Dataset.new(
                core_path_of(digest),
                digest: digest, cache: cache_path_of(digest)
            )
            if validate == :weak
                dataset.weak_validate_identity_metadata
            elsif validate == :full
                dataset.validate_identity_metadata
            elsif validate
                raise ArgumentError, "expected 'validate' to be either :weak or :full"
            end
            dataset.metadata if preload_metadata
            dataset
        end

        # @api private
        #
        # Resolve possible redirects from "old" digests to the new ones
        #
        # @param [String] digest the digest of the dataset we want resolved
        # @return [String] either a new digest if the original one was a redirect,
        #   or the 'digest' argument if it is a full dataset
        def resolve_redirect(digest)
            core_path = core_path_of(digest)
            return digest unless core_path.file?

            resolve_redirect(YAML.safe_load(core_path.read)["to"])
        end

        # @api private
        #
        # Write a redirect file from a valid digest string to another
        def write_redirect(redirected_digest, to:, **metadata)
            metadata = metadata.merge(to: to).transform_keys(&:to_s)
            core_path_of(redirected_digest).open("w") do |io|
                YAML.dump(metadata, io)
            end
        end

        # Resolve a dataset from a shortenest digest
        #
        # @param [String] digest
        # @param (see #get)
        # @return (see #get)
        def from_short_digest(digest, **get_arguments)
            unless (dataset = find_dataset_from_short_digest(digest, **get_arguments))
                raise ArgumentError, "no dataset with digest #{digest} exist"
            end

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

        # Yield to an operation that updates a dataset digest, and repair the
        # store afterwards
        def updating_digest(dataset)
            old_digest = dataset.digest
            old_identity_metadata_path = dataset.identity_metadata_path

            yield

            identity = dataset.read_dataset_identity_from_metadata_file(
                old_identity_metadata_path
            )
            new_digest = dataset.compute_dataset_digest(identity)

            return dataset if old_digest == new_digest

            FileUtils.mv core_path_of(old_digest), core_path_of(new_digest)
            if cache_path_of(old_digest).exist?
                FileUtils.mv cache_path_of(old_digest), cache_path_of(new_digest)
            end

            puts "#{old_digest}: dataset identity changed to #{new_digest}"
            get(new_digest, validate: false, preload_metadata: false)
        end
    end
end

require "syskit/log/datastore/dataset"
