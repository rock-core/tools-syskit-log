# frozen_string_literal: true

module Syskit::Log
    class Datastore
        module Repair # rubocop:disable Style/Documentation
            # Detect problems and perform repair steps on the given dataset
            def self.repair_dataset(
                datastore, dataset, dry_run: true, reporter: NullReporter.new
            )
                if dry_run
                    ops = find_all_repair_ops(datastore, dataset)
                    if ops.empty?
                        puts "Nothing to do"
                    else
                        ops.each { |op| puts op }
                    end
                    dataset
                else
                    loop do
                        return dataset unless (op = find_repair_op(datastore, dataset))

                        puts op
                        (dataset = op.apply(reporter)) unless dry_run
                    end
                end
            end

            # @api private
            #
            # Return the operations that should be applied to this dataset
            def self.find_all_repair_ops(datastore, dataset)
                OPERATIONS.map { |op_class| op_class.detect(datastore, dataset) }
                          .compact
            end

            # @api private
            #
            # Return the first operation that should be applied to this dataset
            def self.find_repair_op(datastore, dataset)
                OPERATIONS.each do |op_class|
                    if (op = op_class.detect(datastore, dataset))
                        return op
                    end
                end
                nil
            end

            # @api private
            #
            # Migration of roby-events.log to roby-events.0.log
            class MigrateRobyLogName
                def self.detect(datastore, dataset)
                    identity = dataset.read_dataset_identity_from_metadata_file
                    if identity.none? { |e| e.path.basename.to_s == "roby-events.log" }
                        return
                    end

                    new(datastore, dataset)
                end

                def initialize(datastore, dataset)
                    @datastore = datastore
                    @dataset = dataset
                end

                def to_s
                    "#{@dataset.digest}: rename roby-events.log to roby-events.0.log, "\
                    "changing the dataset identity"
                end

                def apply(_reporter)
                    @datastore.updating_digest(@dataset) do
                        identity = @dataset.read_dataset_identity_from_metadata_file
                        roby_log = identity.find do |e|
                            e.path.basename.to_s == "roby-events.log"
                        end

                        renamed = (roby_log.path.dirname + "roby-events.0.log")
                        FileUtils.mv roby_log.path, renamed
                        roby_log.path = renamed
                        @dataset.write_dataset_identity_to_metadata_file(identity)
                    end
                end
            end

            # @api private
            #
            # roby-events.?.log were at some point not added to the identity metadata
            class AddRobyLogsToIdentity
                def self.detect(datastore, dataset)
                    paths = roby_log_paths(dataset)
                    return if paths.empty?

                    identity = dataset.read_dataset_identity_from_metadata_file
                    return if paths.none? { |p| identity.none? { |e| e.path == p } }

                    new(datastore, dataset, paths)
                end

                def initialize(datastore, dataset, missing_paths)
                    @datastore = datastore
                    @dataset = dataset
                    @missing_paths = missing_paths
                end

                def self.roby_log_paths(dataset)
                    dataset.each_important_file.find_all do |p|
                        p.basename.to_s =~ /^roby-events\.\d+\.log$/
                    end
                end

                def to_s
                    missing_paths = self.class.roby_log_paths(@dataset)

                    "#{@dataset.digest}: add #{missing_paths.size} missing roby "\
                    "event logs to the dataset identity, changing the digest\n"\
                    "  #{missing_paths.map(&:to_s).join("\n  ")}"
                end

                def apply(_reporter)
                    @datastore.updating_digest(@dataset) do
                        missing_paths = self.class.roby_log_paths(@dataset)
                        identity = @dataset.read_dataset_identity_from_metadata_file
                        identity += missing_paths.map do |p|
                            @dataset.compute_file_identity(p)
                        end
                        @dataset.write_dataset_identity_to_metadata_file(identity)
                    end
                end
            end

            # @api private
            #
            # Builds the cache folder for the given dataset
            class CacheRebuild
                def self.detect(datastore, dataset)
                    return if datastore.cache_path_of(dataset.digest).exist?

                    new(datastore, dataset)
                end

                def initialize(datastore, dataset)
                    @datastore = datastore
                    @dataset = dataset
                end

                def to_s
                    "#{@dataset.digest}: rebuild cache"
                end

                def apply(reporter)
                    Syskit::Log::Datastore.index_build(
                        @datastore, @dataset, reporter: reporter
                    )
                    @dataset
                end
            end

            # @api private
            #
            # Calculate the timestamp and save it in the dataset
            class ComputeTimestamp
                def self.detect(_datastore, dataset)
                    new(dataset) unless dataset.metadata["timestamp"]
                end

                def initialize(dataset)
                    @dataset = dataset
                end

                def to_s
                    "#{@dataset.digest}: save the timestamp metadata "\
                    "#{@dataset.timestamp}"
                end

                def apply(_reporter)
                    # This computes & saves the timestamp in the metadata
                    @dataset.timestamp
                    @dataset.metadata_write_to_file
                    @dataset
                end
            end

            OPERATIONS = [
                MigrateRobyLogName,
                AddRobyLogsToIdentity,

                # Must be after everything that repairs the identity
                CacheRebuild,
                ComputeTimestamp
            ].freeze
        end
    end
end
