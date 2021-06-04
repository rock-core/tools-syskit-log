module Syskit::Log
    class Datastore
        module Repair # rubocop:disable Style/Documentation
            # Detect problems and perform repair steps on the given dataset
            def self.repair_dataset(datastore, dataset, dry_run: true)
                if dry_run
                    ops = find_all_repair_ops(datastore, dataset)
                    if ops.empty?
                        puts "Nothing to do"
                    else
                        ops.each { |op| puts op }
                    end
                else
                    loop do
                        return unless (op = find_repair_op(datastore, dataset))

                        puts op
                        (dataset = op.apply) unless dry_run
                    end
                end
            end

            def self.find_all_repair_ops(datastore, dataset)
                OPERATIONS.map { |op_class| op_class.detect(datastore, dataset) }
                          .compact
            end

            def self.find_repair_op(datastore, dataset)
                OPERATIONS.each do |op_class|
                    if (op = op_class.detect(datastore, dataset))
                        return op
                    end
                end
                nil
            end

            # Calculate the timestamp and save it in the dataset
            class ComputeTimestamp
                def self.detect(_datastore, dataset)
                    new(dataset) unless dataset.metadata["timestamp"]
                end

                def initialize(dataset)
                    @dataset = dataset
                end

                def to_s
                    "#{@dataset.digest}: save the timestamp metadata #{@dataset.timestamp}"
                end

                def apply
                    # This computes & saves the timestamp in the metadata
                    @dataset.timestamp
                    @dataset.metadata_write_to_file
                end
            end

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
                    "rename roby-events.log to roby-events.0.log, "\
                    "changing the dataset identity"
                end

                def apply
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
                    missing_paths =
                        self.class.roby_log_paths(@dataset).map(&:to_s).join("\n  ")

                    "#{@dataset.digest}: add #{missing_paths.size} missing roby "\
                    "event logs to the dataset identity, changing the digest\n"\
                    "  #{missing_paths}"
                end

                def apply
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

            OPERATIONS = [
                ComputeTimestamp,
                MigrateRobyLogName,
                AddRobyLogsToIdentity
            ].freeze
        end
    end
end
