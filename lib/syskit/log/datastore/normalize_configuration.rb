# frozen_string_literal: true

module Syskit
    module Log
        # Object holding the configuration data for the normalization proces
        class NormalizeConfiguration
            def initialize
                @stream_metadata_per_type = {}
                @schema = File.join(__dir__, "normalize_configuration.schema.json")
            end

            def self.from_file(path)
                from_hash(YAML.safe_load(path))
            end

            def self.from_hash(config)
                obj = new
                obj.update_from_hash(config)
                obj
            end

            # Add configuration from hash (usually from a YAML file)
            def update_from_hash(config)
                JSON::Validator.validate!(@schema, config)

                config.dig("metadata", "streams")&.each do |entry|
                    if (typename = entry.dig("match", "type"))
                        @stream_metadata_per_type[typename] = MetadataUpdate.new([entry])
                    end
                end
            end

            # Return a metadata update object for the given type
            #
            # @return MetadataUpdate
            def stream_metadata_update_for_type(type_name)
                @stream_metadata_per_type[type_name] || MetadataUpdate.identity
            end

            # Encapsulation of metadata update operations
            class MetadataUpdate
                def initialize(ops = [])
                    @ops = ops.dup
                end

                def add(update)
                    @ops << update
                end

                def self.identity
                    MetadataUpdate.new
                end

                def update(metadata)
                    return metadata if @ops.empty?

                    metadata = metadata.dup
                    @ops.each do |spec|
                        op, k, v = spec.values_at("op", "key", "value")
                        case op
                        when "set"
                            metadata[k] = v
                        when "delete"
                            metadata.delete(k)
                        end
                    end
                    metadata
                end
            end
        end
    end
end
