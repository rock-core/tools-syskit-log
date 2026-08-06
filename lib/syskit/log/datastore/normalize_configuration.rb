# frozen_string_literal: true

module Syskit
    module Log
        # Object holding the configuration data for the normalization proces
        class NormalizeConfiguration
            def initialize
                @stream_config_per_type = {}
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
                JSON::Validator.validate!(@schema, config, insert_defaults: true)

                config.dig("streams").each do |entry|
                    if (typename = entry.dig("match", "type"))
                        @stream_config_per_type[typename] = StreamConfig.new(
                            lg_allow_duplicates: entry["lg_allow_duplicates"],
                            rt_allow_duplicates: entry["rt_allow_duplicates"],
                            metadata_ops: [entry["metadata"]]
                        )
                    end
                end
            end

            # Return a stream configuration object for the given type
            #
            # @return StreamConfig
            def stream_config_for_type(type_name)
                @stream_config_per_type[type_name] ||= StreamConfig.new
            end

            # Encapsulation of metadata update operations
            class StreamConfig
                def initialize(
                    lg_allow_duplicates: true, rt_allow_duplicates: true,
                    metadata_ops: []
                )
                    @metadata_ops = metadata_ops.dup
                    @lg_allow_duplicates = lg_allow_duplicates
                    @rt_allow_duplicates = rt_allow_duplicates
                end

                # Update {#lg_allow_duplicates?}
                attr_writer :lg_allow_duplicates

                # Whether the importer should reject samples that have the same logical
                # time than the previous sample
                #
                # The default is to accept such samples, as it can happen "naturally"
                # with Rock's logger
                #
                # @see lg_allow_duplicates= rt_allow_duplicates?
                def lg_allow_duplicates?
                    @lg_allow_duplicates
                end

                # Update {#lg_allow_duplicates?}
                attr_writer :rt_allow_duplicates

                # Whether the importer should reject samples that have the same real
                # time than the previous sample
                #
                # The default is to accept such samples, as it can happen "naturally"
                # with Rock's logger
                #
                # @see rt_allow_duplicates= lg_allow_duplicates?
                def rt_allow_duplicates?
                    @rt_allow_duplicates
                end

                def metadata_add_op(update)
                    @metadata_ops << update
                end

                def metadata_update(metadata)
                    return metadata if @metadata_ops.empty?

                    metadata = metadata.dup
                    @metadata_ops.each do |spec|
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
