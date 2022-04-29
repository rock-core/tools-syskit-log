# frozen_string_literal: true

module Syskit
    module Log
        module RobySQLIndex
            EVENT_PROPAGATION_CALL = 0
            EVENT_PROPAGATION_EMIT = 1
            EVENT_PROPAGATION_EMIT_FAILED = 2

            # Namespace for the definition of the SQL schema and relations
            module Definitions
                # @api private
                #
                # Create the schema on a ROM database configuration
                def self.schema(config) # rubocop:disable Metrics/AbcSize
                    config.default.create_table :metadata do
                        primary_key :id
                        column :name, String, null: false
                        column :cycle_count, Integer, null: false
                        column :time_start, Time, null: false
                        column :time_end, Time, null: false
                    end

                    config.default.create_table :models do
                        primary_key :id
                        column :name, String, null: false
                    end

                    config.default.create_table :tasks do
                        primary_key :id
                        foreign_key :model_id, :models, null: false

                        column :arguments, String, null: false
                    end

                    config.default.create_table :event_propagations do
                        primary_key :id
                        foreign_key :task_id, :tasks, null: false
                        index :task_id

                        column :time, Time, null: false
                        index :time
                        column :name, String, null: false
                        index :name
                        column :kind, Integer, null: false
                        index :kind

                        column :context, String
                    end
                end

                def self.configure(config)
                    Sequel.application_timezone = :local
                    Sequel.database_timezone = :utc
                    config.register_relation(Models, Tasks, EventPropagations, Metadata)
                end

                # Representation of metadata about the whole log
                class Metadata < ROM::Relation[:sql]
                    schema(:metadata, infer: true)

                    struct_namespace Entities
                    auto_struct true
                end

                # Representation of a Roby model
                class Models < ROM::Relation[:sql]
                    schema(:models, infer: true) do
                        associations do
                            has_many :tasks
                        end
                    end

                    struct_namespace Entities
                    auto_struct true

                    def by_name(name)
                        where(name: name)
                    end
                end

                # Representation of a Roby task instance
                class Tasks < ROM::Relation[:sql]
                    schema(:tasks, infer: true) do
                        associations do
                            belongs_to :model
                            has_many :event_propagations
                        end
                    end

                    struct_namespace Entities
                    auto_struct true

                    # Returns the list of event propagation related to this task
                    def history_of(task)
                        where(id: task.id).left_join(:event_propagations).to_a
                    end
                end

                # Representation of a Roby emitted event
                class EventPropagations < ROM::Relation[:sql]
                    schema(:event_propagations, infer: true) do
                        associations do
                            belongs_to :task
                        end
                    end

                    struct_namespace Entities
                    auto_struct true

                    def calls
                        where(kind: EVENT_PROPAGATION_CALL)
                    end

                    def emissions
                        where(kind: EVENT_PROPAGATION_EMIT)
                    end

                    def failed_emissions
                        where(kind: EVENT_PROPAGATION_EMIT_FAILED)
                    end

                    def by_name(name)
                        where(name: name.to_s)
                    end

                    def from_task_id(task_id)
                        where(task_id: task_id)
                    end
                end
            end
        end
    end
end
