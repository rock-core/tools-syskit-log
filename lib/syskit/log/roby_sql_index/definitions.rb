# frozen_string_literal: true

module Syskit
    module Log
        module RobySQLIndex
            # Namespace for the definition of the SQL schema and relations
            module Definitions
                # @api private
                #
                # Create the schema on a ROM database configuration
                def self.schema(config)
                    config.default.create_table :models do
                        primary_key :id
                        column :name, String, null: false
                    end

                    config.default.create_table :tasks do
                        primary_key :id
                        foreign_key :model_id, :models, null: false
                    end

                    config.default.create_table :emitted_events do
                        primary_key :id
                        foreign_key :task_id, :tasks, null: false

                        column :time, Time, null: false
                        column :name, String, null: false
                    end
                end

                def self.configure(config)
                    Sequel.application_timezone = :local
                    Sequel.database_timezone = :utc
                    config.register_relation(Models, Tasks, EmittedEvents)
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
                            has_many :emitted_events
                        end
                    end

                    struct_namespace Entities
                    auto_struct true

                    # Returns the list of events that were emitted by the given
                    # task
                    def history_of(task)
                        where(id: task.id).left_join(:emitted_events).to_a
                    end
                end

                # Representation of a Roby emitted event
                class EmittedEvents < ROM::Relation[:sql]
                    schema(:emitted_events, infer: true) do
                        associations do
                            belongs_to :task
                        end
                    end

                    struct_namespace Entities
                    auto_struct true

                    def by_name(name)
                        where(name: name.to_s)
                    end
                end
            end
        end
    end
end
