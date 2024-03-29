# frozen_string_literal: true

module Syskit
    module Log
        module RobySQLIndex
            module Entities
                class Metadatum < ROM::Struct
                end

                class Model < ROM::Struct
                end

                class Task < ROM::Struct
                end

                # Representation of an event emission
                class EventPropagations < ROM::Struct
                    def full_name
                        "#{task.model.name}.#{name}"
                    end
                end
            end
        end
    end
end
