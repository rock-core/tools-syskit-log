# frozen_string_literal: true

module Syskit
    module Log
        module DSL
            # @api private
            class Summary
                def initialize(object, zero_time)
                    @object = object
                    @zero_time = zero_time
                end

                def relative_time(time)
                    time - @zero_time if @zero_time
                end

                def to_html
                    case @object
                    when Datastore::Dataset
                        object_to_html(@object, "dataset")
                    when TaskStreams
                        object_to_html(@object, "task_streams")
                    when LazyDataStream
                        object_to_html(@object, "data_stream")
                    when RobySQLIndex::Accessors::TaskModel
                        object_to_html(@object, "roby_task_model")
                    end
                end

                def object_to_html(object, type)
                    path = File.expand_path("templates/summary_#{type}.html.erb", __dir__)
                    template = File.read(path)
                    bind = binding
                    bind.local_variable_set type.to_sym, object
                    ERB.new(template).result(bind)
                end
            end
        end
    end
end
