# frozen_string_literal: true

module Syskit
    module Log
        module DSL
            # @api private
            class Summary
                # Option hash passed to {#initialize}
                #
                # It is accessible within the templates as 'options'
                attr_reader :options

                def initialize(object, zero_time, type: guess_type(object), **options)
                    @object = object
                    @zero_time = zero_time
                    @type = type
                    @options = options
                end

                def summarize(object, type: guess_type(object), **options)
                    Summary.new(object, @zero_time, type: type, **options).to_html
                end

                ARRAY_RENDERERS = {
                    RobySQLIndex::Accessors::Task => "roby_tasks",
                    RobySQLIndex::Accessors::Event => "roby_events"
                }.freeze

                SUMMARY_TYPES = {
                    Array => "array",
                    Enumerator => "array",
                    Datastore::Dataset => "dataset",
                    TaskStreams => "task_streams",
                    LazyDataStream => "data_stream",
                    RobySQLIndex::Accessors::TaskModel => "roby_task_model",
                }.merge(ARRAY_RENDERERS).freeze

                def guess_type(object)
                    SUMMARY_TYPES.each do |matcher, name|
                        return name if matcher === object
                    end

                    raise ArgumentError, "do not know how to summarize #{object}"
                end

                def relative_time(time)
                    time - @zero_time if @zero_time
                end

                def to_html
                    object_to_html(@object, @type)
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
