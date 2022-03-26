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

                TEMPLATES_PATH = Pathname.new(__dir__).expand_path.join("templates")

                ARRAY_TYPE_MATCHERS = {
                    RobySQLIndex::Accessors::Task => "roby_tasks",
                    RobySQLIndex::Accessors::EventPropagation =>
                        "roby_event_propagations"
                }.freeze

                TYPELIB_TYPE_MATCHER = Class.new do
                    def ===(obj)
                        Class === obj && (obj <= Typelib::Type)
                    end
                end.new

                TYPE_MATCHERS = {
                    Array => "array",
                    Enumerator => "array",
                    Datastore::Dataset => "dataset",
                    TaskStreams => "task_streams",
                    LazyDataStream => "data_stream",
                    Pocolog::DataStream => "data_stream",
                    RobySQLIndex::Accessors::TaskModel => "roby_task_model",
                    RobySQLIndex::Accessors::EventModel => "roby_event_model",
                    TYPELIB_TYPE_MATCHER => "type"
                }.merge(ARRAY_TYPE_MATCHERS).freeze

                def initialize( # rubocop:disable Metrics/ParameterLists
                    object, zero_time,
                    templates_path: TEMPLATES_PATH,
                    type_matchers: TYPE_MATCHERS,
                    type: resolve_type_from_object(object, type_matchers, :iruby),
                    array_type_matchers: ARRAY_TYPE_MATCHERS,
                    **options
                )
                    @object = object
                    @zero_time = zero_time
                    @type = type
                    @options = options
                    @templates_path = templates_path
                    @type_matchers = type_matchers
                    @array_type_matchers = array_type_matchers
                end

                def summarize(object, type: find_type_of(object), **options)
                    Summary.new(
                        object, @zero_time,
                        type: type,
                        templates_path: @templates_path,
                        type_matchers: @type_matchers,
                        array_type_matchers: @array_type_matchers,
                        **options
                    ).object_to_html
                end

                # Return a suitable array type from one of its element
                #
                # The corresponding template assumes that all the elements in
                # the array are compatible
                def find_array_type_from_element(element)
                    resolve_type_from_object(element, @array_type_matchers, nil)
                end

                # Return a suitable type for a given object
                #
                # It always falls back to :iruby, which calls #to_iruby
                def find_type_of(object)
                    resolve_type_from_object(object, @type_matchers, :iruby)
                end

                # @private
                def resolve_type_from_object(object, matchers, default)
                    matchers.each do |matcher, name|
                        return name if matcher === object
                    end

                    default
                end

                def relative_time(time)
                    time - @zero_time if @zero_time
                end

                def to_iruby
                    return @object.to_iruby if @type == :iruby

                    ["text/html", object_to_html(@object, @type)]
                end

                def object_to_html(object = @object, type = @type)
                    return iruby_object_to_html(object) if type == :iruby

                    path = @templates_path.join("summary_#{type}.html.erb")
                    template = path.read
                    bind = binding
                    bind.local_variable_set type.to_sym, object
                    erb = ERB.new(template)
                    erb.location = [path.to_s, 0]
                    erb.result(bind)
                end

                def iruby_object_to_html(object)
                    mime, body = object.to_iruby
                    unless %w[text/html text/plain].include?(mime)
                        raise ArgumentError,
                              "#{object}.to_iruby returns neither HTML nor "\
                              "plain text, cannot format"
                    end

                    body
                end
            end
        end
    end
end
