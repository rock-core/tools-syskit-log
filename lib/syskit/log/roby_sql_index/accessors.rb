# frozen_string_literal: true

module Syskit
    module Log
        module RobySQLIndex
            module Accessors
                # Exception raised when trying to access an event that is not registered
                class NoSuchEvent < RuntimeError; end

                # Represents the query root
                #
                # It gives access to the constants under it through the method
                # missing interface
                class Root
                    def initialize(index)
                        @index = index
                        @prefix = ""
                        @separator = "::"
                        @namespace_class = Namespace
                    end

                    def validate_method_missing_noargs(m, args, kw)
                        return if args.empty? && kw.empty?

                        raise ArgumentError,
                              "#{m} expected zero arguments, but got #{args.size} "\
                              "positional and #{kw.size} keyword arguments"
                    end

                    def respond_to_missing?(m, _include_private = false)
                        full_name = "#{@prefix}#{m}"
                        @index.models.where(name: full_name).exist? ||
                            @index.models.where { name.like("#{full_name}::%") }.exist? ||
                            super
                    end

                    def model_query
                        # !!! pattern must be calculated outside of `where`
                        pattern = "#{@prefix}%"
                        @index.models.where { name.like(pattern) }
                    end

                    def each_task_model
                        return enum_for(__method__) unless block_given?

                        model_query.pluck(:name, :id).each do |name, model_id|
                            yield TaskModel.new(@index, name, model_id)
                        end
                    end

                    def event_propagation_query
                        @index.event_propagations
                    end

                    def each_event_propagation(where: nil, **where_kw)
                        unless block_given?
                            return enum_for(__method__, where: where, **where_kw)
                        end

                        query = event_propagation_query
                                .order(:time).where(**where_kw, &where)
                        query.each do |entity|
                            task = @index.task_by_id(entity.task_id)
                            event_m = task.model.event(entity.name)
                            yield EventPropagation.from_entity(
                                @index, entity, task, event_m
                            )
                        end
                    end

                    def method_missing(m, *args, **kw, &block)
                        full_name = "#{@prefix}#{m}"
                        pattern = "#{@prefix}#{m}#{@separator}"
                        if %I[OroGen Deployments].include?(m)
                            return OroGenNamespace.new(@index, m.to_s)
                        end

                        model_id = @index.models.where(name: full_name).pluck(:id).first
                        if model_id
                            validate_method_missing_noargs(m, args, kw)
                            TaskModel.new(@index, full_name, model_id)
                        elsif @index.models.where { name.like("#{pattern}%") }.exist?
                            validate_method_missing_noargs(m, args, kw)
                            @namespace_class.new(@index, full_name)
                        else
                            super
                        end
                    end

                    def to_iruby
                        names = model_query.pluck(:name).sort
                        ["text/html", IRuby::HTML.table(names, maxrows: nil)]
                    end
                end

                # The OroGen model hierarchy
                class OroGenNamespace < Root
                    def initialize(index, name)
                        super(index)
                        @prefix = "#{name}."
                        @separator = "."
                        @namespace_class = OroGenNamespace
                    end
                end

                # A non-root namespace
                #
                # It gives access to the constants under it through the method
                # missing interface
                class Namespace < Root
                    def initialize(index, name)
                        super(index)
                        @prefix = "#{name}::"
                    end
                end

                # A task model
                #
                # It can give access to the constants under it, or to the events
                # that are known to the index
                class TaskModel < Namespace
                    # The task model name
                    attr_reader :name
                    # A unique ID for this task model
                    attr_reader :id

                    def initialize(index, name, id)
                        super(index, name)
                        @name = name
                        @id = id
                        @events = {}
                        @tasks = {}
                    end

                    def ==(other)
                        other.kind_of?(TaskModel) && other.id == id
                    end

                    def each_event
                        return enum_for(__method__) unless block_given?

                        event_propagations_query
                            .select(:name).distinct
                            .pluck(:name).each do |event_name|
                                yield(EventModel.new(@index, event_name, self))
                            end
                    end

                    def event_propagations_query
                        @index.event_propagations.where(task_id: task_ids)
                    end

                    def each_event_propagation(**where)
                        return enum_for(__method__, **where) unless block_given?

                        query = event_propagations_query.where(**where).order(:time)
                        query.each do |entity|
                            event_m = event(entity.name)
                            task = task_by_id(entity.task_id)
                            yield EventPropagation.from_entity(
                                @index, entity, task, event_m
                            )
                        end
                    end

                    def orogen_model_name
                        unless (m = /^OroGen\./.match(name))
                            raise ArgumentError, "#{name} is not an orogen model"
                        end

                        m.post_match.gsub(".", "::")
                    end

                    def each_port_model
                        return enum_for(__method__) unless block_given?

                        streams = @index.find_all_streams(
                            RockStreamMatcher
                            .new.ports
                            .task_orogen_model_name(orogen_model_name)
                        )
                        port_names = streams.map(&:task_object_name).uniq
                        port_names.each do |name|
                            streams = streams.find_all_streams(
                                RockStreamMatcher.new.object_name(name)
                            )
                            yield PortModel.new(self, name, streams)
                        end
                    end

                    def find_port_by_name(name)
                        streams = @index.find_all_streams(
                            RockStreamMatcher
                            .new.ports
                            .task_orogen_model_name(orogen_model_name)
                            .object_name(name)
                        )
                        if streams.empty?
                            raise ArgumentError,
                                  "no port stream named #{name} in this dataset"
                        end

                        PortModel.new(self, name, Streams.new(streams))
                    end

                    def each_emission(**where, &block)
                        each_event_propagation(
                            kind: EVENT_PROPAGATION_EMIT, **where, &block
                        )
                    end

                    # Tests whether this task model seems to have an event with that name
                    def event?(name)
                        @index.event_propagations
                              .from_task_id(task_ids).where(name: name).exist?
                    end

                    # Return the event model with the given name
                    #
                    # @param [String] name
                    # @return [EventModel]
                    # @raise NoSuchEvent if there are no events with that name
                    def event(name)
                        if (ev = @events[name])
                            return ev
                        end

                        unless event?(name)
                            raise NoSuchEvent, "no events named '#{name}' in #{self}'"
                        end

                        @events[name] = EventModel.new(@index, name, self)
                    end

                    # @api private
                    #
                    # Query that returns the tasks from this model
                    def tasks_query
                        @index.tasks.where(model_id: id)
                    end

                    # Enumerate the tasks that are instances of this model
                    def each_task
                        return enum_for(__method__) unless block_given?

                        tasks_query.each do |entity|
                            yield task_from_entity(entity)
                        end
                    end

                    def find_event_by_name(event_name)
                        event_name = event_name.to_str
                        return unless @index.event_with_name?(event_name)
                        return unless event?(event_name)

                        EventModel.new(@index, event_name, self)
                    end

                    def respond_to_missing?(m, include_private = true)
                        MetaRuby::DSLs.has_through_method_missing?(
                            self, m,
                            "_event" => "find_event_by_name",
                            "_port" => "find_port_by_name"
                        ) || super
                    end

                    def method_missing(m, *args, **kw, &block)
                        MetaRuby::DSLs.find_through_method_missing(
                            self, m, args,
                            "_event" => "find_event_by_name",
                            "_port" => "find_port_by_name"
                        ) || super
                    end

                    # Return the task instance object with the given ID
                    def task_by_id(task_id)
                        if (task = @tasks[task_id])
                            return task
                        end

                        entity = @index.tasks.by_pk(task_id).where(model_id: id).one
                        unless entity
                            raise ArgumentError,
                                  "no task with ID #{task_id} and model #{name}"
                        end

                        @tasks[task_id] = task_from_entity(entity)
                    end

                    # Enumerate the

                    # @api private
                    #
                    # The query that returns the task IDs of the instances of this model
                    def task_ids
                        @task_ids ||= tasks_query.pluck(:id)
                    end

                    # @api private
                    #
                    # Create a Task from the database entity
                    #
                    # @param [Entities::Task]
                    # @return [Task]
                    def task_from_entity(entity)
                        Task.new(@index, entity, self)
                    end
                end

                # Representation of a port on a task model
                #
                # Use {Task#find_port} or {#each_port} to get ports of actual task
                # instances
                class PortModel
                    attr_reader :task_model
                    attr_reader :name
                    attr_reader :streams

                    def initialize(task_model, name, streams)
                        @task_model = task_model
                        @name = name
                        @streams = streams
                    end

                    def bind(task)
                        orocos_name = task.arguments[:orocos_name]
                        task_stream =
                            @streams.find_task_by_name(orocos_name).streams.first
                        Port.new(task, @name, task_stream)
                    end

                    def each_port
                        return enum_for(__method__) unless block_given?

                        task_model.each_task { |task| yield bind(task) }
                    end
                end

                # Represents an event generator model
                class EventModel
                    # The event's name
                    attr_reader :name
                    # This event's task model
                    attr_reader :task_model

                    def initialize(index, name, task_model)
                        @index = index
                        @name = name
                        @task_model = task_model
                    end

                    # Returns the event generator model for the given task instance
                    #
                    # @return [Event]
                    def bind(task)
                        Event.new(@index, @name, task, self)
                    end

                    def ==(other)
                        other.kind_of?(EventModel) &&
                            other.name == name &&
                            other.task_model == task_model
                    end

                    def event_propagations_query
                        task_model.event_propagations_query.by_name(name)
                    end

                    # @api private
                    #
                    # Create an EventPropagation accessor from the DRY entity
                    #
                    # @return [EventPropagation]
                    def event_propagation_from_entity(entity)
                        task = @task_model.task_by_id(entity.task_id)
                        EventPropagation.from_entity(@index, entity, task, self)
                    end

                    # Return the first propagation matching a given query
                    #
                    # @return [EventPropagation,nil]
                    def first_event_propagation(**where)
                        entity = event_propagations_query
                                 .where(**where).order(:time).first
                        event_propagation_from_entity(entity) if entity
                    end

                    # Return the last propagation matching a given query
                    #
                    # @return [EventPropagation,nil]
                    def last_event_propagation(**where)
                        entity = event_propagations_query
                                 .where(**where).order(:time).last
                        event_propagation_from_entity(entity) if entity
                    end

                    # Enumerate the event propagations coming from generators of
                    # this model
                    def each_event_propagation(**where)
                        return enum_for(__method__, **where) unless block_given?

                        query = event_propagations_query.where(**where).order(:time)
                        query.each do |entity|
                            yield event_propagation_from_entity(entity)
                        end
                    end

                    # List the matching event emissions
                    #
                    # @yieldparam [EventPropagation] propagation
                    def each_emission(**where, &block)
                        each_event_propagation(
                            kind: EVENT_PROPAGATION_EMIT, **where, &block
                        )
                    end

                    # Get the first emission matching the given query
                    #
                    # @return [EventPropagation,nil]
                    def first_emission(**where)
                        first_event_propagation(kind: EVENT_PROPAGATION_EMIT, **where)
                    end

                    # Get the last emission matching the given query
                    #
                    # @return [EventPropagation,nil]
                    def last_emission(**where)
                        last_event_propagation(kind: EVENT_PROPAGATION_EMIT, **where)
                    end

                    def full_name
                        "#{task_model.name}.#{name}_event"
                    end
                end

                # Represents a task instance
                class Task
                    # The task model
                    #
                    # @return [TaskModel]
                    attr_reader :model

                    def initialize(index, obj, model)
                        @index = index
                        @obj = obj
                        @model = model
                    end

                    def id
                        @obj.id
                    end

                    def ==(other)
                        other.kind_of?(Task) && other.id == id
                    end

                    def arguments
                        return @arguments if @arguments

                        @arguments = JSON.load(@obj.arguments).transform_keys(&:to_sym) # rubocop:disable Security/JSONLoad
                    end

                    def start_time
                        event("start").first&.time
                    rescue NoSuchEvent # rubocop:disable Lint/SuppressedException
                    end

                    def stop_time
                        stop_ev =
                            begin
                                event("stop").first
                            rescue NoSuchEvent # rubocop:disable Lint/SuppressedException
                            end

                        stop_ev&.time || (@index.time_end if start_time)
                    end

                    # Return the task's activation interval
                    #
                    # This is named like this to match Pocolog::DataStream's interface
                    def interval_lg
                        [start_time, stop_time]
                    end

                    def event_propagations_query
                        @model.event_propagations_query.from_task_id(id)
                    end

                    # @api private
                    #
                    # @return [EventPropagation]
                    def event_propagation_from_entity(entity)
                        event_m = model.event(entity.name)
                        EventPropagation.from_entity(@index, entity, self, event_m)
                    end

                    # Enumerate event propagations from events of this task
                    #
                    # @yieldparam [EventPropagation] propagation
                    def each_event_propagation(**where)
                        return enum_for(__method__, **where) unless block_given?

                        query = event_propagations_query.where(**where).order(:time)
                        query.each do |entity|
                            yield event_propagation_from_entity(entity)
                        end
                    end

                    # Return the first event propagation of one of this task's generators
                    #
                    # @return [EventPropagation,nil]
                    def first_event_propagation(**where)
                        entity = event_propagations_query
                                 .where(**where).order(:time).first
                        event_propagation_from_entity(entity) if entity
                    end

                    # Return the last event propagation of one of this task's generators
                    #
                    # @return [EventPropagation,nil]
                    def last_event_propagation(**where)
                        entity = event_propagations_query
                                 .where(**where).order(:time).last
                        event_propagation_from_entity(entity) if entity
                    end

                    # Get the first emission matching the given query
                    #
                    # The query is obviously scoped to the task's own event generators
                    #
                    # @return [EventPropagation,nil]
                    def first_emission(**where)
                        first_event_propagation(kind: EVENT_PROPAGATION_EMIT, **where)
                    end

                    # Get the last emission matching the given query
                    #
                    # The query is obviously scoped to the task's own event generators
                    #
                    # @return [EventPropagation,nil]
                    def last_emission(**where)
                        last_event_propagation(kind: EVENT_PROPAGATION_EMIT, **where)
                    end

                    # Enumerate this task's emissions
                    #
                    # @yieldparam [EventPropagation] propagation the emission (`kind` is
                    #   always EVENT_PROPAGATION_EMIT)
                    def each_emission(**where, &block)
                        each_event_propagation(
                            kind: EVENT_PROPAGATION_EMIT, **where, &block
                        )
                    end

                    def event(name)
                        unless (ev = find_event_by_name(name))
                            raise NoSuchEvent,
                                  "cannot find an event '#{name}' on #{self}"
                        end

                        ev
                    end

                    # Look for one of this task's events
                    #
                    # @param [String] name
                    # @return [Event,nil]
                    def find_event_by_name(name)
                        model.find_event_by_name(name)&.bind(self)
                    end

                    # Look for one of this task's ports
                    #
                    # @param [String] name
                    # @return [Port,nil]
                    def find_port_by_name(name)
                        model.find_port_by_name(name)&.bind(self)
                    end

                    # Enumerate this task's ports
                    #
                    # @yieldparam [Port] port
                    def each_port
                        return enum_for(__method__) unless block_given?

                        task_model.each_port_model { |p| p.bind(self) }
                    end

                    def respond_to_missing?(m, include_private = false)
                        MetaRuby::DSLs.has_through_method_missing?(
                            self, m,
                            "_event" => "find_event_by_name",
                            "_port" => "find_port_by_name"
                        ) || super
                    end

                    def method_missing(m, *args, **kw, &block)
                        MetaRuby::DSLs.find_through_method_missing(
                            self, m, args,
                            "_event" => "find_event_by_name",
                            "_port" => "find_port_by_name"
                        ) || super
                    end
                end

                # A port of a given task instance
                #
                # Port objects can be used in the data processing DSL as data
                # streams are. They are simply the port's data stream limited to
                # the task's lifetime.
                class Port
                    attr_reader :task
                    attr_reader :name

                    def ==(other)
                        other.task == task && other.name == name
                    end

                    def initialize(task, name, global_stream)
                        @task = task
                        @name = name
                        @global_stream = global_stream
                    end

                    def stream
                        s = @global_stream.syskit_eager_load
                        start, stop = @task.interval_lg
                        return unless start

                        s.from_logical_time(start).to_logical_time(stop)
                    end

                    def samples
                        stream.samples
                    end
                end

                # An event model bound to a particular task instance
                #
                # Note that such an event object is an event source (in Roby parlance,
                # an event generator). That is, it may have emitted zero, one or many
                # times.
                class Event
                    # The event name
                    attr_reader :name
                    # The task it is bound to
                    attr_reader :task
                    # The event model
                    attr_reader :model

                    def initialize(index, name, task, model)
                        @index = index
                        @name = name
                        @task = task
                        @model = model
                    end

                    # Enumerate the emissions of this event source
                    def each_emission(&block)
                        task.each_emission(name: name, &block)
                    end

                    # Return the first emission of this event source
                    #
                    # @return [EventPropagation]
                    def first_emission
                        task.first_emission(name: name)
                    end

                    # Return the last emission of this event source
                    #
                    # @return [EventPropagation]
                    def last_emission
                        task.last_emission(name: name)
                    end

                    # @deprecated use {#first_emission} instead
                    def first
                        first_emission
                    end

                    # @deprecated use {#last_emission} instead
                    def last
                        last_emission
                    end
                end

                # Represents an emitted event
                class EventPropagation
                    # The event's emission time
                    attr_reader :time
                    # The event's name
                    attr_reader :name
                    # The event propagation's type (as one of the
                    # EVENT_PROPAGATION_ constants)
                    attr_reader :kind
                    # The emission context (if kind is EVENT_PROPAGATION_EMIT)
                    def context
                        @context ||= (JSON.parse(@json_context) if @json_context)
                    end

                    # The event's task
                    #
                    # @return [Task]
                    attr_reader :task
                    # The event's model
                    #
                    # @return [EventModel]
                    attr_reader :model

                    # A unique ID for this propagation
                    attr_reader :id

                    def ==(other)
                        other.kind_of?(Event) && other.id == id
                    end

                    def full_name
                        "#{task.model.name}.#{name}"
                    end

                    def self.from_entity(index, entity, task, model)
                        new(index, entity.kind, entity.id, entity.time, entity.name,
                            entity.context, task, model)
                    end

                    # @param [Task] task
                    # @param [EventModel] model
                    def initialize(index, kind, id, time, name, context, task, model) # rubocop:disable Metrics/ParameterLists
                        @index = index
                        @kind = kind
                        @id = id
                        @time = time
                        @name = name
                        @json_context = context
                        @task = task
                        @model = model
                    end
                end
            end
        end
    end
end
