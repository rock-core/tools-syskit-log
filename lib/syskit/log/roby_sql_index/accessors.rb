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
                        return OroGenNamespace.new(@index, "OroGen") if m == :OroGen

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

                    def ==(other)
                        other.kind_of?(TaskModel) && other.id == id
                    end

                    def method_missing(m, *args, **kw, &block)
                        m_to_s = m.to_s
                        return super unless m_to_s.end_with?("_event")

                        event_name = m_to_s[0..-7]
                        unless @index.event_with_name?(event_name)
                            msg = "no events named #{event_name} have been emitted"
                            raise NoMethodError.new(msg, m)
                        end

                        unless event?(event_name)
                            msg = "there are emitted events named #{event_name}, but "\
                                  "not for a task of model #{@name}"
                            raise NoMethodError.new(msg, m)
                        end

                        EventModel.new(@index, event_name, self)
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

                    def ==(other)
                        other.kind_of?(EventModel) &&
                            other.name == name &&
                            other.task_model == task_model
                    end

                    def event_propagations_query
                        task_model.event_propagations_query.by_name(name)
                    end

                    # Enumerate the event propagations coming from generators of
                    # this model
                    def each_event_propagation(**where)
                        return enum_for(__method__, **where) unless block_given?

                        query = event_propagations_query.where(**where).order(:time)
                        query.each do |entity|
                            task = @task_model.task_by_id(entity.task_id)
                            yield EventPropagation.from_entity(
                                @index, entity, task, self
                            )
                        end
                    end

                    # List the matching event emissions
                    def each_emission(**where, &block)
                        each_event_propagation(
                            kind: EVENT_PROPAGATION_EMIT, **where, &block
                        )
                    end

                    # Get the first emission
                    def first_emission
                        each_emission.first
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

                    def ==(other)
                        other.kind_of?(Task) && other.id == id
                    end

                    def event_propagations_query
                        @model.event_propagations_query.from_task_id(id)
                    end

                    # Enumerate event propagations from events of this task
                    def each_event_propagation(**where)
                        return enum_for(__method__, **where) unless block_given?

                        query = event_propagations_query.where(**where).order(:time)
                        query.each do |entity|
                            yield EventPropagation.from_entity(
                                @index, entity, self, model
                            )
                        end
                    end

                    def each_emission(**where, &block)
                        each_event_propagation(
                            kind: EVENT_PROPAGATION_EMIT, **where, &block
                        )
                    end

                    def event(name)
                        BoundEvent.new(@index, name, self, model.event(name))
                    end

                    def respond_to_missing?(m, include_private = false)
                        super || m.to_s.end_with?("_event")
                    end

                    def method_missing(m, *args, **kw, &block)
                        if m.to_s.end_with?("_event")
                            unless args.empty? && kw.empty?
                                raise ArgumentError, "wrong number of arguments"
                            end

                            event(m[0..-7])
                        else
                            super
                        end
                    end
                end

                # An event model bound to a particular task instance
                class BoundEvent
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

                    def each_emission(&block)
                        task.each_emission(name: name, &block)
                    end

                    def first
                        each_emission.first
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
