# frozen_string_literal: true

module Syskit
    module Log
        module RobySQLIndex
            module Accessors
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

                    def method_missing(m, *args, **kw, &block)
                        full_name = "#{@prefix}#{m}"
                        pattern = "#{@prefix}#{m}#{@separator}"
                        if m == :OroGen
                            OroGenNamespace.new(@index, "OroGen")
                        elsif @index.models.where(name: full_name).exist?
                            validate_method_missing_noargs(m, args, kw)
                            TaskModel.new(@index, full_name)
                        elsif @index.models.where { name.like("#{pattern}%") }.exist?
                            validate_method_missing_noargs(m, args, kw)
                            @namespace_class.new(@index, full_name)
                        else
                            super
                        end
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

                    def initialize(index, name)
                        super(index, name)
                        @name = name
                        @id = @index.models.where(name: name).one!.id
                        @query = @index.tasks.where(model_id: id)
                    end

                    def each_event
                        return enum_for(__method__) unless block_given?

                        @index.history_of(@query)
                              .select(:name).distinct
                              .pluck(:name).each do |event_name|
                                  yield(EventModel.new(@index, event_name, self))
                              end
                    end

                    # Return the event model with the given name
                    #
                    # @raise ArgumentError if there are no events with that name
                    def event(name)
                        unless @index.history_of(@query).where(name: name).first
                            raise ArgumentError, "no events named '#{name}' in #{self}'"
                        end

                        EventModel.new(@index, name, self)
                    end

                    def each_task
                        return enum_for(__method__) unless block_given?

                        @query.each do |obj|
                            yield Task.new(@index, obj.id, self)
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
                            raise NoMethodError.new(m),
                                  "no events named #{event_name} have been emitted"
                        end

                        has_events =
                            @index.history_of(@index.tasks_by_model_name(@name))
                                  .where(name: event_name)
                                  .exist?

                        unless has_events
                            raise NoMethodError.new("", m),
                                  "there are emitted events named #{event_name}, but "\
                                  "not for a task of model #{@name}"
                        end

                        EventModel.new(@index, event_name, self)
                    end

                    # Return the task instance object with the given ID
                    def by_id(id)
                        Task.new(@index, id, self)
                    end

                    # @api private
                    #
                    # The query that returns the task IDs of the instances of this model
                    def task_ids
                        @query.pluck(:id)
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
                        @query = @index.emitted_events
                                       .where(name: name, task_id: task_model.task_ids)
                    end

                    def ==(other)
                        other.kind_of?(EventModel) &&
                            other.name == name &&
                            other.task_model == task_model
                    end

                    # List the matching event emissions
                    def each_emission
                        return enum_for(__method__) unless block_given?

                        @query.each do |obj|
                            yield Event.new(@index, obj.id, obj.time, obj.name,
                                            @task_model.by_id(obj.task_id), self)
                        end
                    end

                    # Get the first emission
                    def first_emission
                        each_emission.first
                    end
                end

                # Represents a task instance
                class Task
                    # A unique ID
                    attr_reader :id
                    # The task model
                    #
                    # @return [TaskModel]
                    attr_reader :model

                    def initialize(index, id, model)
                        @index = index
                        @id = id
                        @model = model
                    end

                    # Return the task's activation interval
                    #
                    # This is named like this to match Pocolog::DataStream's interface
                    def interval_lg
                        [start_event.first.time, stop_event.first.time]
                    end

                    def ==(other)
                        other.kind_of?(Task) && other.id == id
                    end

                    def each_emission(**where)
                        return enum_for(__method__, **where) unless block_given?

                        query = @index.emitted_events
                                      .where(task_id: @id, **where)
                        query.each do |emission|
                            name = emission.name
                            yield Event.new(@index, emission.id, emission.time, name,
                                            self, model.event(name))
                        end
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
                class Event
                    # The event's emission time
                    attr_reader :time
                    # The event's name
                    attr_reader :name

                    # The event's task
                    attr_reader :task
                    # The event's model
                    attr_reader :model

                    # A unique ID for this event
                    attr_reader :id

                    def ==(other)
                        other.kind_of?(Event) && other.id == id
                    end

                    def initialize(index, id, time, name, task, model) # rubocop:disable Metrics/ParameterLists
                        @index = index
                        @id = id
                        @time = time
                        @name = name
                        @task = task
                        @model = model
                    end
                end
            end
        end
    end
end
