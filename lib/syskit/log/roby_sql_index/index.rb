module Syskit
    module Log
        module RobySQLIndex
            # Access and creation API of the Roby SQL index
            class Index
                # Opens an existing index file, or creates one
                def self.open(path)
                    raise ArgumentError, "#{path} does not exist" unless path.exist?

                    rom = ROM.container(:sql, "sqlite://#{path}") do |config|
                        Definitions.configure(config)
                    end
                    new(rom)
                end

                # Create a new index file
                def self.create(path)
                    raise ArgumentError, "#{path} already exists" if path.exist?

                    rom = ROM.container(:sql, "sqlite://#{path}") do |config|
                        Definitions.schema(config)
                        Definitions.configure(config)
                    end
                    new(rom)
                end

                def initialize(rom)
                    @rom = rom
                    @models = rom.relations[:models]
                    @tasks = rom.relations[:tasks]
                    @emitted_events = rom.relations[:emitted_events]
                end

                def dispose
                    @db.close
                end

                # Access to models stored in the index
                #
                # @return [Models]
                attr_reader :models

                # Access to tasks stored in the index
                #
                # @return [Tasks]
                attr_reader :tasks

                # Access to emitted events stored in the index
                #
                # @return [EmittedEvents]
                attr_reader :emitted_events

                # Add information from a raw Roby log
                def add_roby_log(path, reporter: Pocolog::CLI::NullReporter.new)
                    require "roby/droby/logfile/reader"
                    require "roby/droby/plan_rebuilder"

                    @registered_models = {}
                    @registered_tasks = {}

                    size = path.stat.size
                    reporter.reset_progressbar("#{path.basename} [:bar]", total: size)

                    stream = Roby::DRoby::Logfile::Reader.open(path)
                    rebuilder = Roby::DRoby::PlanRebuilder.new

                    while (data = stream.load_one_cycle)
                        data.each_slice(4) do |m, sec, usec, args|
                            rebuilder.process_one_event(m, sec, usec, args)
                        end

                        @emitted_events.transaction do
                            add_log_emitted_events(rebuilder.plan.emitted_events)
                        end
                        rebuilder&.clear_integrated
                        reporter.current = stream.tell
                    end
                ensure
                    stream&.close
                end

                # @api private
                #
                # Add information about an emitted event
                #
                # @param [Roby::Event] ev
                # @return [Integer] the record ID
                def add_log_emitted_events(events)
                    task_ids = add_log_tasks(events.map { |e| e.generator.task })
                    records = events.zip(task_ids).map do |ev, task_id|
                        { name: ev.symbol.to_s, time: ev.time, task_id: task_id }
                    end

                    @emitted_events.command(:create, result: :many).call(records)
                end

                # @api private
                #
                # Add information about a task instance
                #
                # @param [Roby::Task] task
                # @return [Integer] the record ID
                def add_log_tasks(tasks)
                    unique_tasks = tasks.uniq(&:droby_id)
                    new_tasks = unique_tasks.find_all do |task|
                        !@registered_tasks[task.droby_id]
                    end

                    model_ids = new_tasks.map { |t| { model_id: add_model(t.model) } }
                    new_task_ids =
                        @tasks
                        .command(:create, result: :many)
                        .call(model_ids)
                        .map(&:id)
                    new_tasks.zip(new_task_ids).each do |task, id|
                        @registered_tasks[task.droby_id] = id
                    end

                    tasks.map { |t| @registered_tasks.fetch(t.droby_id) }
                end

                # @api private
                #
                # Add information about a Roby model
                #
                # @param [Class<Roby::Task>] model
                # @return [Integer] the record ID
                def add_model(model)
                    if (model_id = @registered_models[model.droby_id])
                        return model_id
                    end

                    match = @models.where(name: model.name).pluck(:id).first
                    return @registered_models[model.droby_id] = match if match

                    @registered_models[model.droby_id] =
                        @models.insert({ name: model.name })
                end

                # Return the events emitted by the given task
                def history_of(task)
                    if task.respond_to?(:pluck)
                        @emitted_events.where(task_id: task.pluck(:id))
                    else
                        @emitted_events.where(task_id: task.id)
                    end
                end

                # Tests whether there are events with the given name
                def event_with_name?(name)
                    @emitted_events.where(name: name).exist?
                end

                # Return the events emitted by the given task
                def tasks_by_model_name(name)
                    @tasks.where(model_id: @models.where(name: name).pluck(:id))
                end

                # Return the events emitted by the given task
                def tasks_by_model(model)
                    @tasks.where(model: model)
                end

                # Returns the full name of an event
                def event_full_name(event)
                    model_id = @tasks.by_pk(event.task_id).pluck(:id)
                    model_name = @models.by_pk(model_id).pluck(:name).first
                    "#{model_name}.#{event.name}_event"
                end
            end
        end
    end
end
