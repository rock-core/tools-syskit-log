# frozen_string_literal: true

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
                    @metadata = rom.relations[:metadata]
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
                    metadata_update = start_roby_log_import(path.basename.to_s)

                    size = path.stat.size
                    reporter.reset_progressbar("#{path.basename} [:bar]", total: size)

                    stream = Roby::DRoby::Logfile::Reader.open(path)
                    rebuilder = Roby::DRoby::PlanRebuilder.new

                    while (data = stream.load_one_cycle)
                        add_one_cycle(metadata_update, rebuilder, data)
                        reporter.current = stream.tell
                    end
                ensure
                    stream&.close
                end

                # Do the necessary initialization to import a new log
                #
                # @param [String] name the name for the new log
                # @return [#call] metadata updater object for the newly created log,
                #   meant to be passed to e.g. {#add_one_cycle}
                def start_roby_log_import(name)
                    require "roby/droby/logfile/reader"
                    require "roby/droby/plan_rebuilder"

                    @registered_models = {}
                    @registered_tasks = {}

                    metadata = @metadata.command(:create).call(
                        { name: name, cycle_count: 0,
                          time_start: Time.at(0), time_end: Time.at(0) }
                    )
                    @metadata.by_pk(metadata.id).command(:update)
                end

                # Add a cycle worth of data to the index
                #
                # @param [#call] metadata a ROM command object that allows to update the
                #   log metadata
                # @param [Roby::DRoby::PlanRebuilder] rebuilder Roby's plan rebuilder
                #   used to decode the log
                # @param [Array] data the cycle data
                def add_one_cycle(metadata, rebuilder, data)
                    data.each_slice(4) do |m, sec, usec, args|
                        rebuilder.process_one_event(m, sec, usec, args)
                    end

                    @emitted_events.transaction do
                        add_log_emitted_events(rebuilder.plan.emitted_events)
                    end

                    update_log_metadata(metadata, data)
                    rebuilder.clear_integrated
                end

                # @api internal
                #
                # Update the metadata of a single log using the cycle end statistics
                def update_log_metadata(metadata, cycle)
                    m, _, _, stats = cycle[-4, 4]
                    if m != :cycle_end
                        raise "unexpected last message in cycle data, "\
                              "expected cycle_end but got #{m}"
                    end
                    stats = stats.first

                    cycle_index = stats[:cycle_index]
                    time_start_s, time_start_usec = stats[:start]
                    time_start = Time.at(time_start_s, time_start_usec)
                    time_end = time_start + stats[:end]

                    if cycle_index == 0
                        s, us = cycle[1, 2]
                        metadata.call({ time_start: Time.at(s, us) })
                    end

                    metadata.call(
                        {
                            cycle_count: cycle_index,
                            time_end: time_end
                        }
                    )
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

                # Exception raised when trying to get information about a logfile
                # that is not registered in the index
                class NoSuchLogfile < RuntimeError; end

                # Enumerate information about each log added to this index
                def each_log_metadata(&block)
                    @metadata.each(&block)
                end

                # Return the metadata information for a log file from its basename
                #
                # @param [String] name
                def log_metadata_for(name)
                    unless (info = @metadata.where(name: name).one)
                        raise NoSuchLogfile, "no log file named #{name} in this index"
                    end

                    info
                end

                def time_start
                    @metadata.order(:time_start).first.time_start
                end

                def time_end
                    @metadata.order(:time_end).last.time_end
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

                # Task model accessor from its ID
                def task_model_by_id(model_id)
                    name = @models.where(id: model_id).pluck(:name).first
                    raise ArgumentError, "no task model with ID #{model_id}" unless name

                    Accessors::TaskModel.new(self, name, model_id)
                end

                # Task accessor from its ID
                def task_by_id(id)
                    model_id = @tasks.where(id: id).pluck(:model_id).first
                    raise ArgumentError, "no task with ID #{id}" unless model_id

                    model = task_model_by_id(model_id)
                    Accessors::Task.new(self, id, model)
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
