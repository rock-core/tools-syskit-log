# frozen_string_literal: true

module Syskit
    module Log
        module RobySQLIndex
            # Access and creation API of the Roby SQL index
            class Index
                # Opens an existing index file, or creates one
                def self.open(path, dataset: nil)
                    raise ArgumentError, "#{path} does not exist" unless path.exist?

                    uri = "sqlite://#{path}"
                    rom = ROM.container(:sql, uri) do |config|
                        Definitions.configure(config)
                    end
                    new(rom, dataset: dataset)
                end

                # Create a new index file
                def self.create(path, dataset: nil)
                    raise ArgumentError, "#{path} already exists" if path.exist?

                    uri = "sqlite://#{path}"
                    rom = ROM.container(:sql, uri) do |config|
                        Definitions.schema(config)
                        Definitions.configure(config)
                    end
                    new(rom, dataset: dataset)
                end

                def initialize(rom, dataset: nil)
                    @rom = rom
                    @models = rom.relations[:models]
                    @tasks = rom.relations[:tasks]
                    @event_propagations = rom.relations[:event_propagations]
                    @metadata = rom.relations[:metadata]

                    @dataset = dataset
                end

                def close
                    @rom.disconnect
                end

                # Access to models stored in the index
                #
                # @return [Models]
                attr_reader :models

                # Access to tasks stored in the index
                #
                # @return [Tasks]
                attr_reader :tasks

                # Access to information about event propagation
                #
                # @return [EventPropagations]
                attr_reader :event_propagations

                # Add information from a raw Roby log
                def add_roby_log(path, reporter: NullReporter.new)
                    metadata_update = start_roby_log_import(path.basename.to_s)

                    size = path.stat.size
                    reporter.reset_progressbar("#{path.basename} [:bar]", total: size)

                    stream = Roby::DRoby::Logfile::Reader.open(path)
                    rebuilder = Roby::DRoby::PlanRebuilder.new

                    cycles = []
                    count = 0
                    while (data = stream.load_one_cycle)
                        cycles << data
                        count += data.size

                        if count > 10_000
                            @event_propagations.transaction do
                                cycles.each do |cycle_data|
                                    add_one_cycle(metadata_update, rebuilder, cycle_data)
                                end
                            end
                            cycles = []
                            count = 0

                            reporter.current = stream.tell
                        end
                    end

                    @event_propagations.transaction do
                        cycles.each do |cycle_data|
                            add_one_cycle(metadata_update, rebuilder, cycle_data)
                        end
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

                    add_log_emitted_events(rebuilder.plan.emitted_events)
                    add_log_propagated_events(rebuilder.plan.propagated_events)
                    add_log_failed_emissions(rebuilder.plan.failed_emissions)

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
                # Add information about event emissions
                def add_log_emitted_events(records)
                    records = records.map do |time, event|
                        next unless event.generator

                        [EVENT_PROPAGATION_EMIT, time, event.generator,
                         (event.context unless !event.context || event.context.empty?)]
                    end

                    add_log_event_propagations(records)
                end

                # @api private
                #
                # Add information about an event call
                #
                # @param [Array] records records, as stored in
                #   {Roby::PlanRebuilder#propagated_events}
                # @return [Integer] the record ID
                def add_log_propagated_events(records)
                    records = records.map do |time, is_forwarding, _, generator|
                        # emissions are handled by add_log_event_emissions
                        next if is_forwarding

                        [EVENT_PROPAGATION_CALL, time, generator]
                    end

                    add_log_event_propagations(records.compact)
                end

                # @api private
                #
                # Add information about an event emission that failed
                #
                # @param [Roby::Event] ev
                # @return [Integer] the record ID
                def add_log_failed_emissions(records)
                    records = records.map do |time, generator, _|
                        [EVENT_PROPAGATION_EMIT_FAILED, time, generator]
                    end
                    add_log_event_propagations(records)
                end

                # @api private
                #
                # Helper for the other methods that add event propagations to the log
                #
                # @param [Array] records the info to add, as (kind, time, generator)
                def add_log_event_propagations(records)
                    tasks = records.map { |_, _, g| g.task }
                    task_ids = add_log_tasks(tasks)
                    records =
                        records
                        .zip(task_ids)
                        .map do |(kind, time, generator, context), task_id|
                            { name: generator.symbol.to_s, time: time, task_id: task_id,
                              context: JSON.dump(context), kind: kind }
                        end

                    @event_propagations.command(:create, result: :many).call(records)
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

                    model_records = new_tasks.map { |t| make_task_record(t) }
                    new_task_ids =
                        @tasks
                        .command(:create, result: :many)
                        .call(model_records)
                        .map(&:id)
                    new_tasks.zip(new_task_ids).each do |task, id|
                        @registered_tasks[task.droby_id] = id
                    end

                    tasks.map { |t| @registered_tasks.fetch(t.droby_id) }
                end

                def make_task_record(task)
                    arguments = task.arguments.to_hash.transform_values do |v|
                        if v.kind_of?(Typelib::Type)
                            v.to_simple_value
                        else
                            v
                        end
                    end

                    arguments_json =
                        begin
                            JSON.dump(arguments)
                        rescue StandardError
                            JSON.dump({})
                        end

                    model_name = task.model.name
                    # Workaround a bug in syskit, where deployment models are
                    # named weird
                    case model_name
                    when "Syskit::Deployment#"
                        model_name = "Deployments.#{task.arguments[:process_name]}"
                    when /^Deployments?::RubyTasks/
                        model_name = "Deployments.RubyTasks#{$'.gsub('::', '.')}"
                    end

                    {
                        model_id: add_model(task.model, name: model_name),
                        arguments: arguments_json
                    }
                end

                # @api private
                #
                # Add information about a Roby model
                #
                # @param [Class<Roby::Task>] model
                # @return [Integer] the record ID
                def add_model(model, name: model.name)
                    if (model_id = @registered_models[model.droby_id])
                        return model_id
                    end

                    match = @models.where(name: name).pluck(:id).first
                    return @registered_models[model.droby_id] = match if match

                    @registered_models[model.droby_id] = @models.insert({ name: name })
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

                # Tests whether there is an event with this name
                def event_with_name?(name)
                    @event_propagations.with(name: name).exist?
                end

                def task_model_by_id(id)
                    entity = @models.by_pk(id).one
                    raise ArgumentError, "no task model with ID #{id}" unless entity

                    Accessors::TaskModel.new(self, entity.name, entity.id)
                end

                def task_by_id(id)
                    entity = @tasks.by_pk(id).one
                    raise ArgumentError, "no task with ID #{id}" unless entity

                    task_model = task_model_by_id(entity.model_id)
                    Accessors::Task.new(self, entity, task_model)
                end

                # (see Dataset#find_all_streams)
                def find_all_streams(query)
                    @dataset.streams.find_all_streams(query)
                end
            end
        end
    end
end
