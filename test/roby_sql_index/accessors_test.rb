# frozen_string_literal: true

require "test_helper"
require "iruby"

module Syskit
    module Log
        module RobySQLIndex
            describe Accessors do
                before do
                    @index = Index.create(logfile_pathname("roby.sql"))
                    @index.add_roby_log(roby_log_path("accessors"))
                    @root = Accessors::Root.new(@index)
                end

                describe "the root" do
                    it "raises for a name that is not a known namespace" do
                        e = assert_raises(NoMethodError) do
                            @root.does_not_exist
                        end
                        assert_equal :does_not_exist, e.name
                    end

                    it "gives access to a namespace" do
                        assert @root.Namespace
                    end

                    it "enumerates all event propagations" do
                        c = EVENT_PROPAGATION_CALL
                        e = EVENT_PROPAGATION_EMIT
                        expected = [
                            ["start", c], ["start", e],
                            ["start", c], ["start", e],
                            ["stop", c], ["failed", c], ["failed", e], ["stop", e],
                            ["stop", c], ["failed", c], ["failed", e], ["stop", e]
                        ]

                        propagations = @root.each_event_propagation
                                            .map { |p| [p.name, p.kind] }
                        assert_equal expected, propagations
                    end

                    it "enumerates its task models" do
                        models = @root.each_task_model.to_a
                        assert_equal(
                            [@root.Namespace.M, @root.Namespace.M.Submodel],
                            models.sort_by(&:name)
                        )
                    end

                    it "displays the list of models children of self in to_iruby" do
                        mime, body = @root.to_iruby
                        assert_equal "text/html", mime
                        assert_equal read_snapshot("root_to_iruby"), body
                    end
                end

                describe "a namespace" do
                    before do
                        @namespace = @root.Namespace
                    end

                    it "raises for a name that is not a known namespace" do
                        e = assert_raises(NoMethodError) do
                            @namespace.does_not_exist
                        end
                        assert_equal :does_not_exist, e.name
                    end

                    it "gives access to a task model" do
                        assert @namespace.M
                    end

                    it "displays the list of models children of self in to_iruby" do
                        mime, body = @namespace.to_iruby
                        assert_equal "text/html", mime
                        assert_equal read_snapshot("namespace_to_iruby"), body
                    end
                end

                describe "a task model" do
                    before do
                        @task_model = @root.Namespace.M
                    end

                    it "raises for a name that is not a known namespace" do
                        e = assert_raises(NoMethodError) do
                            @task_model.does_not_exist
                        end
                        assert_equal :does_not_exist, e.name
                    end

                    it "gives access to a task model" do
                        assert @task_model.Submodel
                    end

                    it "raises if trying to access an event that does not exist" do
                        e = assert_raises(NoMethodError) do
                            @task_model.not_an_event
                        end
                        assert_equal :not_an_event, e.name, e.message
                    end

                    it "gives access to task instances" do
                        tasks = @task_model.each_task.to_a
                        assert_equal 1, tasks.size

                        t = tasks.first
                        assert_equal @task_model, t.model
                    end

                    it "resolves tasks by their ID" do
                        id = @task_model.each_task.first.id
                        task = @task_model.task_by_id(id)
                        assert_equal id, task.id
                        assert_equal @task_model, task.model
                    end

                    it "raises ArgumentError if the task does not exist" do
                        e = assert_raises(ArgumentError) do
                            @task_model.task_by_id(-1)
                        end
                        assert_match(/no task with ID -1 and model Namespace::M/,
                                     e.message)
                    end

                    it "raises ArgumentError if a task does exist with the ID "\
                       "but for a different model" do
                        id = @root.Namespace.M.Submodel.each_task.first.id
                        e = assert_raises(ArgumentError) do
                            @task_model.task_by_id(id)
                        end
                        assert_match(/no task with ID #{id} and model Namespace::M/,
                                     e.message)
                    end

                    it "gives access to an existing event" do
                        ev = @task_model.start_event
                        assert_equal "start", ev.name
                        assert_equal @task_model, ev.task_model
                    end

                    it "allows to enumerate all events of the task model" do
                        events = @task_model.each_event.to_a
                        assert_equal Set["start", "failed", "stop"],
                                     events.map(&:name).to_set
                        events.each do |ev|
                            assert_equal @task_model, ev.task_model
                        end
                    end

                    it "can be built by ID directly from the index" do
                        assert_equal @task_model, @index.task_model_by_id(@task_model.id)
                    end

                    it "raises if the model ID does not exist" do
                        e = assert_raises(ArgumentError) { @index.task_model_by_id(-1) }
                        assert_equal "no task model with ID -1", e.message
                    end

                    it "enumerates the event propagation from all events in this model" do
                        p = @task_model.each_event_propagation.to_a
                        assert_equal %w[start start stop failed failed stop],
                                     p.map(&:name)

                        call = EVENT_PROPAGATION_CALL
                        emit = EVENT_PROPAGATION_EMIT
                        assert_equal([call, emit, call, call, emit, emit], p.map(&:kind))

                        assert_equal [@task_model], p.map { |obj| obj.task.model }.uniq
                    end
                end

                describe "an event model" do
                    before do
                        @event_model = @root.Namespace.M.start_event
                    end

                    it "builds its full name" do
                        assert_equal "Namespace::M.start_event", @event_model.full_name
                    end

                    it "allows to enumerate its emissions" do
                        emissions = @event_model.each_emission.to_a
                        assert_equal 1, emissions.size

                        ev = emissions.first
                        assert_equal "start", ev.name
                        assert_equal @event_model, ev.model
                        assert_equal ev.model.task_model.each_task.first,
                                     ev.task
                    end

                    it "enumerates the event propagations from this event model" do
                        p = @event_model.each_event_propagation.to_a
                        assert_equal %w[start start], p.map(&:name)

                        call = EVENT_PROPAGATION_CALL
                        emit = EVENT_PROPAGATION_EMIT
                        assert_equal([call, emit], p.map(&:kind))

                        assert_equal [@event_model], p.map(&:model).uniq
                    end

                    it "returns nil if there is no matching first propagation" do
                        p = @event_model.first_event_propagation(name: "does_not_exist")
                        assert_nil p
                    end

                    it "returns nil if there is no matching last propagation" do
                        p = @event_model.last_event_propagation(name: "does_not_exist")
                        assert_nil p
                    end

                    it "returns the first propagation of a given event model" do
                        p = @event_model.first_event_propagation
                        assert_kind_of Accessors::EventPropagation, p
                        assert_equal "start", p.name
                        assert_equal(EVENT_PROPAGATION_CALL, p.kind)
                    end

                    it "returns the last propagation of a given event model" do
                        p = @event_model.last_event_propagation
                        assert_kind_of Accessors::EventPropagation, p
                        assert_equal "start", p.name
                        assert_equal(EVENT_PROPAGATION_EMIT, p.kind)
                    end

                    it "returns the first emission of a given event model" do
                        p = @event_model.first_emission
                        assert_kind_of Accessors::EventPropagation, p
                        assert_equal "start", p.name
                        assert_equal(EVENT_PROPAGATION_EMIT, p.kind)
                    end

                    it "returns the last emission of a given event model" do
                        p = @event_model.last_emission
                        assert_kind_of Accessors::EventPropagation, p
                        assert_equal "start", p.name
                        assert_equal(EVENT_PROPAGATION_EMIT, p.kind)
                    end
                end

                describe "a task" do
                    before do
                        @task = @root.Namespace.M.each_task.first
                    end

                    it "gives access to the task's arguments" do
                        assert_equal({ arg: 10 }, @task.arguments)
                    end

                    it "allows to enumerate its emissions" do
                        emissions = @task.each_emission.to_a
                        assert_equal 3, emissions.size
                        assert_equal %w[start failed stop], emissions.map(&:name)
                        assert_equal [@task, @task, @task], emissions.map(&:task)
                    end

                    it "returns the first event propagation" do
                        p = @task.first_event_propagation
                        assert_kind_of Accessors::EventPropagation, p
                        assert_equal "start", p.name
                        assert_equal(EVENT_PROPAGATION_CALL, p.kind)
                    end

                    it "returns the last event propagation" do
                        p = @task.last_event_propagation
                        assert_kind_of Accessors::EventPropagation, p
                        assert_equal "stop", p.name
                        assert_equal(EVENT_PROPAGATION_EMIT, p.kind)
                    end

                    it "returns the first event emission" do
                        p = @task.first_emission
                        assert_kind_of Accessors::EventPropagation, p
                        assert_equal "start", p.name
                        assert_equal(EVENT_PROPAGATION_EMIT, p.kind)
                    end

                    it "returns the last event emission" do
                        p = @task.last_emission
                        assert_kind_of Accessors::EventPropagation, p
                        assert_equal "stop", p.name
                        assert_equal(EVENT_PROPAGATION_EMIT, p.kind)
                    end

                    it "gives access to a specific bound event" do
                        emissions = []
                        @task.start_event.each_emission do |e|
                            emissions << e
                        end

                        assert_equal 1, emissions.size

                        ev = emissions.first
                        assert_equal "start", ev.name
                        assert_equal @task, ev.task
                    end

                    it "gives access to a specific bound event" do
                        emissions = @task.start_event.each_emission.to_a
                        assert_equal 1, emissions.size

                        ev = emissions.first
                        assert_equal "start", ev.name
                        assert_equal @task, ev.task
                    end

                    it "can be built by ID directly from the index" do
                        task = @index.task_by_id(@task.id)
                        assert_equal @task, task
                        assert_equal @root.Namespace.M, task.model
                    end

                    it "raises if the ID does not exist" do
                        e = assert_raises(ArgumentError) { @index.task_by_id(-1) }
                        assert_equal "no task with ID -1", e.message
                    end

                    describe "tasks without a stop in a truncated log file" do
                        it "returns the end time of the log itself "\
                           "as the end time of the task" do
                            index = Index.create(logfile_pathname("truncated-roby.sql"))
                            index.add_roby_log(roby_log_path("event_emission_truncated"))
                            task = index.task_by_id(index.tasks.one!.id)
                            assert_equal index.time_end, task.stop_time
                        end
                    end
                end

                describe "an event" do
                    before do
                        @event_model = @root.Namespace.M.start_event
                    end

                    it "allows to enumerate the event's emissions" do
                        emissions = @event_model.each_emission.to_a
                        assert_equal 1, emissions.size

                        ev = emissions.first
                        assert_equal "start", ev.name
                        assert_equal @event_model, ev.model
                        assert_equal ev.model.task_model.each_task.first,
                                     ev.task
                    end

                    it "returns the first emission" do
                    end

                    it "returns the last emission" do
                    end
                end

                describe "the deployments" do
                    it "gives access through the OroGen.Deployments namespace" do
                        Roby.app.plugins_enabled = true
                        index = Index.create(logfile_pathname("roby2.sql"))
                        index.add_roby_log(roby_log_path("deployments"))
                        root = Accessors::Root.new(index)
                        task_model = root.Deployments.RubyTasks.T
                        assert_equal "Deployments.RubyTasks.T", task_model.name
                    end
                end

                describe "the relationship with data streams" do
                    before do
                        @datastore, @dataset =
                            prepare_fixture_datastore "dsl_orogen_accessors"
                    end

                    it "gives access to the port stream through the port model" do
                        task_m = @dataset.roby.OroGen.orogen_syskit_tests.Echo.out_port
                        samples = task_m.streams.echo_task.out_port.samples.to_a

                        assert samples[0].last < 1000
                        assert samples[-1].last > 1000
                    end

                    it "restricts the port streams for a given task instance" do
                        task_m = @dataset.roby.OroGen.orogen_syskit_tests.Echo
                        tasks = task_m.each_task.to_a
                        assert_equal 2, tasks.size

                        all_samples =
                            task_m.out_port.streams.echo_task.out_port.samples.to_a
                        samples0 = tasks.first.out_port.samples.to_a
                        samples1 = tasks.last.out_port.samples.to_a
                        assert samples0[0].last < 1000
                        assert samples0[-1].last < 1000
                        assert samples1[0].last > 1000
                        assert samples1[-1].last > 1000
                        assert_equal all_samples, (samples0 + samples1)
                    end

                    it "allows to get a port either form its model or from the task" do
                        task_m = @dataset.roby.OroGen.orogen_syskit_tests.Echo
                        port_from_model = task_m.out_port.each_port.first
                        port_from_task = task_m.each_task.first.out_port

                        assert_equal port_from_model, port_from_task
                    end
                end

                def read_snapshot(name)
                    Pathname.new(__dir__).join("#{name}.snapshot").read
                            .gsub(/\s/, "")
                end
            end
        end
    end
end
