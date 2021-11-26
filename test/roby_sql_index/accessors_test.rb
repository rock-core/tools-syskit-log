# frozen_string_literal: true

require "test_helper"

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
                end
            end
        end
    end
end
