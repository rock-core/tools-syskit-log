# frozen_string_literal: true

require "test_helper"

module Syskit::Log
    describe Deployment do
        attr_reader :replay_manager
        attr_reader :replay_manager, :streams, :port_stream, :task_m, :deployment_m
        attr_reader :subject
        before do
            double_t = Roby.app.default_loader.registry.get "/double"

            create_logfile "test.0.log" do
                stream0 = create_logfile_stream(
                    "/port0",
                    type: double_t,
                    metadata: { "rock_task_name" => "task",
                                "rock_task_object_name" => "out",
                                "rock_stream_type" => "port" }
                )
                stream1 = create_logfile_stream(
                    "/port1",
                    type: double_t,
                    metadata: { "rock_task_name" => "task",
                                "rock_task_object_name" => "other_out",
                                "rock_stream_type" => "port" }
                )
                stream0.write Time.at(0), Time.at(0), 0
                stream1.write Time.at(1), Time.at(1), 1
            end
            @streams = load_logfiles_as_stream.find_task_by_name("task")
            @port_stream = streams.find_port_by_name("out")
            @task_m = Syskit::TaskContext.new_submodel do
                output_port "out", double_t
            end
            @replay_manager = execution_engine.pocolog_replay_manager

            @deployment_m = Syskit::Log::Deployment.for_streams(
                streams, model: task_m, name: "task"
            )
            plan.add_permanent_task(
                @subject = deployment_m.new(process_name: "test", on: "pocolog")
            )
        end

        def self.common_behavior # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
            it "gets notified of new samples when running" do
                expect_execution { subject.start! }
                    .to { emit subject.ready_event }
                flexmock(subject)
                    .should_receive(:process_sample)
                    .with(port_stream, Time.at(0), 0)
                    .once
                replay_manager.step
            end

            it "does nothing if the streams are eof?" do
                expect_execution { subject.start! }
                    .to { emit subject.ready_event }
                replay_manager.step
                replay_manager.step
            end

            describe "dynamic stream addition and removal" do
                attr_reader :other_deployment
                before do
                    expect_execution { subject.start! }
                        .to { emit subject.ready_event }
                    replay_manager.step

                    other_task_m = Syskit::TaskContext.new_submodel do
                        output_port "other_out", "/double"
                    end
                    other_deployment_m = Syskit::Log::Deployment.for_streams(
                        streams, model: other_task_m, name: "task"
                    )
                    @other_deployment = other_deployment_m.new(
                        process_name: "other_test", on: "pocolog"
                    )
                    plan.add_permanent_task(other_deployment)
                end

                it "does not skip a sample when eof? and a new stream is added to the alignment" do
                    replay_manager.step

                    expect_execution { other_deployment.start! }
                        .to { emit other_deployment.ready_event }
                    flexmock(other_deployment)
                        .should_receive(:process_sample)
                        .with(streams.find_port_by_name("other_out"), Time.at(1), 1)
                        .once
                    replay_manager.step
                end

                it "does not skip a sample when the current sample is from a stream that has been removed" do
                    expect_execution { other_deployment.start! }
                        .to { emit other_deployment.ready_event }
                    expect_execution { subject.stop! }
                        .to { emit subject.stop_event }
                    flexmock(other_deployment)
                        .should_receive(:process_sample)
                        .with(streams.find_port_by_name("other_out"), Time.at(1), 1)
                        .once
                    replay_manager.step
                end
            end

            it "does not get notified if pending" do
                flexmock(subject).should_receive(:process_sample).never
                replay_manager.step
            end

            it "does not get notified if stopped" do
                expect_execution { subject.start! }.to { emit subject.ready_event }
                expect_execution { subject.stop! }.to { emit subject.stop_event }
                flexmock(subject).should_receive(:process_sample).never
                replay_manager.step
            end

            it "forwards the samples to an existing, running, deployed task" do
                plan.add(task = subject.task("task"))
                syskit_configure_and_start(task)
                reader = allow_blocking_calls { task.orocos_task.out.reader }
                subject.process_sample(port_stream, Time.now, 1)
                sample = expect_execution.to { have_one_new_sample reader }
                assert_equal 1, sample
            end

            it "does not forward the samples to a configured task" do
                plan.add(task = subject.task("task"))
                syskit_configure(task)
                flexmock(task.orocos_task.out).should_receive(:write).never
                subject.process_sample(port_stream, Time.now, 1)
            end

            it "does not forward the samples to a finished task" do
                plan.add(task = subject.task("task"))
                syskit_configure_and_start(task)
                expect_execution { task.stop! }
                    .to { emit task.stop_event }

                flexmock(task.orocos_task.out).should_receive(:write).never
                subject.process_sample(port_stream, Time.now, 1)
            end
        end

        describe "from_dir" do
            def load_logfiles_as_stream
                Streams.from_dir(logfile_pathname)
            end

            common_behavior
        end

        describe "from_dataset" do
            def load_logfiles_as_stream
                _, dataset = import_logfiles
                Streams.from_dataset(dataset).find_task_by_name("task")
            end

            common_behavior
        end

        def allow_blocking_calls(&block)
            if defined?(Runkit)
                Runkit.allow_blocking_calls(&block)
            else
                Orocos.allow_blocking_calls(&block)
            end
        end
    end
end
