# frozen_string_literal: true

require "test_helper"

module Syskit::Log
    describe ReplayManager do
        attr_reader :subject, :streams, :port_stream, :task_m, :deployment_m
        before do
            double_t = Roby.app.default_loader.registry.get "/double"

            create_logfile "test.0.log" do
                create_logfile_stream(
                    "/port0",
                    type: double_t,
                    metadata: { "rock_task_name" => "task",
                                "rock_task_object_name" => "out",
                                "rock_stream_type" => "port" }
                )
            end
            @streams = Streams.from_dir(logfile_pathname)
                              .find_task_by_name("task")
            @port_stream = streams.find_port_by_name("out")
            @task_m = Syskit::TaskContext.new_submodel do
                output_port "out", double_t
            end

            plan = Roby::ExecutablePlan.new
            @subject = ReplayManager.new(plan.execution_engine)

            @deployment_m = Syskit::Log::Deployment
                            .for_streams(streams, model: task_m, name: "task")
        end

        describe "#register" do
            it "registers the stream-to-deployment mapping" do
                deployment_task = deployment_m.new
                subject.register(deployment_task)
                other_task = deployment_m.new
                subject.register(other_task)
                assert_equal [deployment_task, other_task],
                             subject.find_deployments_of_stream(port_stream)
            end
            it "deregisters the stream if no deployment tasks are still using it" do
                deployment_task = deployment_m.new
                subject.register(deployment_task)
                assert_equal [deployment_task],
                             subject.find_deployments_of_stream(port_stream)
            end
            it "aligns the streams that are managed by the deployment task" do
                deployment_task = deployment_m.new
                flexmock(subject.stream_aligner)
                    .should_receive(:add_streams)
                    .with(streams.find_port_by_name("out"))
                    .once
                subject.register(deployment_task)
            end
            it "does not realign the streams that are already in-use by another deployment" do
                deployment_task = deployment_m.new
                subject.register(deployment_task)

                flexmock(subject.stream_aligner)
                    .should_receive(:add_streams)
                    .with_no_args
                    .once.pass_thru
                other_task = deployment_m.new
                subject.register(other_task)
            end
        end

        describe "#deregister" do
            it "removes the streams that are managed by the deployment task from the aligner" do
                deployment_task = deployment_m.new
                subject.register(deployment_task)
                flexmock(subject.stream_aligner)
                    .should_receive(:remove_streams)
                    .with(streams.find_port_by_name("out"))
                    .once
                subject.deregister(deployment_task)
            end
            it "does not deregister streams that are still in use by another deployment" do
                deployment_task = deployment_m.new
                subject.register(deployment_task)
                other_task = deployment_m.new
                subject.register(other_task)

                flexmock(subject.stream_aligner)
                    .should_receive(:remove_streams)
                    .with().once.pass_thru
                subject.deregister(other_task)
            end
            it "deregisters the deployment task from the targets for the stream" do
                deployment_task = deployment_m.new
                subject.register(deployment_task)
                other_task = deployment_m.new
                subject.register(other_task)
                subject.deregister(other_task)
                assert_equal [deployment_task],
                             subject.find_deployments_of_stream(port_stream)
            end
            it "deregisters the stream if no deployment tasks are still using it" do
                deployment_task = deployment_m.new
                subject.register(deployment_task)
                subject.deregister(deployment_task)
                assert_equal [], subject.find_deployments_of_stream(port_stream)
            end
        end

        describe "realtime replay" do
            it "raises RuntimeError in #start if it is already running" do
                subject.start
                assert_raises(RuntimeError) { subject.start }
            end

            it "is running after a call to #start" do
                subject.start
                assert subject.running?
            end

            it "raises RuntimeError if #stop is called while it is not running" do
                assert_raises(RuntimeError) { subject.stop }
            end

            it "is not running after a call to #stop" do
                subject.start
                subject.stop
                assert !subject.running?
            end

            it "installs a handler that calls #process_in_realtime once in each cycle" do
                subject.execution_engine.execute_one_cycle
                subject.start(replay_speed: 10)
                flexmock(subject).should_receive(:process_in_realtime).twice.with(10)
                subject.execution_engine.execute_one_cycle
                subject.execution_engine.execute_one_cycle
                subject.stop
                subject.execution_engine.execute_one_cycle
            end
        end

        describe "#process_in_realtime" do
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
                    stream0.write Time.at(0), Time.at(0), 0
                    stream0.write Time.at(0), Time.at(1), 1
                    stream0.write Time.at(0), Time.at(2), 2
                end

                streams = Streams.from_dir(logfile_pathname)
                                 .find_task_by_name("task")
                task_m = Syskit::TaskContext.new_submodel do
                    output_port "out", double_t
                end

                deployment_m = Syskit::Log::Deployment
                               .for_streams(streams, model: task_m, name: "task")
                deployment = deployment_m.new(process_name: "test", on: "pocolog")
                plan.add_permanent_task(deployment)
                expect_execution { deployment.start! }
                    .to { emit deployment.ready_event }

                @subject = plan.execution_engine.pocolog_replay_manager
                subject.reset_replay_base_times
                flexmock(subject)
            end

            it "plays as many samples as required to match real and expected logical time" do
                subject.should_receive(:sleep)
                realtime = subject.base_real_time
                flexmock(Time).should_receive(:now).and_return { realtime }
                subject.should_receive(:dispatch).once.with(0, Time.at(0))
                       .and_return { realtime += 1 }
                subject.should_receive(:dispatch).once.with(0, Time.at(1))
                       .and_return { realtime += 1 }
                subject.process_in_realtime(1, limit_real_time: realtime + 1.1)
            end

            it "applies the speed to the sample limit" do
                subject.should_receive(:sleep)
                realtime = subject.base_real_time
                flexmock(Time).should_receive(:now).and_return { realtime }
                subject.should_receive(:dispatch).once.with(0, Time.at(0))
                       .and_return { realtime += 1 }
                subject.should_receive(:dispatch).once.with(0, Time.at(1))
                       .and_return { realtime += 1 }
                subject.process_in_realtime(2, limit_real_time: realtime + 0.55)
            end

            it "returns true if there are samples left to play" do
                subject.should_receive(:sleep)
                realtime = subject.base_real_time
                flexmock(Time).should_receive(:now).and_return { realtime }
                assert subject.process_in_realtime(1, limit_real_time: realtime + 1.1)
            end

            it "returns false on eof" do
                subject.should_receive(:sleep)
                realtime = subject.base_real_time
                flexmock(Time).should_receive(:now).and_return { realtime }
                assert !subject.process_in_realtime(1, limit_real_time: realtime + 2.1)
            end

            it "sleeps between samples" do
                realtime = subject.base_real_time
                flexmock(Time).should_receive(:now).and_return { realtime }
                subject.should_receive(:dispatch).with(0, Time.at(0)).globally.ordered
                subject.should_receive(:sleep).explicitly.with(1).once.globally.ordered
                       .and_return { realtime += 1 }
                subject.should_receive(:dispatch).with(0, Time.at(1)).globally.ordered
                       .and_return { realtime += 1 }
                subject.process_in_realtime(1, limit_real_time: realtime + 1.1)
            end

            it "applies the replay speed to the sleeping times" do
                realtime = subject.base_real_time
                flexmock(Time).should_receive(:now).and_return { realtime }
                subject.should_receive(:dispatch)
                subject.should_receive(:sleep).explicitly.with(0.5).once.globally.ordered
                       .and_return { realtime += 0.5 }
                subject.process_in_realtime(2, limit_real_time: realtime + 0.55)
            end

            it "does not sleep if the required sleep time is below MIN_TIME_DIFF_TO_SLEEP" do
                realtime = subject.base_real_time
                flexmock(Time).should_receive(:now).and_return { realtime }
                subject.should_receive(:dispatch)
                subject.should_receive(:sleep).explicitly.never
                subject.process_in_realtime(1000, limit_real_time: realtime + 0.0011)
            end
        end
    end
end
