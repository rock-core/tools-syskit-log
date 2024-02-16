# frozen_string_literal: true

require "test_helper"

module Syskit::Log
    module Models
        describe Deployment do
            attr_reader :streams

            before do
                double_t = Roby.app.default_loader.registry.get "/double"
                mismatch_t = Typelib::Registry.new.create_numeric "/double", 4, :sint

                create_logfile "test.0.log" do
                    create_logfile_stream "/port0", type: double_t,
                                                    metadata: Hash["rock_task_name" => "task",
                                                                   "rock_task_object_name" => "object0",
                                                                   "rock_stream_type" => "port"]
                    create_logfile_stream "/port1", type: mismatch_t,
                                                    metadata: Hash["rock_task_name" => "task_with_mismatching_type",
                                                                   "rock_task_object_name" => "port_with_mismatching_type",
                                                                   "rock_stream_type" => "port"]
                end

                Syskit::Log.logger.level = Logger::FATAL
            end

            after do
                Syskit::Log.logger.level = Logger::WARN
            end

            def self.common_behavior # rubocop:disable Metrics/AbcSize,Metrics/MethodLength
                describe "#add_stream" do
                    describe "error cases" do
                        attr_reader :deployment_m, :task_m
                        before do
                            @task_m = task_m = Syskit::TaskContext.new_submodel do
                                input_port "in", "/double"
                                output_port "out", "/double"
                            end
                            @deployment_m =
                                Syskit::Log::Deployment
                                .for_streams(TaskStreams.new, model: task_m, name: "task")
                        end

                        it "raises ArgumentError if the port is not a port "\
                           "of the deployment's task model" do
                            other_task_m = Syskit::TaskContext.new_submodel do
                                output_port "out", "/double"
                            end
                            assert_raises(ArgumentError) do
                                deployment_m.add_stream(
                                    streams.find_task_by_name("object0"),
                                    other_task_m.out_port
                                )
                            end
                        end
                        it "raises MismatchingType if the stream and port "\
                           "have different types" do
                            streams = @all_streams.task_with_mismatching_type_task
                            replay_task_m =
                                Syskit::Log::ReplayTaskContext
                                .model_for(task_m.orogen_model)
                            assert_raises(MismatchingType) do
                                deployment_m.add_stream(
                                    streams.find_port_by_name("port_with_mismatching_type"),
                                    replay_task_m.out_port
                                )
                            end
                        end
                        it "raises ArgumentError if the port is an input port" do
                            assert_raises(ArgumentError) do
                                deployment_m.add_stream(streams.find_task_by_name("object0"), task_m.in_port)
                            end
                        end
                    end

                    it "uses the task's port with the same name by default" do
                        task_m = Syskit::TaskContext.new_submodel do
                            output_port "object0", "/double"
                        end
                        replay_task_m = Syskit::Log::ReplayTaskContext.model_for(task_m.orogen_model)
                        deployment_m = Syskit::Log::Deployment.for_streams(TaskStreams.new, model: task_m, name: "task")
                        deployment_m.add_stream(port_stream = streams.find_port_by_name("object0"))
                        assert_equal Hash[port_stream => replay_task_m.object0_port],
                                     deployment_m.streams_to_port
                    end
                end

                describe "#add_streams_from" do
                    it "issues a warning if allow_missing is true and some output ports do not have a matching stream" do
                        task_m = Syskit::TaskContext.new_submodel do
                            output_port "unknown_port", "/double"
                        end
                        deployment_m = Syskit::Log::Deployment.for_streams(TaskStreams.new, name: "test", model: task_m)
                        flexmock(Syskit::Log, :strict).should_receive(:warn).with(/state/).once
                        flexmock(Syskit::Log, :strict).should_receive(:warn).with(/unknown_port/).once
                        deployment_m.add_streams_from(streams, allow_missing: true)
                    end

                    it "raises MissingStream if allow_missing is false and some output ports do not have a matching stream" do
                        task_m = Syskit::TaskContext.new_submodel do
                            output_port "unknown_port", "/double"
                        end
                        deployment_m = Syskit::Log::Deployment.for_streams(TaskStreams.new, name: "test", model: task_m)
                        assert_raises(MissingStream) do
                            deployment_m.add_streams_from(streams, allow_missing: false)
                        end
                    end

                    it "adds the matching streams" do
                        task_m = Syskit::TaskContext.new_submodel do
                            output_port "object0", "/double"
                        end
                        replay_task_m = Syskit::Log::ReplayTaskContext.model_for(task_m.orogen_model)
                        deployment_m = Syskit::Log::Deployment.for_streams(TaskStreams.new, name: "test", model: task_m)
                        flexmock(deployment_m).should_receive(:add_stream)
                                              .with(streams.find_port_by_name("object0"), replay_task_m.object0_port)
                                              .once
                        deployment_m.add_streams_from(streams)
                    end
                end
            end

            describe "from_dir" do
                before do
                    @all_streams = Streams.from_dir(logfile_pathname)
                    @streams = @all_streams.find_task_by_name("task")
                end

                def compress?
                    false
                end

                common_behavior
            end

            describe "from_dataset" do
                before do
                    _, dataset = import_logfiles

                    @all_streams = Streams.from_dataset(dataset)
                    @streams = @all_streams.find_task_by_name("task")
                end

                common_behavior
            end
        end
    end
end
