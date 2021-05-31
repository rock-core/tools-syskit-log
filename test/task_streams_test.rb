# frozen_string_literal: true

require "test_helper"

module Syskit::Log
    describe TaskStreams do
        attr_reader :subject
        before do
            create_logfile "test.0.log" do
                create_logfile_stream "/port0",
                                      metadata: Hash["rock_task_name" => "task",
                                                     "rock_task_object_name" => "object0",
                                                     "rock_stream_type" => "port"]
                create_logfile_stream "/port1_1",
                                      metadata: Hash["rock_task_name" => "task",
                                                     "rock_task_object_name" => "object1",
                                                     "rock_stream_type" => "port"]
                create_logfile_stream "/port1_2",
                                      metadata: Hash["rock_task_name" => "task",
                                                     "rock_task_object_name" => "object1",
                                                     "rock_stream_type" => "port"]
                create_logfile_stream "/property0",
                                      metadata: Hash["rock_task_name" => "task",
                                                     "rock_task_object_name" => "object0",
                                                     "rock_stream_type" => "property"]
                create_logfile_stream "/property1_1",
                                      metadata: Hash["rock_task_name" => "task",
                                                     "rock_task_object_name" => "object1",
                                                     "rock_stream_type" => "property"]
                create_logfile_stream "/property1_2",
                                      metadata: Hash["rock_task_name" => "task",
                                                     "rock_task_object_name" => "object1",
                                                     "rock_stream_type" => "property"]
            end
            streams = Streams.from_dir(logfile_pathname)
            @subject = streams.find_task_by_name("task")
        end

        describe "#find_port_by_name" do
            it "returns nil if there are no matches" do
                assert !subject.find_port_by_name("does_not_exist")
            end
            it "returns the matching port stream" do
                object = subject.find_port_by_name("object0")
                assert_kind_of ::Pocolog::DataStream, object
                assert_equal "/port0", object.name
            end
            it "raises Ambiguous if there are more than one port with the given name" do
                assert_raises(Ambiguous) do
                    subject.find_port_by_name("object1")
                end
            end

            describe "access through #method_missing" do
                it "returns a single match if there is one" do
                    assert_equal "/port0", subject.object0_port.name
                end
                it "raises Ambiguous for multiple matches" do
                    assert_raises(Ambiguous) do
                        subject.object1_port
                    end
                end
                it "raises NoMethodError if there are no matches" do
                    assert_raises(NoMethodError) do
                        subject.does_not_exist_port
                    end
                end
            end
        end

        describe "#find_property_by_name" do
            it "returns nil if there are no matches" do
                assert !subject.find_property_by_name("does_not_exist")
            end
            it "returns the matching port stream" do
                object = subject.find_property_by_name("object0")
                assert_kind_of ::Pocolog::DataStream, object
                assert_equal "/property0", object.name
            end
            it "raises Ambiguous if there are more than one port with the given name" do
                assert_raises(Ambiguous) do
                    subject.find_property_by_name("object1")
                end
            end

            describe "access through #method_missing" do
                it "returns a single match if there is one" do
                    assert_equal "/property0", subject.object0_property.name
                end
                it "raises Ambiguous for multiple matches" do
                    assert_raises(Ambiguous) do
                        subject.object1_property
                    end
                end
                it "raises NoMethodError if there are no matches" do
                    assert_raises(NoMethodError) do
                        subject.does_not_exist_property
                    end
                end
            end
        end

        describe "#orogen_model_name" do
            describe "no model declared at all" do
                it "raises Unknown if none is declared in the streams" do
                    assert_raises(Unknown) do
                        subject.orogen_model_name
                    end
                end
                it "raises Unknown if the streams are empty" do
                    assert_raises(Unknown) do
                        TaskStreams.new([]).orogen_model_name
                    end
                end
            end

            describe "models are declared" do
                before do
                    subject.streams.each do |s|
                        s.metadata["rock_task_model"] = "orogen::Model"
                    end
                end

                it "raises Unknown if some streams do not have a declared model" do
                    subject.streams.first.metadata.delete("rock_task_model")
                    assert_raises(Unknown) do
                        subject.orogen_model_name
                    end
                end
                it "raises Ambiguous if the streams declare multiple models" do
                    subject.streams.first.metadata["rock_task_model"] = "orogen::AnotherModel"
                    assert_raises(Ambiguous) do
                        subject.orogen_model_name
                    end
                end
                it "returns the model if there is only one" do
                    assert_equal "orogen::Model", subject.orogen_model_name
                end

                describe "#model" do
                    it "returns the resolved model" do
                        task_m = Syskit::TaskContext.new_submodel(name: "orogen::Model")
                        flexmock(task_m.orogen_model).should_receive(:name).and_return("orogen::Model")
                        assert_equal task_m, subject.model
                    end
                    it "raises Unknown if the model cannot be resolved" do
                        assert_raises(Unknown) do
                            subject.model
                        end
                    end
                end
            end
        end

        describe "#each_port_stream" do
            it "enumerates the streams that are a task's port" do
                ports = subject.each_port_stream
                               .map { |name, stream| [name, stream.name] }.to_set
                expected = Set[
                    ["object0", "/port0"],
                    ["object1", "/port1_1"],
                    ["object1", "/port1_2"]]
                assert_equal expected, ports
            end
        end

        describe "#each_property_stream" do
            it "enumerates the streams that are a task's property" do
                ports = subject.each_property_stream
                               .map { |name, stream| [name, stream.name] }.to_set
                expected = Set[
                    ["object0", "/property0"],
                    ["object1", "/property1_1"],
                    ["object1", "/property1_2"]]
                assert_equal expected, ports
            end
        end
    end
end
