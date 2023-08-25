# frozen_string_literal: true

require "test_helper"

module Syskit::Log
    describe RockStreamMatcher do
        attr_reader :streams
        before do
            double_t = Typelib::Registry.new.create_numeric "/double", 8, :float
            create_logfile "test.0.log" do
                create_logfile_stream "task.port", type: double_t, metadata: Hash[rock_stream_type: "port", rock_task_object_name: "port", rock_task_name: "task"]
                create_logfile_stream "task.property", metadata: Hash[rock_stream_type: "property", rock_task_object_name: "property", rock_task_name: "task"]
                create_logfile_stream "stream_without_properties"
                create_logfile_stream "other_task.port", metadata: Hash[rock_task_name: "other_task"]
                create_logfile_stream "stream_with_task_model", metadata: Hash[rock_task_model: "orogen_model::Test"]
            end

            @streams = Streams.new
            streams.add_file logfile_pathname("test.0.log")
        end
        subject { RockStreamMatcher.new }

        def compress?
            false
        end

        def assert_finds_streams(query, *stream_names)
            assert_equal stream_names, streams.find_all_streams(query).map(&:name)
        end

        describe "matching the stream type" do
            it "matches against ports" do
                assert_finds_streams subject.ports, "task.port"
            end
            it "matches against properties" do
                assert_finds_streams subject.properties, "task.property"
            end
            it "never matches streams that do not have the property" do
            end
            it "ORs ports and properties if both are specified" do
                assert_finds_streams subject.ports.properties, "task.port", "task.property"
            end
        end

        describe "matching the task name" do
            it "matches tasks that have the name" do
                assert_finds_streams subject.task_name("task"), "task.port", "task.property"
            end
            it "ORs the different names" do
                assert_finds_streams subject.task_name("task").task_name("other_task"), "task.port", "task.property", "other_task.port"
            end
        end

        describe "matching the object name" do
            it "matches objects that have the name" do
                assert_finds_streams subject.object_name("port"), "task.port"
            end
            it "ORs the different names" do
                assert_finds_streams subject.object_name("port").object_name("property"), "task.port", "task.property"
            end
        end

        describe "matching the task model" do
            it "matches the task model by name" do
                task_m = Syskit::TaskContext.new_submodel
                flexmock(task_m.orogen_model, :strict, name: "orogen_model::Test")
                assert_finds_streams subject.task_model(task_m), "stream_with_task_model"
            end
        end

        describe "matching the type" do
            it "matches the type by name" do
                assert_finds_streams subject.type(/double/), "task.port"
            end
        end
    end
end
