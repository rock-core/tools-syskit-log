# frozen_string_literal: true

require "test_helper"
module Syskit::Log
    module Models
        describe ReplayTaskContext do
            subject { Syskit::Log::ReplayTaskContext }
            describe "#model_for" do
                attr_reader :task_m, :replay_task_m
                before do
                    @task_m = Syskit::TaskContext.new_submodel
                    @replay_task_m = subject.model_for(task_m.orogen_model)
                end

                it "returns an existing model" do
                    assert_same replay_task_m, subject.model_for(task_m.orogen_model)
                end
                it "automatically creates a new model" do
                    refute_nil replay_task_m
                    assert_same task_m.orogen_model, replay_task_m.orogen_model
                end
                it "sets the new model's name and registers it under OroGen::Pocolog" do
                    task_m = Syskit::TaskContext.new_submodel(
                        orogen_model_name: "project::Task"
                    )
                    replay_task_m = subject.model_for(task_m.orogen_model)
                    assert_same ::OroGen::Pocolog.project.Task, replay_task_m
                    assert_equal "OroGen::Pocolog.project.Task", replay_task_m.name
                end
            end
            describe "#setup_submodel" do
                attr_reader :task_m
                before do
                    @task_m = Syskit::TaskContext.new_submodel
                end
                it "copies the data services from the plain task model" do
                    srv_m = Syskit::DataService.new_submodel
                    task_m.provides srv_m, as: "test"
                    replay_task_m = subject.model_for(task_m.orogen_model)
                    srv = replay_task_m.test_srv
                    refute_nil srv
                    assert_same srv_m, srv.model
                    refute_same srv, task_m.test_srv
                end
                it "copies the dynamic data services from the plain task model" do
                    srv_m = Syskit::DataService.new_submodel do
                        output_port "out", "/double"
                    end
                    task_m = Syskit::TaskContext.new_submodel do
                        dynamic_output_port(/^out_\w+$/, "/double")
                    end
                    task_m.dynamic_service srv_m, as: "test" do
                        provides srv_m, as: name, "out" => "out_#{name}"
                    end

                    replay_task_m = subject.model_for(task_m.orogen_model)
                    replay_task_m = replay_task_m.specialize
                    replay_task_m.require_dynamic_service "test", as: "dyn"
                    srv = replay_task_m.dyn_srv
                    assert_equal srv.out_port.to_component_port, replay_task_m.out_dyn_port
                end
            end

            describe "#fullfills?" do
                it "fullfills the plain task model" do
                    task_m = Syskit::TaskContext.new_submodel
                    replay_task_m = subject.model_for(task_m.orogen_model)
                    assert replay_task_m.fullfills?(task_m)
                end
            end
        end
    end
end
