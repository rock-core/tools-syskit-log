require 'test_helper'

module Syskit::Pocolog
    describe Extensions::Configuration do
        attr_reader :group, :streams, :double_t
        before do
            @group = Syskit::Models::DeploymentGroup.new
            double_t = Roby.app.default_loader.registry.get '/double'

            create_logfile 'test.0.log' do
                create_logfile_stream '/port0', type: double_t,
                    metadata: Hash['rock_task_name' => "task",
                                   'rock_task_object_name' => 'object0',
                                   'rock_task_model' => 'task::Model',
                                   'rock_stream_type' => 'port']
                create_logfile_stream '/port1_1', type: double_t,
                    metadata: Hash['rock_task_name' => "task",
                                   'rock_task_object_name' => 'object1',
                                   'rock_task_model' => 'task::Model',
                                   'rock_stream_type' => 'port']
                create_logfile_stream '/port1_2', type: double_t,
                    metadata: Hash['rock_task_name' => "task",
                                   'rock_task_object_name' => 'object1',
                                   'rock_task_model' => 'task::Model',
                                   'rock_stream_type' => 'port']
                create_logfile_stream '/property0', type: double_t,
                    metadata: Hash['rock_task_name' => "task",
                                   'rock_task_object_name' => 'object0',
                                   'rock_task_model' => 'task::Model',
                                   'rock_stream_type' => 'property']
                create_logfile_stream '/property1_1', type: double_t,
                    metadata: Hash['rock_task_name' => "task",
                                   'rock_task_object_name' => 'object1',
                                   'rock_task_model' => 'task::Model',
                                   'rock_stream_type' => 'property']
                create_logfile_stream '/property1_2', type: double_t,
                    metadata: Hash['rock_task_name' => "task",
                                   'rock_task_object_name' => 'object1',
                                   'rock_task_model' => 'task::Model',
                                   'rock_stream_type' => 'property']
            end
            streams = Streams.from_dir(logfile_path)
            @streams = streams.find_task_by_name('task')
        end

        describe "#use_pocolog_task" do
            it "registers the stream-to-port mappings for the matching ports on the deployment model" do
                task_m = Syskit::TaskContext.new_submodel
                deployment_m = Deployment.new_submodel
                flexmock(Syskit::Pocolog::Deployment).
                    should_receive(:for_streams).
                    with(streams, ->(h) { h[:model] == task_m && h[:name] == 'test' }).
                    and_return(mock = flexmock(deployment_m))

                configured_deployment = group.use_pocolog_task(streams, name: 'test', model: task_m, allow_missing: true)
                assert_equal mock, configured_deployment.model
            end

            # This really is a synthetic test
            it "allows for the deployment of a stream task" do
                task_m = Syskit::TaskContext.new_submodel(orogen_model_name: 'task::Model')
                req = task_m.to_instance_requirements.use_deployment_group(streams)
                syskit_deploy(req)
            end
        end
    end
end
