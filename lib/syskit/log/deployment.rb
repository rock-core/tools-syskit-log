# frozen_string_literal: true

module Syskit::Log
    # Task supporting the replay process
    #
    # To give some control over how the streams are aligned together (and
    # potentially allow optimizing the replay process), streams supporting a
    # given task are injected in the replay process when they are associated
    # with a deployment task, and removed when the deployment is removed.
    class Deployment < Syskit::Deployment
        extend Models::Deployment

        attr_reader :stream_to_port

        def initialize(**options)
            super
            @stream_to_port = {}
        end

        def deployed_model_by_orogen_model(orogen_model)
            ReplayTaskContext.model_for(orogen_model.task_model)
        end

        def log_port?(*)
            false
        end

        def replay_manager
            execution_engine.pocolog_replay_manager
        end

        on :start do |_context|
            replay_manager.register(self)
        end

        on :stop do |_context|
            replay_manager.deregister(self)
        end

        def added_execution_agent_parent(executed_task, _info)
            super
            executed_task.start_event.on do
                model.each_stream_mapping do |stream, (ops, model_port)|
                    orocos_port = model_port.bind(executed_task).to_orocos_port
                    unless orocos_port.name == "state"
                        stream_to_port[stream] =
                            [ops, orocos_port.new_sample, orocos_port]
                    end
                end
            end
            executed_task.stop_event.on do
                stream_to_port.clear
            end
        end

        def process_sample(stream, _time, sample)
            ops, upgraded_sample, orocos_port = stream_to_port[stream]
            return unless ops

            ops.call(upgraded_sample, sample)
            orocos_port.write(upgraded_sample)
        end
    end
end
