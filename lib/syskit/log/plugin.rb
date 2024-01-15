# frozen_string_literal: true

require "syskit"

module Syskit::Log
    module Plugin
        def self.setup(app)
            Pocolog.logger = Syskit::Log.logger
            manager =
                if defined?(Runkit)
                    Runkit::RubyTasks::ProcessManager.new(app.default_loader)
                else
                    Orocos::RubyTasks::ProcessManager.new(app.default_loader)
                end

            Syskit.conf.register_process_server("pocolog", manager, app.log_dir)
        end

        # This hooks into the network generation to deploy all tasks using
        # replay streams
        def self.override_all_deployments_by_replay_streams(
            streams, skip_incompatible_types: false
        )
            streams_group = streams.to_deployment_group(
                skip_incompatible_types: skip_incompatible_types
            )
            Syskit::NetworkGeneration::Engine
                .register_system_network_postprocessing do |system_network_generator|
                    system_network_generator
                        .plan.find_local_tasks(Syskit::TaskContext).each do |task|
                            task.requirements.reset_deployment_selection
                            task.requirements.use_deployment_group(streams_group)
                        end
                end
        end
    end
end
