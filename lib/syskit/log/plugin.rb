# frozen_string_literal: true

require "syskit"

module Syskit::Log
    module Plugin
        def self.setup(app)
            Pocolog.logger = Syskit::Log.logger

            if Syskit.conf.respond_to?(:register_ruby_tasks_manager)
                Syskit.conf.register_ruby_tasks_manager(
                    "pocolog",
                    loader: app.default_loader, log_dir: app.log_dir,
                    logging_enabled: false
                )
            else
                manager = Orocos::RubyTasks::ProcessManager.new(app.default_loader)
                Syskit.conf.register_process_server("pocolog", manager, app.log_dir)
            end
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
