# frozen_string_literal: true

module Syskit::Log
    module Extensions
        # Extension of the Syskit::Models::DeploymentGroup class to add APIs related to
        # replaying tasks
        module DeploymentGroup
            # Expose a given set of streams as a task context in Syskit
            #
            # (see Models::ReplayTaskContext#for_streams)
            def use_pocolog_task(
                streams,
                name: streams.task_name, model: streams.model.to_component_model,
                allow_missing: true, skip_incompatible_types: false,
                on: "pocolog", process_managers: Syskit.conf
            )
                # Verify the process manager's availability
                process_managers.process_server_config_for(on)

                deployment_model = Deployment.for_streams(
                    streams,
                    name: name, model: model, allow_missing: allow_missing,
                    skip_incompatible_types: skip_incompatible_types
                )

                configured_deployment =
                    Syskit::Models::ConfiguredDeployment
                    .new(on, deployment_model, Hash[name => name], name)
                register_configured_deployment(configured_deployment)
                configured_deployment
            end
        end
    end
end
