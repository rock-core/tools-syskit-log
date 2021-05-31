# frozen_string_literal: true

module Syskit::Log
    module Extensions
        # Extension of the Syskit::InstanceRequirements class to add APIs related to
        # replaying tasks
        module InstanceRequirements
            # Add a set replay task(s) that should be used to deploy self
            #
            # (see DeploymentGroup#use_pocolog_task)
            def use_pocolog_task(streams, **options)
                invalidate_template
                deployment_group.use_pocolog_task(streams, **options)
            end
        end
    end
end
