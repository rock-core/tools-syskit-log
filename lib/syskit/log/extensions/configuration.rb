# frozen_string_literal: true

module Syskit::Log
    module Extensions
        # Extension of the Syskit configuration class to add APIs related to
        # replaying tasks
        module Configuration
            # @deprecated use the deployment group API instead
            #
            # (see DeploymentGroup#use_pocolog_task)
            def use_pocolog_task(streams, **options)
                Roby.warn_deprecated "defining deployments globally on Syskit.conf is deprecated, use the deployment group API instead"
                deployment_group.use_pocolog_task(streams, **options)
            end
        end
    end
end
