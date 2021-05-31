# frozen_string_literal: true

module Syskit::Log
    # Set of modules used to extend Roby and Syskit's own classes
    module Extensions
        extend Logger::Hierarchy
        extend Logger::Forward
    end
end

require "syskit/log/extensions/deployment_group"
require "syskit/log/extensions/instance_requirements"
require "syskit/log/extensions/configuration"
require "syskit/log/extensions/execution_engine"

Syskit::Models::DeploymentGroup.class_eval do
    prepend Syskit::Log::Extensions::DeploymentGroup
end
Syskit::InstanceRequirements.class_eval do
    prepend Syskit::Log::Extensions::InstanceRequirements
end
Syskit::RobyApp::Configuration.class_eval do
    prepend Syskit::Log::Extensions::Configuration
end
Roby::ExecutionEngine.class_eval do
    prepend Syskit::Log::Extensions::ExecutionEngine
end
