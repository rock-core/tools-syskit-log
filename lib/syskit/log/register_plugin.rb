# frozen_string_literal: true

require "syskit/log/plugin"
Roby::Application.register_plugin("syskit-log", Syskit::Log::Plugin) do
end
