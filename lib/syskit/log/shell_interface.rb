# frozen_string_literal: true

begin
    require "roby/interface/core"
rescue LoadError
    require "roby/interface"
end

module Syskit::Log
    # Definition of the syskit-specific interface commands
    class ShellInterface < Roby::Interface::CommandLibrary
        attr_reader :replay_manager

        def initialize(app)
            super
            @replay_manager = app.plan.execution_engine.pocolog_replay_manager
            if defined?(Runkit)
                Runkit.load_typekit "base"
                @time_channel = Runkit::RubyTasks::TaskContext
            else
                Orocos.load_typekit "base"
                @time_channel = Orocos::RubyTasks::TaskContext
            end
        end

        def time
            replay_manager.time
        end
        command :time, "the current replay time", advanced: true
    end
end

Roby::Interface::Interface.subcommand "replay", Syskit::Log::ShellInterface, "Commands specific to syskit-pocolog"
