# frozen_string_literal: true

require "roby"
require "syskit"
require "roby/cli/base"

require "syskit/log"

module Syskit::Log
    class << self
        # Streams selected by the user on the command line
        #
        # @return [Syskit::Log::Streams]
        attr_accessor :streams
    end

    module CLI
        class Replay < Roby::CLI::Base
            no_commands do
                def setup_roby_for_running(run_controllers: false)
                    super
                    app.using "syskit"
                    app.using "syskit-log"
                end
            end

            desc "start [SCRIPTS] [DATASETS]",
                 "replays a data replay script. If no script is given, allows "\
                 "to replay streams using profile definitions"
            option :single, type: :boolean, default: true
            option :robot, aliases: "r", type: :string,
                           desc: "the robot configuration to load"
            def start(*path)
                paths = path.map { |p| Pathname.new(p) }
                if (non_existent = paths.find { |p| !p.exist? })
                    raise ArgumentError, "#{non_existent} does not exist"
                end

                paths = paths.map(&:expand_path)

                setup_common
                setup_roby_for_running(run_controllers: true)
                app.single if options[:single]
                script_paths, dataset_paths =
                    paths.partition { |p| p.extname == ".rb" }

                app.setup
                begin
                    streams = Syskit::Log::Streams.new
                    dataset_paths.each do |p|
                        if p.directory?
                            streams.add_dir(p)
                        else
                            streams.add_file(p)
                        end
                    end
                    Syskit::Log.streams = streams

                    if script_paths.empty?
                        # Load the default script
                        Syskit::Log::Plugin
                            .override_all_deployments_by_replay_streams(streams)
                    else
                        script_paths.each { |p| require p.to_s }
                    end
                    app.run
                ensure
                    Syskit::Log.streams = nil
                    app.cleanup
                end
            end
        end
    end
end
