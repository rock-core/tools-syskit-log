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
            no_commands do # rubocop:disable Metrics/BlockLength
                def setup_roby_for_running(run_controllers: false)
                    super
                    app.using "syskit"
                    app.using "syskit-log"
                end

                def start_resolve_streams_and_scripts(args, from: nil, to: nil)
                    hash, rest =
                        args.partition { Datastore::Dataset.valid_encoded_digest?(_1) }
                    raise "cannot give more than one dataset" if hash.size > 1

                    dataset_hash = hash.first
                    paths = rest.map { |p| Pathname.new(p) }
                    if (non_existent = paths.find { |p| !p.exist? })
                        raise ArgumentError, "#{non_existent} does not exist"
                    end

                    paths = paths.map(&:expand_path)
                    [dataset_hash, paths]
                end

                def start_resolve_streams_from_hash(dataset_hash, from: nil, to: nil)
                    dataset = Datastore.default.get(dataset_hash)
                    dataset.streams(from: from, to: to)
                end

                def start_resolve_streams_from_paths(dataset_paths, from: nil, to: nil)
                    streams = Syskit::Log::Streams.new
                    dataset_paths.each do |p|
                        if p.directory?
                            streams.add_dir(p, from: from, to: to)
                        else
                            streams.add_file(p, from: from, to: to)
                        end
                    end
                    streams
                end

                def start_setup_app(script_paths)
                    setup_common
                    setup_roby_for_running(run_controllers: options[:controller])
                    app.single if options[:single]

                    options[:set].each do |s|
                        app.argv_set << s
                        Roby::Application.apply_conf_from_argv(s)
                    end

                    options[:log].each do |spec_list|
                        spec_list.split(",").each do |spec|
                            mod, level, file = spec.split(":")
                            Roby.app.log_setup(mod, level, file)
                        end
                    end

                    app.on_setup(user: true) do
                        app.on_require(user: true) do
                            script_paths.each { |p| require p.to_s }
                        end
                    end

                    Conf.ui = options[:ui]

                    app.public_shell_interface = true
                    app.public_logs = true
                    app.public_log_server = true
                    app.setup

                    start_replay_all if script_paths.empty?
                end

                def start_setup_ui
                    require "syskit/log/replay_ui"
                    @replay_manager_ui = Syskit::Log::ReplayUI.new
                    @replay_manager_ui.replay_speed = options[:speed]
                    app.controller(user: false) do
                        @replay_manager_ui.show
                    end
                end

                def start_should_play?
                    if options[:play].nil?
                        !options[:ui]
                    else
                        options[:play]
                    end
                end

                def start_replay_all
                    # Load the default script
                    Syskit::Log::Plugin.override_all_deployments_by_replay_streams(
                        streams,
                        skip_incompatible_types: options[:skip_incompatible_types]
                    )
                end

                def replay_manager
                    app.execution_engine.pocolog_replay_manager
                end

                def start_replay(streams, from, to)
                    replay_manager.seek(from) if from

                    begin
                        Syskit::Log.streams = streams
                        start_play(to) if start_should_play?

                        app.run
                    ensure
                        Syskit::Log.streams = nil
                        app.cleanup
                    end
                end

                def reached_time?(target)
                    return unless (current_time = replay_manager.time)

                    current_time >= target
                end

                def start_play(to_time)
                    if to_time && options[:quit_when_done]
                        app.execution_engine.each_cycle do
                            app.quit if reached_time?(to_time)
                        end
                    end

                    if options[:ui]
                        start_play_with_ui(to_time)
                    else
                        start_play_headless(to_time)
                    end
                end

                def start_play_with_ui(to_time)
                    @replay_manager_ui.play
                    return unless to_time

                    handler = app.execution_engine.each_cycle do
                        if reached_time?(to_time)
                            @replay_manager_ui.pause
                            handler.dispose
                        end
                    end
                end

                def start_play_headless(to_time)
                    engine = app.execution_engine
                    engine.each_cycle do
                        unless reached_time?(to_time)
                            engine.pocolog_replay_manager.process_in_realtime(
                                options[:speed]
                            )
                        end
                    end
                end

                def parse_date_and_time(string)
                    return unless string
                    return Time.at(Integer(string)) if /^\d+\.?\d+$/.match?(string)

                    Time.parse(string)
                end
            end

            desc "start [SCRIPTS] [DATASETS]",
                 "replays a data replay script. If no script is given, allows "\
                 "to replay streams using profile definitions"
            option :single, type: :boolean, default: true
            option :robot, aliases: "r", type: :string,
                           desc: "the robot configuration to load"
            option :controller, aliases: "c", type: :boolean, default: false
            option :skip_incompatible_types, type: :boolean, default: false
            option :set, type: :string, repeatable: true,
                         desc: "configuration variables to set, as path.to.key=value"
            option :from, type: :string, desc: "start replay at this date & time"
            option :to, type: :string, desc: "end replay at this date & time"
            def start(*args)
                from = parse_date_and_time(options[:from]) if options[:from]
                to = parse_date_and_time(options[:to]) if options[:to]
                streams, script_paths =
                    start_resolve_streams_and_scripts(args, from: from, to: to)

                setup_common
                setup_roby_for_running(run_controllers: options[:controller])
                app.single if options[:single]

                options[:set].each do |s|
                    app.argv_set << s
                    Roby::Application.apply_conf_from_argv(s)
                end

                app.on_require(user: true) do
                    script_paths.each { |p| require p.to_s }
                end

                app.public_shell_interface = true
                app.public_logs = true
                app.public_log_server = true
                app.setup

                begin
                    Syskit::Log.streams = streams
                    if script_paths.empty?
                        # Load the default script
                        Syskit::Log::Plugin.override_all_deployments_by_replay_streams(
                            streams,
                            skip_incompatible_types: options[:skip_incompatible_types]
                        )
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
