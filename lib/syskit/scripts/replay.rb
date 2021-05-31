# frozen_string_literal: true

require "syskit/log/cli/replay"

class CLI < Thor
    desc "replay", "replaying of log data"
    subcommand "replay", Syskit::Log::CLI::Replay
end

Roby.display_exception do
    CLI.start(["replay", *ARGV])
end
