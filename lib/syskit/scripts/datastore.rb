# frozen_string_literal: true

require "syskit/log/cli/datastore"

class CLI < Thor
    def self.exit_on_failure?
        true
    end

    desc "datastore", "data management"
    subcommand "datastore", Syskit::Log::CLI::Datastore

    desc "ds", "data management"
    subcommand "ds", Syskit::Log::CLI::Datastore
end

Roby.display_exception do
    CLI.start(["datastore", *ARGV])
end
