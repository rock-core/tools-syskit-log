# frozen_string_literal: true

require "pocolog"
require "syskit"

module Syskit
    # Toplevel module for all the log management functionality
    module Log
        extend Logger::Root("Syskit::Log", Logger::WARN)
    end
end

require "digest/sha2"
require "metaruby/dsls/find_through_method_missing"
require "pocolog/cli/null_reporter"
require "syskit/log/version"
require "syskit/log/exceptions"
require "syskit/log/lazy_data_stream"
require "syskit/log/streams"
require "syskit/log/task_streams"
require "syskit/log/rock_stream_matcher"

require "syskit/log/models/deployment"
require "syskit/log/deployment"
require "syskit/log/models/replay_task_context"
require "syskit/log/replay_task_context"
require "syskit/log/replay_manager"

require "syskit/log/extensions"
require "syskit/log/shell_interface"
require "syskit/log/registration_namespace"
require "syskit/log/plugin"

require "syskit/log/datastore"

require "rom-sql"
require "syskit/log/roby_sql_index/entities"
require "syskit/log/roby_sql_index/definitions"
require "syskit/log/roby_sql_index/index"
require "syskit/log/roby_sql_index/accessors"

module Syskit
    module Log # rubocop:disable Style/Documentation
        # Returns the paths of the pocolog log files in a given directory
        #
        # The returned paths are sorted in 'pocolog' order, i.e. multi-IO files are
        # following each other in the order of their place in the overall IO
        # sequence
        #
        # @param [Pathname] dir_path path to the directory
        def self.logfiles_in_dir(dir_path)
            real_path = Pathname.new(dir_path).realpath

            paths = Pathname.enum_for(:glob, real_path + "*.*.log").map do |path|
                basename = path.basename
                m = /(.*)\.(\d+)\.log$/.match(basename.to_s)
                [m[1], Integer(m[2]), path] if m
            end
            paths.compact.sort.map { |_, _, path| path }
        end
    end
end
