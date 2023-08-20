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
require "zstd-ruby"
require "syskit/log/version"
require "syskit/log/exceptions"
require "syskit/log/digest_io"
require "syskit/log/zstd_io"
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

            paths = logfiles_glob(real_path).map do |path|
                basename = path.basename
                m = /(.*)\.(\d+)\.log(?:\.zst)?$/.match(basename.to_s)
                [m[1], Integer(m[2]), path] if m
            end
            paths.compact.sort.map { |_, _, path| path }
        end

        def self.logfiles_glob(path)
            Pathname.enum_for(:glob, path + "*.*.log") +
                Pathname.enum_for(:glob, path + "*.*.log.zst")
        end

        def self.open_in_stream(path, &block)
            return path.open(&block) unless path.extname == ".zst"
            return ZstdIO.new(path.open) unless block_given?

            path.open do |io|
                yield(ZstdIO.new(io))
            end
        end

        def self.open_out_stream(path, &block)
            return path.open("w", &block) unless path.extname == ".zst"

            unless block_given?
                return ZstdIO.new(path.open("w"), read: false, write: true)
            end

            path.open("w") do |io|
                zstd = ZstdIO.new(io, read: false, write: true)
                yield zstd
            ensure
                zstd.flush
            end
        end
    end
end
