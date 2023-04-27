# frozen_string_literal: true

module Syskit
    # Provide an alias for the RockLogger is set to the logger model in orogen/logger.rb
    # as OroGen.logger is taken (can't access through OroGen.logger.LoggerTask)
    RockLogger = OroGen.syskit_model_by_orogen_name("logger::Logger")
end

Syskit.extend_model Syskit::RockLogger do # rubocop:disable Metrics/BlockLength
    class << self
        attr_reader :logfile_indexes

        def reset_log_indexes
            @logfile_indexes = {}
        end
    end
    @logfile_indexes = {}

    provides Syskit::LoggerService
    include Syskit::NetworkGeneration::LoggerConfigurationSupport

    stub do
        def createLoggingPort(port_name, port_type, _metadata) # rubocop:disable Naming/MethodName
            create_input_port(port_name, port_type)
            true
        end
    end

    # True if this logger is its deployment's default logger
    #
    # In this case, it will set itself up using the deployment's logging
    # configuration
    attr_predicate :default_logger?, true

    def update_properties
        super

        properties.overwrite_existing_files = false
        properties.auto_timestamp_files = false
    end

    event :start do |context|
        properties.file = default_logger_next_file_path if default_logger?

        super(context)
    end

    def default_logger_file_name
        orocos_name.sub(/_[L|l]ogger/, "")
    end

    def default_logger_next_index(log_file_name)
        Syskit::RockLogger.logfile_indexes[log_file_name] =
            Syskit::RockLogger.logfile_indexes.fetch(log_file_name, -1) + 1
    end

    def default_logger_log_dir
        Syskit.conf.process_server_config_for(log_server_name).log_dir
    end

    # Sets up the default logger of this process
    def default_logger_next_file_path
        log_file_name = default_logger_file_name
        index = default_logger_next_index(log_file_name)

        log_file_path = "#{log_file_name}.#{index}.log"

        log_dir = default_logger_log_dir
        # NOTE: log_dir should be nil to mean "no log dir", not empty.
        # This is a workaround to help migrating from Syskit code that sets it to empty
        log_file_path = File.join(log_dir, log_file_path) if log_dir && !log_dir.empty?
        log_file_path
    end

    def log_server_name
        execution_agent.arguments[:on]
    end

    def rotate_log
        return [] unless default_logger?

        previous_file = properties.file
        properties.file = default_logger_next_file_path
        [previous_file]
    end
end
