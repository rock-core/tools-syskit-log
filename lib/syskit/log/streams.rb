# frozen_string_literal: true

module Syskit::Log
    # A set of log streams
    class Streams
        # Load the set of streams available from a directory
        #
        # Note that in each directory, a stream's identity (task name,
        # port/property name and type) must be unique. If you need to mix
        # log streams, load files in separate {Streams} objects
        def self.from_dir(path)
            streams = new
            streams.add_dir(Pathname(path))
            streams
        end

        # Load the set of streams available from a file
        def self.from_file(file)
            streams = new
            streams.add_file(Pathname(file))
            streams
        end

        # Load the set of streams available from a dataset
        def self.from_dataset(dataset)
            streams = new
            streams.add_dataset(dataset)
            streams
        end

        # The list of streams that are available
        #
        # @return [Array<LazyDataStream>]
        attr_reader :streams

        # The common registry
        attr_reader :registry

        def initialize(streams = [], registry: Typelib::Registry.new)
            @streams = streams
            @registry = registry
        end

        # The number of data streams in self
        def num_streams
            streams.size
        end

        # Enumerate the streams by grouping them per-task
        #
        # It will only enumerate the tasks that are "functional", that is that
        # they have a name and model, and the model can be resolved
        #
        # @param [Boolean] load_models whether the method should attempt to
        #   load the task's models if they are not yet loaded
        # @param [Boolean] skip_tasks_without_models whether the tasks whose
        #   model cannot be found should be enumerated or not
        # @param [Boolean] raise_on_missing_task_models whether the method
        #   should raise if a task model cannot be resolved
        # @param [#project_model_from_orogen_name] loader the object that should
        #   be used to load the missing models when load_models is true
        # @yieldparam [TaskStreams] task
        def each_task(
            load_models: false,
            skip_tasks_without_models: false,
            raise_on_missing_task_models: false,
            loader: Roby.app.default_loader
        )
            unless block_given?
                return enum_for(
                    __method__,
                    load_models: load_models,
                    skip_tasks_without_models: skip_tasks_without_models,
                    raise_on_missing_task_models: raise_on_missing_task_models,
                    loader: loader
                )
            end

            available_tasks = Hash.new { |h, k| h[k] = [] }
            ignored_streams = Hash.new { |h, k| h[k] = [] }
            empty_task_models = []
            each_stream do |s|
                next unless (task_model_name = s.metadata["rock_task_model"])

                if task_model_name.empty?
                    empty_task_models << s
                    next
                end

                task_m = Syskit::TaskContext.find_model_from_orogen_name(task_model_name)
                if !task_m && load_models
                    orogen_project_name, *_tail = task_model_name.split("::")
                    begin
                        loader.project_model_from_name(orogen_project_name)
                    rescue OroGen::ProjectNotFound
                        raise if raise_on_missing_task_models
                    end
                    task_m = Syskit::TaskContext
                             .find_model_from_orogen_name(task_model_name)
                end

                if !task_m && raise_on_missing_task_models
                    raise OroGen::NotFound, "cannot find #{task_model_name}"
                end

                if task_m || !skip_tasks_without_models
                    available_tasks[s.metadata["rock_task_name"]] << s
                else
                    ignored_streams[task_model_name] << s
                end
            end

            unless empty_task_models.empty?
                Syskit::Log.warn(
                    "ignored #{empty_task_models.size} streams that declared a "\
                    "task model, but left it empty: "\
                    "#{empty_task_models.map(&:name).sort.join(', ')}"
                )
            end

            ignored_streams.each do |task_model_name, streams|
                Syskit::Log.warn(
                    "ignored #{streams.size} streams because the task model "\
                    "#{task_model_name.inspect} cannot be found: "\
                    "#{streams.map(&:name).sort.join(', ')}"
                )
            end

            available_tasks.each_value.map do |streams|
                yield(TaskStreams.new(streams))
            end
        end

        # Enumerate the streams
        #
        # @yieldparam [Pocolog::DataStream,LazyDataStream]
        def each_stream(&block)
            streams.each(&block)
        end

        # @api private
        #
        # Find the pocolog logfile groups and returns them
        #
        # @param [Pathname] path the directory to look into
        # @return [Array<Array<Pathname>>]
        def make_file_groups_in_dir(path)
            files_per_basename = Hash.new { |h, k| h[k] = [] }
            path.children.each do |file_or_dir|
                next unless file_or_dir.file?
                next unless file_or_dir.extname == ".log"

                base_filename = file_or_dir.sub_ext("")
                id = base_filename.extname[1..-1]
                next if id !~ /^\d+$/

                base_filename = base_filename.sub_ext("")
                files_per_basename[base_filename.to_s][Integer(id)] = file_or_dir
            end
            files_per_basename.values.map(&:compact)
        end

        # Load all log files from a directory
        def add_dir(path, from: nil, to: nil)
            make_file_groups_in_dir(path).each do |files|
                add_file_group(files, from: from, to: to)
            end
        end

        # Load all streams contained in a dataset
        #
        # @param [Datastore::Dataset] dataset
        # @return [void]
        def add_dataset(dataset)
            dataset.each_pocolog_lazy_stream do |stream|
                add_stream(stream)
            end
        end

        # Returns the normalized file basename for the given stream
        #
        # @param [Pocolog::DataStream] stream
        # @return [String]
        def self.normalized_filename(metadata)
            task_name   = metadata["rock_task_name"].gsub(%r{^/}, "")
            object_name = metadata["rock_task_object_name"]
            (task_name + "::" + object_name).gsub("/", ":")
        end

        # Returns the normalized stream name for the given stream
        #
        # @param [Pocolog::DataStream] stream
        # @return [String]
        def self.normalized_stream_name(metadata)
            task_name   = metadata["rock_task_name"].gsub(%r{^/}, "")
            object_name = metadata["rock_task_object_name"]
            "#{task_name}.#{object_name}"
        end

        # Open a list of pocolog files that belong as a group
        #
        # I.e. each file is part of the same general datastream
        #
        # @raise Errno::ENOENT if the path does not exist
        def add_file_group(group, from: nil, to: nil)
            file = Pocolog::Logfiles.new(*group.map(&:open), registry)
            file.streams.each do |s|
                s = s.from_logical_time(from) if from
                s = s.to_logical_time(to) if to
                add_stream(LazyDataStream.from_pocolog_stream(s))
            end
        end

        # Modify a stream metadata to remove quirks that exist(ed) during log
        # generation
        #
        # @param [Hash] metadata
        def self.sanitize_metadata(metadata, stream_name: nil)
            metadata = metadata.dup
            if (model = metadata["rock_task_model"]) && model.empty?
                Syskit::Log.warn(
                    "removing empty metadata property 'rock_task_model' "\
                    "from #{stream_name}"
                )
                metadata.delete("rock_task_model")
            end

            if model&.start_with?("OroGen.")
                normalized_model = model[7..-1].gsub(".", "::")

                Syskit::Log.warn(
                    "found Syskit-looking model name #{model}, "\
                    "normalized to #{normalized_model}"
                )
                metadata["rock_task_model"] = normalized_model
            end

            return metadata unless (task_name = metadata["rock_task_name"])

            # Remove leading /
            task_name = task_name[1..-1] if task_name.start_with?("/")
            metadata["rock_task_name"] = task_name

            # Remove the namespace, store it in a separate metadata entry
            namespace = task_name.gsub(%r{/.*}, "")
            return metadata if namespace == task_name

            metadata["rock_task_namespace"] = namespace
            metadata["rock_task_name"] = task_name.gsub(%r{.*/}, "")
            metadata
        end

        # Load the streams from a log file
        def add_file(file, from: nil, to: nil)
            add_file_group([file], from: from, to: to)
        end

        # Add a new stream
        #
        # @param [Pocolog::DataStream] s
        def add_stream(s)
            s.metadata = Streams.sanitize_metadata(s.metadata, stream_name: s.name)
            streams << s
        end

        # Find all streams whose metadata match the given query
        #
        # @return [Array<Pocolog::DataStream,LazyDataStream>]
        def find_all_streams(query)
            streams.find_all { |s| query === s }
        end

        # Find all streams that belong to a task
        def find_task_by_name(name)
            streams = find_all_streams(RockStreamMatcher.new.task_name(name))
            return if streams.empty?

            TaskStreams.new(streams)
        end

        def each_task_name
            return enum_for(__method__) unless block_given?

            seen = Set.new
            each_stream do |s|
                next unless (name = s.metadata["rock_task_name"])

                yield("#{name}_task") if seen.add?(name)
            end
        end

        def methods(*)
            super + each_task_name.to_a.sort
        end

        def respond_to_missing?(m, include_private = false)
            MetaRuby::DSLs.has_through_method_missing?(
                self, m,
                "_task" => "find_task_by_name"
            ) || super
        end

        # Give access to the streams per-task by calling <task_name>_task
        def method_missing(m, *args, &block)
            MetaRuby::DSLs.find_through_method_missing(
                self, m, args,
                "_task" => "find_task_by_name"
            ) || super
        end

        # Creates a deployment group object that deploys all streams
        #
        # @param (see Streams#each_task)
        def to_deployment_group(
            load_models: true,
            skip_tasks_without_models: true,
            skip_incompatible_types: false,
            raise_on_missing_task_models: false,
            loader: Roby.app.default_loader
        )
            group = Syskit::Models::DeploymentGroup.new
            each_task(load_models: load_models,
                      skip_tasks_without_models: skip_tasks_without_models,
                      raise_on_missing_task_models: raise_on_missing_task_models,
                      loader: loader) do |task_streams|
                group.use_pocolog_task(
                    task_streams, skip_incompatible_types: skip_incompatible_types
                )
            end
            group
        end
    end
end
