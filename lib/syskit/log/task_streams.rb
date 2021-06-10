# frozen_string_literal: true

module Syskit::Log
    # Stream accessor for streams that have already be narrowed down to a
    # single task
    #
    # It is returned from the main stream pool by
    # {Streams#find_task_by_name)
    class TaskStreams < Streams
        def initialize(streams = [], task_name: nil)
            super(streams)
            @task_name = task_name
            @orogen_model_name = nil
        end

        # Returns the task name for all streams in self
        #
        # @raise (see unique_metadata)
        def task_name
            @task_name ||= unique_metadata("rock_task_name")
        end

        # Returns the orogen model name for all streams in self
        #
        # @raise (see unique_metadata)
        def orogen_model_name
            @orogen_model_name ||= unique_metadata("rock_task_model")
        end

        # Returns the Syskit model for the orogen model name in
        # {#orogen_model_name}
        #
        # @raise (see orogen_model_name)
        def model
            name = orogen_model_name
            unless (model = Syskit::TaskContext.find_model_from_orogen_name(name))
                raise Unknown, "cannot find a Syskit model for '#{name}'"
            end

            model
        end

        # Returns the replay task model for this streams
        def replay_model
            ReplayTaskContext.model_for(model.orogen_model)
        end

        # Enumerate the streams that are ports
        #
        # @yieldparam [String] port_name the name of the port
        # @yieldparam [Pocolog::DataStream] stream the data stream
        def each_port_stream
            return enum_for(__method__) unless block_given?

            streams.each do |s|
                if (s.metadata["rock_stream_type"] == "port") &&
                   (port_name = s.metadata["rock_task_object_name"])
                    yield(port_name, s)
                end
            end
        end

        # Enumerate the streams that are properties
        #
        # @yieldparam [String] property_name the name of the property
        # @yieldparam [Pocolog::DataStream] stream the data stream
        def each_property_stream
            return enum_for(__method__) unless block_given?

            streams.each do |s|
                if (s.metadata["rock_stream_type"] == "property") &&
                   (port_name = s.metadata["rock_task_object_name"])
                    yield(port_name, s)
                end
            end
        end

        # Find a port stream that matches the given name
        def find_port_by_name(name)
            objects = find_all_streams(RockStreamMatcher.new.ports.object_name(name))
            if objects.size > 1
                raise Ambiguous, "there are multiple ports with the name #{name}"
            end

            objects.first
        end

        # Find a property stream that matches the given name
        def find_property_by_name(name)
            objects = find_all_streams(RockStreamMatcher.new.properties.object_name(name))
            if objects.size > 1
                raise Ambiguous, "there are multiple properties with the name #{name}"
            end

            objects.first
        end

        # Property accessor object, mimicking the 'properties' accessor on Syskit tasks
        class Properties < BasicObject
            def initialize(task_streams)
                @task_streams = task_streams
            end

            def respond_to_missing?(name, _include_private = true)
                @task_streams.find_property_by_name(name.to_s)
            end

            def method_missing(name, *args)
                if (property = @task_streams.find_property_by_name(name.to_s))
                    return property if args.empty?

                    raise ArgumentError,
                          "wrong number of arguments (given #{args.size}, expected 0)"
                end

                super
            end
        end

        def properties
            Properties.new(self)
        end

        def respond_to_missing?(m, include_private = true)
            MetaRuby::DSLs.has_through_method_missing?(
                self, m,
                "_port" => "find_port_by_name"
            ) || super
        end

        # Syskit-looking accessors for ports (_port) and properties
        # (_property)
        def method_missing(m, *args)
            MetaRuby::DSLs.find_through_method_missing(
                self, m, args,
                "_port" => "find_port_by_name"
            ) || super
        end

        # @api private
        #
        # Resolves a metadata that must be unique among all the streams
        #
        # @raise Unknown if there are no streams, if they have different values
        #   for the metadata or if at least one of them does not have a value for
        #   the metadata.
        # @raise Ambiguous if some streams have different values for the
        #   metadata
        def unique_metadata(metadata_name)
            raise Unknown, "no streams" if streams.empty?

            model_name = nil
            streams.each do |s|
                unless (name = s.metadata[metadata_name])
                    raise Unknown,
                          "stream #{s.name} does not declare the "\
                          "#{metadata_name} metadata"
                end

                model_name ||= name
                if model_name != name
                    raise Ambiguous,
                          "streams declare more than one value for "\
                          "#{metadata_name}: #{model_name} and #{name}"
                end
            end
            model_name
        end

        def to_instance_requirements
            requirements = self.replay_model.to_instance_requirements
            requirements.use_deployment_group(to_deployment_group)
            requirements
        end

        def as_plan
            to_instance_requirements.as_plan
        end
    end
end
