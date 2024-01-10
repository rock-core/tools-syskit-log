# frozen_string_literal: true

module Syskit::Log
    module Models
        # Metamodel for {Syskit::Log::Deployment}
        module Deployment
            # Pocolog deployments only have a single task. This is it
            #
            # @return [Syskit::Models::TaskContext]
            attr_reader :task_model

            # Mappings from streams to ports
            #
            # @see add_stream add_streams_from
            attr_reader :streams_to_port

            # Define a deployment model that will manage the given streams
            def for_streams(
                streams,
                name: streams.task_name,
                model: streams.replay_model,
                allow_missing: true, skip_incompatible_types: false
            )
                deployment_model = new_submodel(name: "Deployment::Pocolog::#{name}") do
                    task name, model.orogen_model
                end
                deployment_model.add_streams_from(
                    streams,
                    allow_missing: allow_missing,
                    skip_incompatible_types: skip_incompatible_types
                )
                deployment_model
            end

            # Create a new deployment model
            #
            # Unlike the Syskit deployment models, this does not yield. The task
            # model is instead supposed to be given as the task_model argument.
            #
            # @param [String] task_name the task name
            # @param options the standard options given to super
            # @return [Syskit::Log::Models::Deployment]
            def new_submodel(**options, &block)
                super(**options, &block)
            end

            # @api private
            #
            # Callback called by metaruby in {#new_submodel}
            def setup_submodel(submodel, **options, &block)
                super

                orogen_model = submodel.each_orogen_deployed_task_context_model.first
                return unless orogen_model

                submodel.instance_variable_set(
                    :@task_model,
                    Syskit::Log::ReplayTaskContext.model_for(orogen_model.task_model)
                )
                submodel.instance_variable_set :@streams_to_port, {}
            end

            def each_deployed_task_model
                return enum_for(__method__) unless block_given?

                super do |name, plain_task_model|
                    syskit_model = Syskit::TaskContext
                                   .model_for(plain_task_model.orogen_model)
                    yield(name, syskit_model)
                end
            end

            # Add all matching streams from the given streams argument
            #
            # The streams are matched by name.
            #
            # @param [Streams] streams the set of streams that should be
            #   exported. Streams which does not have a corresponding output port
            #   are ignored.
            # @param [Boolean] allow_missing controls the behaviour if the task
            #   has a port that does not have a matching stream. If true, a
            #   warning is issued, if false a {MissingStream} exception is raised.
            #
            #   Note that this does not affect the error raised if a task's
            #   output is connected with no associated stream
            def add_streams_from(
                streams, allow_missing: true, skip_incompatible_types: false
            )
                task_model.each_output_port do |p|
                    if (p_stream = streams.find_port_by_name(p.name))
                        begin
                            add_stream(p_stream, p)
                        rescue MismatchingType
                            raise unless skip_incompatible_types
                        end

                        next
                    end

                    raise MissingStream, "no stream named #{p.name}" unless allow_missing

                    ports = streams.each_port_stream.map { |name, _| name }
                    if ports.empty?
                        Syskit::Log.warn(
                            "no log stream available for #{p}, ignored "\
                            "as allow_missing is true (there are no log "\
                            "streams for the underlying task)"
                        )
                    else
                        Syskit::Log.warn(
                            "no log stream available for #{p}, ignored "\
                            "as allow_missing is true, known ports are: "\
                            "#{ports.sort.join(', ')}"
                        )
                    end
                end
            end

            # Add a stream-to-port mapping
            #
            # @param [Pocolog::DataStream] stream the data stream
            # @param [Syskit::Models::OutputPort] port the output port that
            #   should output the stream's data.
            # @return [void]
            #
            # @raise ArgumentError if the port is not an output port
            # @raise ArgumentError if the port is not from {#task_model}
            # @raise MismatchingType if the port and stream have differing types
            def add_stream(
                stream,
                port = task_model.port_by_name(stream.metadata["rock_task_object_name"])
            )
                if !port.output?
                    raise ArgumentError, "cannot map a log stream to an input port"
                elsif port.component_model != task_model
                    raise ArgumentError,
                          "#{self} deploys #{task_model} but the stream mapping "\
                          "is for #{port.component_model}"
                end

                ops = stream.upgrade_ops_for_target(port.type)
                streams_to_port[stream] = [ops, port]
            rescue Pocolog::Upgrade::InvalidCast => e
                raise MismatchingType.new(stream, port), e.message, e.backtrace
            end

            # Enumerate the declared stream-to-port mappings
            #
            # @yieldparam [Pocolog::DataStream] stream
            # @yieldparam [Syskit::Models::OutputPort] output_port
            def each_stream_mapping(&block)
                streams_to_port.each(&block)
            end
        end
    end
end
