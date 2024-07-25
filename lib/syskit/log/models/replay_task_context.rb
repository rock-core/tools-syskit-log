# frozen_string_literal: true

module Syskit
    module Log
        module Models
            # Model of tasks that replay data streams
            #
            # To replay the data streams in a Syskit network, one cannot use the
            # normal Syskit::TaskContext tasks, as they can be customized by the
            # system designer (reimplement #configure, add polling blocks,
            # scripts, ...)
            #
            # So, instead, syskit-pocolog maintains a parallel hierarchy of task
            # context models that mirrors the "plain" ones, but does not have
            # all the runtime handlers
            module ReplayTaskContext
                attr_reader :plain_task_model

                # Define an replay model for the given existing task model
                def for_plain_model(plain_model, register: true)
                    if plain_model <= Syskit::Log::ReplayTaskContext
                        raise ArgumentError, "#{plain_model} is already a replay model"
                    end

                    if (model = find_model_by_orogen(plain_model.orogen_model))
                        return model
                    end

                    define_from_orogen(
                        plain_model.orogen_model,
                        supermodel: Syskit::Log::ReplayTaskContext,
                        plain_model: plain_model, register: register
                    )
                end

                # @deprecated use {#for_plain_model} instead
                #
                # Returns the {ReplayTaskContext} model that should be used to
                # replay tasks of the given orogen model
                def model_for(orogen_model, register: true)
                    if (model = find_model_by_orogen(orogen_model))
                        return model
                    end

                    define_from_orogen(
                        orogen_model,
                        supermodel: Syskit::Log::ReplayTaskContext,
                        plain_model: Syskit::TaskContext.model_for(orogen_model),
                        register: register
                    )
                end

                # @api private
                #
                # Setup a newly created {ReplayTaskContext}. This is called
                # internally by MetaRuby's #new_submodel
                def setup_submodel(
                    submodel,
                    orogen_model: nil,
                    plain_model: @plain_task_model,
                    **options, &block
                )
                    super(submodel, orogen_model: orogen_model, **options, &block)

                    submodel.instance_variable_set :@plain_task_model, plain_model
                    submodel.copy_services_from_plain_model(plain_model)
                    submodel.copy_arguments_from_plain_model(plain_model)
                end

                def register_model
                    self.name = OroGen::Pocolog.register_syskit_model(self)
                end

                # @api private
                #
                # Copy the services of a task model (in this case, expected to
                # be the replay model's {#plain_task_model}) onto this model
                def copy_services_from_plain_model(plain_model)
                    plain_model.each_data_service do |name, srv|
                        data_services[name] = srv.attach(self)
                    end
                    plain_model.each_dynamic_service do |name, srv|
                        dynamic_services[name] = srv.attach(self)
                    end
                end

                # @api private
                #
                # Copy the argument definitions of a task model (in this case, expected to
                # be the replay model's {#plain_task_model}) onto this model
                def copy_arguments_from_plain_model(plain_model)
                    plain_model.each_argument do |_name, arg|
                        argument arg.name, doc: arg.doc, default: arg.default
                    end
                end

                # Reimplemented to make ReplayTaskContext fullfills?
                # {#plain_task_model}
                def fullfills?(model)
                    self == model || super || @plain_task_model.fullfills?(model)
                end

                def each_fullfilled_model
                    return enum_for(__method__) unless block_given?

                    super

                    yield(@plain_task_model)
                end
            end
        end
    end
end
