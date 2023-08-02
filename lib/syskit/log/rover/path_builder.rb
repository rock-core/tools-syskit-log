# frozen_string_literal: true

require "syskit/log"
require "syskit/log/datastore"

module Syskit
    module Log
        module Daru
            # A dsl-based builder for {Typelib::Path}
            class PathBuilder < BasicObject
                def initialize(
                    type, name = "", path = ::Typelib::Path.new([]),
                    transform = nil, vector_transform = nil
                )
                    @type = type
                    @name = name
                    @path = path
                    @transform = transform
                    @vector_transform = vector_transform
                end

                def __type
                    @type
                end

                def __name
                    @name
                end

                def __path
                    @path
                end

                def __transform
                    @transform
                end

                def __vector_transform
                    @vector_transform
                end

                def __terminal?
                    @transform ||
                        @type <= ::Typelib::NumericType ||
                        @type <= ::Typelib::EnumType
                end

                def vector_transform(&block)
                    if @vector_transform
                        raise ArgumentError, "there is already a vector transform block"
                    elsif @transform
                        raise ArgumentError, "cannot mix transform and vector transform"
                    elsif !(@type <= ::Typelib::NumericType) || @type.integer?
                        raise ArgumentError,
                              "vector transforms can only be applied on "\
                              "floating-point values"
                    end

                    ::Syskit::Log::Daru::PathBuilder.new(
                        @type, @name, @path, nil, block
                    )
                end

                def transform(&block)
                    if @vector_transform
                        raise ArgumentError, "cannot mix transform and vector transform"
                    elsif @transform
                        raise ArgumentError, "there is already a transform block"
                    end

                    ::Syskit::Log::Daru::PathBuilder.new(
                        @type, @name, @path, block, nil
                    )
                end

                def respond_to?(m)
                    if @type <= ::Typelib::CompoundType
                        @type.has_field?(m)
                    elsif @type <= ::Typelib::ArrayType ||
                          @type <= ::Typelib::ContainerType
                        m == :[]
                    else
                        false
                    end
                end

                def method_missing(m, *args, **kw) # rubocop:disable Style/MissingRespondToMissing
                    if __terminal?
                        super
                    elsif @type <= ::Typelib::CompoundType
                        __validate_compound_call(m, args, kw)
                        __resolve_compound_call(m)
                    elsif @type <= ::Typelib::ArrayType
                        __validate_array_call(m, args, kw)
                        __resolve_sequence_call(args.first)
                    elsif @type <= ::Typelib::ContainerType
                        __validate_sequence_call(m, args, kw)
                        __resolve_sequence_call(args.first)
                    else
                        super
                    end
                end

                def __resolve_compound_call(field_name)
                    path = @path.dup
                    path.push_call(:raw_get, field_name)
                    ::Syskit::Log::Daru::PathBuilder.new(
                        @type[field_name], "#{@name}.#{field_name}", path
                    )
                end

                def __validate_compound_call(field_name, args, kw)
                    if !@type.has_field?(field_name)
                        ::Kernel.raise ::NoMethodError.new(field_name),
                                       "#{@type.name} has no field named '#{field_name}'"
                    elsif !args.empty? || !kw.empty?
                        ::Kernel.raise ::ArgumentError,
                                       "#{@type.name}.#{field_name} expects no "\
                                       "arguments, got #{args.size} positional and "\
                                       "#{kw.size} keyword arguments"
                    end
                end

                def __validate_sequence_call(name, args, kw)
                    if name != :[]
                        ::Kernel.raise ::NoMethodError.new(name),
                                       "#{@type.name} only accessor is [], got #{name}"
                    elsif !kw.empty?
                        ::Kernel.raise ::ArgumentError,
                                       "#[] does not accept any keyword arguments"
                    elsif args.size != 1
                        ::Kernel.raise ::ArgumentError,
                                       "expected 1 argument to [], but got #{args}"
                    elsif !args[0].kind_of?(::Integer)
                        ::Kernel.raise ::TypeError,
                                       "expected a single numeric argument, got #{args[0]}"
                    end
                end

                def __validate_array_call(name, args, kw)
                    __validate_sequence_call(name, args, kw)

                    index = args[0]
                    return if index >= 0 && index < @type.length

                    ::Kernel.raise ::ArgumentError,
                                   "#{args[0]} out of bounds in an array of "\
                                   "#{@type.length}"
                end

                def __resolve_sequence_call(index)
                    path = @path.dup
                    path.push_call(:raw_get, index)
                    ::Syskit::Log::Daru::PathBuilder
                        .new(@type.deference, "#{@name}[#{index}]", path)
                end
            end
        end
    end
end
