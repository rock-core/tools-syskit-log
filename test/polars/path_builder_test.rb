# frozen_string_literal: true

require "test_helper"
require "syskit/log/daru"

module Syskit
    module Log
        module Daru
            describe PathBuilder do
                before do
                    @registry = Typelib::Registry.new
                end

                describe "the general behavior" do
                    before do
                        @path_builder = PathBuilder.new(
                            @type = flexmock,
                            @name = flexmock,
                            @path = flexmock
                        )
                    end

                    it "reports its current name" do
                        assert_same @name, @path_builder.__name
                    end

                    it "reports its current type" do
                        assert_same @type, @path_builder.__type
                    end

                    it "reports its current path" do
                        assert_same @path, @path_builder.__path
                    end
                end

                describe "when on a numeric type" do
                    before do
                        @type = @registry.create_numeric "/something", 4, :sint
                        @path_builder = PathBuilder.new(@type, "somename")
                    end

                    it "reports that it is terminal" do
                        assert @path_builder.__terminal?
                    end

                    it "does not respond to []" do
                        refute @path_builder.respond_to?(:[])
                    end

                    it "does not respond to some method" do
                        refute @path_builder.respond_to?(:some_method)
                    end

                    it "raises if trying to call []" do
                        assert_raises(NoMethodError) do
                            @path_builder[0]
                        end
                    end

                    it "raises if trying to call some method" do
                        assert_raises(NoMethodError) do
                            @path_builder.some_method
                        end
                    end
                end

                describe "when on an enum type" do
                    before do
                        @type = @registry.create_enum "/enum", 4 do |e|
                            e.bla = 1
                            e.blo = 2
                        end
                        @path_builder = PathBuilder.new(@type, "somename")
                    end

                    it "reports that it is terminal" do
                        assert @path_builder.__terminal?
                    end

                    it "does not respond to []" do
                        refute @path_builder.respond_to?(:[])
                    end

                    it "does not respond to some method" do
                        refute @path_builder.respond_to?(:some_method)
                    end

                    it "raises if trying to call []" do
                        assert_raises(NoMethodError) do
                            @path_builder[0]
                        end
                    end

                    it "raises if trying to call some method" do
                        assert_raises(NoMethodError) do
                            @path_builder.some_method
                        end
                    end
                end

                describe "when on a compound type" do
                    before do
                        @scalar_t = @registry.create_numeric "/numeric", 4, :sint
                        @type = @registry.create_compound "/compound" do |t|
                            t.field = "/numeric"
                        end

                        @path_builder = PathBuilder.new(@type, "somename")
                    end

                    it "reports that it is not terminal" do
                        refute @path_builder.__terminal?
                    end

                    it "does not respond to []" do
                        refute @path_builder.respond_to?(:[])
                    end

                    it "does not respond to some method" do
                        refute @path_builder.respond_to?(:some_method)
                    end

                    it "does respond to a method that matches a field name" do
                        assert @path_builder.respond_to?(:field)
                    end

                    it "raises if trying to call []" do
                        assert_raises(NoMethodError) { @path_builder[0] }
                    end

                    it "raises if trying to call some arbitrary method" do
                        assert_raises(NoMethodError) { @path_builder.some_method }
                    end

                    it "returns a path builder that points to the given field" do
                        builder = @path_builder.field
                        assert_equal "somename.field", builder.__name
                        assert_equal @scalar_t, builder.__type
                        assert_equal [[:call, %I[raw_get field]]], builder.__path.elements
                    end

                    it "raises if trying to call a field method with arguments" do
                        assert_raises(ArgumentError) { @path_builder.field(1) }
                    end
                end

                describe "when on a container type" do
                    before do
                        @scalar_t = @registry.create_numeric "/numeric", 4, :sint
                        @type = @registry.create_container "/std/vector", "/numeric"
                        @path_builder = PathBuilder.new(@type, "somename")
                    end

                    it "reports that it is not terminal" do
                        refute @path_builder.__terminal?
                    end

                    it "responds to []" do
                        assert @path_builder.respond_to?(:[])
                    end

                    it "does not respond to some method" do
                        refute @path_builder.respond_to?(:some_method)
                    end

                    it "raises if trying to call some arbitrary method" do
                        assert_raises(NoMethodError) { @path_builder.some_method }
                    end

                    it "raises if trying to call [] with no arguments" do
                        assert_raises(ArgumentError) { @path_builder[] }
                    end

                    it "raises if trying to call [] with more than one argument" do
                        assert_raises(ArgumentError) { @path_builder[1, 2] }
                    end

                    it "returns a path builder that points to the given element" do
                        builder = @path_builder[2]
                        assert_equal "somename[2]", builder.__name
                        assert_equal @scalar_t, builder.__type
                        assert_equal [[:call, [:raw_get, 2]]], builder.__path.elements
                    end
                end

                describe "when on an array type" do
                    before do
                        @scalar_t = @registry.create_numeric "/numeric", 4, :sint
                        @type = @registry.create_array "/numeric", 2
                        @path_builder = PathBuilder.new(@type, "somename")
                    end

                    it "reports that it is not terminal" do
                        refute @path_builder.__terminal?
                    end

                    it "responds to []" do
                        assert @path_builder.respond_to?(:[])
                    end

                    it "does not respond to some method" do
                        refute @path_builder.respond_to?(:some_method)
                    end

                    it "raises if trying to call some arbitrary method" do
                        assert_raises(NoMethodError) { @path_builder.some_method }
                    end

                    it "raises if trying to call [] with no arguments" do
                        assert_raises(ArgumentError) { @path_builder[] }
                    end

                    it "raises if trying to call [] with more than one argument" do
                        assert_raises(ArgumentError) { @path_builder[1, 2] }
                    end

                    it "returns a path builder that points to the given element" do
                        builder = @path_builder[1]
                        assert_equal "somename[1]", builder.__name
                        assert_equal @scalar_t, builder.__type
                        assert_equal [[:call, [:raw_get, 1]]], builder.__path.elements
                    end

                    it "raises if the array index is out of bounds" do
                        assert_raises(ArgumentError) { @path_builder[3] }
                    end
                end
            end
        end
    end
end
