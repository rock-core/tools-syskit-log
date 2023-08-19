# frozen_string_literal: true

require "test_helper"

module Syskit::Log
    describe ".atomic_write" do
        before do
            @root = make_tmppath
            @out_path = @root + "out"
        end

        it "writes the data sent to the IO on the specified output file" do
            Syskit::Log.atomic_write(@out_path) do |temp_io|
                temp_io.puts "something"
            end

            assert_equal "something\n", @out_path.read
        end

        it "overwrites an existing file" do
            @out_path.write "else\n"
            Syskit::Log.atomic_write(@out_path) do |temp_io|
                temp_io.puts "something"
            end

            assert_equal "something\n", (@root + "out").read
        end

        it "does not create the file if the block raises" do
            assert_raises(RuntimeError) do
                Syskit::Log.atomic_write(@out_path) do
                    raise RuntimeError
                end
            end

            refute @out_path.exist?
        end

        it "does not modify an existing file if the block raises" do
            @out_path.write "else\n"
            assert_raises(RuntimeError) do
                Syskit::Log.atomic_write(@out_path) do
                    raise RuntimeError
                end
            end

            assert_equal "else\n", (@root + "out").read
        end

        it "does not create the file if the rename operation raises" do
            flexmock(File).should_receive(:rename).once.and_raise(RuntimeError)
            assert_raises(RuntimeError) do
                Syskit::Log.atomic_write(@out_path) do |temp_io|
                    temp_io.puts "something"
                end
            end

            refute @out_path.exist?
        end

        it "does not modify an existing file if the rename operation raises" do
            @out_path.write "else\n"
            flexmock(File).should_receive(:rename).once.and_raise(RuntimeError)
            assert_raises(RuntimeError) do
                Syskit::Log.atomic_write(@out_path) do |temp_io|
                    temp_io.puts "something"
                end
            end

            assert_equal "else\n", (@root + "out").read
        end
    end
end
