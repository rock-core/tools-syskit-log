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

    describe ".compress" do
        before do
            @root = make_tmppath
        end

        it "compresses the input path into the output path" do
            in_path = @root + "in"
            out_path = @root + "out"
            in_path.write("something\n")
            Syskit::Log.compress(in_path, out_path)
            assert_equal "something\n", Zstd.decompress(out_path.read)
        end

        it "optionally computes the data digest on-the-fly" do
            in_path = @root + "in"
            out_path = @root + "out"
            in_path.write("something\n")
            digest = Syskit::Log.compress(in_path, out_path, compute_digest: true)
            assert_equal "something\n", Zstd.decompress(out_path.read)
            assert_equal Digest::SHA256.hexdigest("something\n"), digest
        end
    end

    describe ".decompressed" do
        before do
            root = make_tmppath
            @path = root + "data"
            @path.mkpath
            @cache = root + "cache"
            @cache.mkpath
        end

        it "does nothing if the file is not compressed" do
            file_path = @path + "file"
            FileUtils.touch(file_path.to_s)
            assert_equal file_path, Syskit::Log.decompressed(file_path, @cache)
        end

        it "decompresses the file in the cache path and returns the path" do
            file_path = @path + "file.zst"
            file_path.write Zstd.compress("something\n")
            decompressed = Syskit::Log.decompressed(file_path, @cache)
            assert_equal @cache + "file", decompressed
            assert_equal "something\n", decompressed.read
        end

        it "handles files in subfolders" do
            file_path = @path + "dir" + "file.zst"
            file_path.dirname.mkpath
            file_path.write Zstd.compress("something\n")
            decompressed = Syskit::Log.decompressed(file_path, @cache + "dir")
            assert_equal @cache + "dir" + "file", decompressed
            assert_equal "something\n", decompressed.read
        end

        it "returns an already decompressed file" do
            file_path = @path + "file.zst"
            file_path.write Zstd.compress("something\n")
            decompressed = @cache + "file"
            decompressed.write "somethingelse"

            assert_equal decompressed, Syskit::Log.decompressed(file_path, @cache)
            assert_equal "somethingelse", decompressed.read
        end

        it "does decompress again over an already decompressed file if force is true" do
            file_path = @path + "file.zst"
            file_path.write Zstd.compress("something\n")
            decompressed = @cache + "file"
            decompressed.write "somethingelse"

            result = Syskit::Log.decompressed(file_path, @cache, force: true)
            assert_equal decompressed, result
            assert_equal "something\n", decompressed.read
        end
    end

    describe ".roby_metadata_time_to_seconds" do
        it "parses correctly a plain timestamp" do
            time = Syskit::Log.parse_roby_metadata_time("20241020-1224")
            assert_equal "20241020-1224", time.strftime("%Y%m%d-%H%M")
        end

        it "handles leading zeroes" do
            time = Syskit::Log.parse_roby_metadata_time("20240120-1224")
            assert_equal "20240120-1224", time.strftime("%Y%m%d-%H%M")
        end

        it "ignores the disambiguation suffix" do
            time = Syskit::Log.parse_roby_metadata_time("20240120-1224.1")
            assert_equal "20240120-1224", time.strftime("%Y%m%d-%H%M")
        end
    end
end
