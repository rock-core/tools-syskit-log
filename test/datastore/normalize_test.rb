# frozen_string_literal: true

require "test_helper"
require "syskit/log/datastore/normalize"

module Syskit::Log
    class Datastore
        describe Normalize do
            attr_reader :normalize, :base_time
            before do
                @base_time = Time.new(1980, 9, 30)
                @normalize = Normalize.new(compress: compress?)
            end

            def compress?
                ENV["SYSKIT_LOG_TEST_COMPRESS"] == "1"
            end

            describe "#normalize" do
                before do
                    create_logfile "file0.0.log" do
                        create_logfile_stream "stream0", metadata: Hash["rock_task_name" => "task0", "rock_task_object_name" => "port"]
                        write_logfile_sample base_time + 2, base_time + 20, 2
                        create_logfile_stream "stream1", metadata: Hash["rock_task_name" => "task1", "rock_task_object_name" => "port"]
                        write_logfile_sample base_time + 1, base_time + 10, 1
                    end
                end

                it "splits the file into a one-file-per-stream scheme" do
                    logfile_pathname("normalized").mkdir
                    normalize.normalize([logfile_pathname("file0.0.log")])
                    normalized_dir = logfile_pathname("normalized")
                    stream = open_logfile_stream(
                        normalized_dir + "task0::port.0.log", "task0.port"
                    )
                    assert_equal [[base_time + 2, base_time + 20, 2]],
                                 stream.samples.to_a
                    stream = open_logfile_stream(
                        normalized_dir + "task1::port.0.log", "task1.port"
                    )
                    assert_equal [[base_time + 1, base_time + 10, 1]],
                                 stream.samples.to_a
                end
                it "deletes input files if delete_input is true" do
                    logfile_pathname("normalized").mkdir
                    input_path = logfile_pathname("file0.0.log")
                    normalize.normalize([input_path], delete_input: true)
                    normalized_dir = logfile_pathname("normalized")
                    stream = open_logfile_stream(
                        normalized_dir + "task0::port.0.log", "task0.port"
                    )
                    assert_equal [[base_time + 2, base_time + 20, 2]],
                                 stream.samples.to_a
                    stream = open_logfile_stream(
                        normalized_dir + "task1::port.0.log", "task1.port"
                    )
                    assert_equal [[base_time + 1, base_time + 10, 1]],
                                 stream.samples.to_a

                    refute input_path.exist?
                end
                it "does not delete input files if delete_input is true "\
                   "but the import failed" do
                    logfile_pathname("normalized").mkdir
                    input_path = logfile_pathname("file0.0.log")
                    flexmock(normalize)
                        .should_receive(:normalize_logfile_group)
                        .and_raise(e = Class.new(RuntimeError))
                    assert_raises(e) do
                        normalize.normalize([input_path], delete_input: true)
                    end
                    normalized_dir = logfile_pathname("normalized")
                    refute((normalized_dir + "task0::port.0.log").exist?)
                    refute((normalized_dir + "task1::port.0.log").exist?)
                    assert input_path.exist?
                end
                it "does delete unrelated input files if they have been already "\
                   "processed" do
                    logfile_pathname("normalized").mkdir
                    create_logfile "file1.0.log" do
                        create_logfile_stream "stream2", metadata: Hash["rock_task_name" => "task2", "rock_task_object_name" => "port"]
                        write_logfile_sample base_time + 2, base_time + 20, 2
                    end
                    input0_path = logfile_pathname("file0.0.log")
                    input1_path = logfile_pathname("file1.0.log")
                    flexmock(normalize)
                        .should_receive(:normalize_logfile_group)
                        .once.pass_thru
                    flexmock(normalize)
                        .should_receive(:normalize_logfile_group)
                        .once.and_raise(e = Class.new(RuntimeError))
                    assert_raises(e) do
                        normalize.normalize([input0_path, input1_path], delete_input: true)
                    end
                    assert(logfile_pathname("normalized", "task0::port.0.log").exist?)
                    refute(logfile_pathname("normalized", "task2::port.0.log").exist?)
                    refute input0_path.exist?
                    assert input1_path.exist?
                end
                it "generates valid index files for the normalized streams" do
                    skip if compress?

                    logfile_pathname("normalized").mkdir
                    normalize.normalize([logfile_pathname("file0.0.log")])
                    flexmock(Pocolog::Logfiles).new_instances
                                               .should_receive(:rebuild_and_load_index)
                                               .never
                    normalized_dir = logfile_pathname("normalized")
                    open_logfile_stream (normalized_dir + "task0::port.0.log"), "task0.port"
                    open_logfile_stream (normalized_dir + "task1::port.0.log"), "task1.port"
                end
                it "allows to specify the cache directory" do
                    skip if compress?

                    logfile_pathname("normalized").mkdir
                    index_dir = logfile_pathname("cache")
                    normalize.normalize(
                        [logfile_pathname("file0.0.log")], index_dir: index_dir
                    )
                    flexmock(Pocolog::Logfiles)
                        .new_instances
                        .should_receive(:rebuild_and_load_index)
                        .never
                    normalized_dir = logfile_pathname("normalized")
                    open_logfile_stream (normalized_dir + "task0::port.0.log"), "task0.port", index_dir: index_dir
                    open_logfile_stream (normalized_dir + "task1::port.0.log"), "task1.port", index_dir: index_dir
                end
                describe "digest generation" do
                    it "optionally computes the sha256 digest of the generated file, "\
                       "without the prologue" do
                        logfile_pathname("normalized").mkdir
                        result = normalize.normalize(
                            [logfile_pathname("file0.0.log")], compute_sha256: true
                        )

                        path = logfile_pathname("normalized", "task0::port.0.log")
                        actual_data = read_logfile("normalized", "task0::port.0.log")
                        expected = Digest::SHA256.hexdigest(
                            actual_data[Pocolog::Format::Current::PROLOGUE_SIZE..-1]
                        )
                        assert_equal expected, result[path].hexdigest
                    end
                end
                it "detects followup streams" do
                    create_logfile "file0.1.log" do
                        create_logfile_stream "stream0", metadata: Hash["rock_task_name" => "task0", "rock_task_object_name" => "port"]
                        write_logfile_sample base_time + 3, base_time + 30, 3
                    end
                    normalize.normalize([logfile_pathname("file0.0.log"), logfile_pathname("file0.1.log")])
                    normalized_dir = logfile_pathname("normalized")
                    stream = open_logfile_stream (normalized_dir + "task0::port.0.log"), "task0.port"
                    assert_equal [[base_time + 2, base_time + 20, 2],
                                  [base_time + 3, base_time + 30, 3]], stream.samples.to_a
                end
                it "raises if a potential followup stream has an non-matching realtime range" do
                    create_logfile "file0.1.log" do
                        create_logfile_stream "stream0", metadata: Hash["rock_task_name" => "task0", "rock_task_object_name" => "port"]
                        write_logfile_sample base_time + 1, base_time + 30, 3
                    end
                    capture_io do
                        assert_raises(Normalize::InvalidFollowupStream) do
                            normalize.normalize([logfile_pathname("file0.0.log"), logfile_pathname("file0.1.log")])
                        end
                    end
                end
                it "raises if a potential followup stream has an non-matching logical time range" do
                    create_logfile "file0.1.log" do
                        create_logfile_stream(
                            "stream0",
                            metadata: {
                                "rock_task_name" => "task0",
                                "rock_task_object_name" => "port"
                            }
                        )
                        write_logfile_sample base_time + 3, base_time + 10, 3
                    end
                    capture_io do
                        assert_raises(Normalize::InvalidFollowupStream) do
                            normalize.normalize(
                                [logfile_pathname("file0.0.log"),
                                 logfile_pathname("file0.1.log")]
                            )
                        end
                    end
                end
                it "raises if a potential followup stream has an non-matching type" do
                    create_logfile "file0.1.log" do
                        stream_t = Typelib::Registry.new.create_numeric "/test_t", 8, :sint
                        create_logfile_stream "stream0",
                                              type: stream_t,
                                              metadata: Hash["rock_task_name" => "task0", "rock_task_object_name" => "port"]
                        write_logfile_sample base_time + 3, base_time + 30, 3
                    end
                    capture_io do
                        assert_raises(Normalize::InvalidFollowupStream) do
                            normalize.normalize([logfile_pathname("file0.0.log"), logfile_pathname("file0.1.log")])
                        end
                    end
                end
                it "deletes newly created files if the initialization of a new file fails" do
                    create_logfile "file0.1.log" do
                        create_logfile_stream "stream0",
                                              metadata: Hash["rock_task_name" => "task0", "rock_task_object_name" => "port"]
                        write_logfile_sample base_time + 3, base_time + 30, 3
                    end
                    error_class = Class.new(RuntimeError)
                    flexmock(File).new_instances.should_receive(:write).and_raise(error_class)
                    _out, = capture_io do
                        assert_raises(error_class) do
                            normalize.normalize([logfile_pathname("file0.0.log"), logfile_pathname("file0.1.log")])
                        end
                    end
                    normalized_dir = logfile_pathname("normalized")
                    refute (normalized_dir + "task0::port.0.log").exist?
                end
            end

            describe "#normalize_logfile" do
                it "skips invalid files" do
                    write_logfile "file0.0.log", "INVALID"
                    reporter = flexmock(Pocolog::CLI::NullReporter.new)
                    flexmock(reporter).should_receive(:current).and_return(10)
                    ext = ".zst" if compress?
                    reporter
                        .should_receive(:warn)
                        .with("file0.0.log#{ext} does not seem to be "\
                              "a valid pocolog file, skipping")
                        .once
                    assert_nil normalize.normalize_logfile(
                        logfile_pathname("file0.0.log"),
                        logfile_pathname("normalized"), reporter: reporter
                    )
                end
                it "handles truncated files" do
                    create_logfile "file0.0.log", truncate: 1 do
                        create_logfile_stream "stream0",
                                              metadata: Hash["rock_task_name" => "task0", "rock_task_object_name" => "port"]
                        write_logfile_sample base_time + 3, base_time + 30, 3
                        write_logfile_sample base_time + 4, base_time + 40, 4
                    end
                    file0_path = logfile_pathname("file0.0.log")
                    logfile_pathname("normalized").mkpath
                    reporter = flexmock(Pocolog::CLI::NullReporter.new)
                    flexmock(reporter).should_receive(:current).and_return(10)
                    ext = ".zst" if compress?
                    reporter.should_receive(:warn)
                            .with(/^file0.0.log#{ext} looks truncated/)
                            .once
                    normalize.normalize_logfile(
                        file0_path,
                        logfile_pathname("normalized"), reporter: reporter
                    )
                    stream = open_logfile_stream(
                        logfile_pathname("normalized", "task0::port.0.log"),
                        "task0.port"
                    )
                    assert_equal [[base_time + 3, base_time + 30, 3]],
                                 stream.samples.to_a
                end
            end

            def logfile_pathname(*path)
                return super unless /\.\d+\.log$/.match?(path.last)
                return super unless compress?

                super(*path[0..-2], path.last + ".zst")
            end

            def open_logfile_stream(path, stream_name, **kw)
                return super unless compress?

                Tempfile.open(["", ".log"]) do |temp_io|
                    path = path.sub_ext(".log.zst") if path.extname != ".zst"

                    temp_io.write Zstd.decompress(path.read)
                    temp_io.flush
                    temp_io.rewind
                    return super(temp_io.path, stream_name, **kw)
                end
            end

            def read_logfile(*name)
                path = logfile_pathname(*name)
                data = path.read
                return data unless path.extname == ".zst"

                Zstd.decompress(data)
            end

            def write_logfile(name, data)
                path = logfile_pathname(name)
                data = Zstd.compress(data) if path.extname == ".zst"
                path.write data
            end

            def create_logfile(name, truncate: 0)
                path = Pathname.new(super(name))
                path.truncate(path.stat.size - truncate)
                return path unless compress?

                compressed = Zstd.compress(path.read)
                path.sub_ext(".log.zst").write(compressed)
                path.unlink
                path
            end
        end
    end
end
