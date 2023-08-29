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
                    stream = open_logfile_stream(
                        ["normalized", "task0::port.0.log"], "task0.port"
                    )
                    assert_equal [[base_time + 2, base_time + 20, 2]],
                                 stream.samples.to_a
                    stream = open_logfile_stream(
                        ["normalized", "task1::port.0.log"], "task1.port"
                    )
                    assert_equal [[base_time + 1, base_time + 10, 1]],
                                 stream.samples.to_a
                end
                it "deletes input files if delete_input is true" do
                    logfile_pathname("normalized").mkdir
                    input_path = logfile_pathname("file0.0.log")
                    normalize.normalize([input_path], delete_input: true)
                    stream = open_logfile_stream(
                        ["normalized", "task0::port.0.log"], "task0.port"
                    )
                    assert_equal [[base_time + 2, base_time + 20, 2]],
                                 stream.samples.to_a
                    stream = open_logfile_stream(
                        ["normalized", "task1::port.0.log"], "task1.port"
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
                    refute logfile_pathname("normalized", "task0::port.0.log").exist?
                    refute logfile_pathname("normalized", "task1::port.0.log").exist?
                    assert input_path.exist?
                end
                it "does delete unrelated input files if they have been already "\
                   "processed" do
                    logfile_pathname("normalized").mkdir
                    create_logfile "file1.0.log" do
                        create_logfile_stream(
                            "stream2", metadata: {
                                "rock_task_name" => "task2",
                                "rock_task_object_name" => "port"
                            }
                        )
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
                    assert logfile_pathname("normalized", "task0::port.0.log").exist?
                    refute logfile_pathname("normalized", "task2::port.0.log").exist?
                    refute input0_path.exist?
                    assert input1_path.exist?
                end
                describe "digest generation" do
                    it "optionally computes the sha256 digest of the generated file, "\
                       "without the prologue" do
                        logfile_pathname("normalized").mkdir
                        result = normalize.normalize([logfile_pathname("file0.0.log")])

                        path = logfile_pathname("normalized", "task0::port.0.log")
                        actual_data = read_logfile("normalized", "task0::port.0.log")
                        expected = Digest::SHA256.hexdigest(
                            actual_data[Pocolog::Format::Current::PROLOGUE_SIZE..-1]
                        )
                        entry = result.find { |e| e.path == path }
                        assert_equal expected, entry.sha2
                    end
                end
                it "detects followup streams" do
                    create_logfile "file0.1.log" do
                        create_logfile_stream "stream0", metadata: Hash["rock_task_name" => "task0", "rock_task_object_name" => "port"]
                        write_logfile_sample base_time + 3, base_time + 30, 3
                    end
                    normalize.normalize(
                        [logfile_pathname("file0.0.log"), logfile_pathname("file0.1.log")]
                    )
                    stream = open_logfile_stream(
                        ["normalized", "task0::port.0.log"], "task0.port"
                    )
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
            end

            describe "#normalize_logfile" do
                it "skips invalid files" do
                    write_logfile "file0.0.log", "INVALID"
                    reporter = flexmock(NullReporter.new)
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
                        create_logfile_stream(
                            "stream0", metadata: {
                                "rock_task_name" => "task0",
                                "rock_task_object_name" => "port"
                            }
                        )
                        write_logfile_sample base_time + 3, base_time + 30, 3
                        write_logfile_sample base_time + 4, base_time + 40, 4
                    end
                    file0_path = logfile_pathname("file0.0.log")
                    logdir_pathname("normalized").mkpath
                    reporter = flexmock(NullReporter.new)
                    flexmock(reporter).should_receive(:current).and_return(10)
                    ext = ".zst" if compress?
                    reporter.should_receive(:warn)
                            .with(/^file0.0.log#{ext} looks truncated/)
                            .once
                    normalize.normalize_logfile(
                        file0_path,
                        logdir_pathname("normalized"), reporter: reporter
                    )
                    stream = open_logfile_stream(
                        ["normalized", "task0::port.0.log"], "task0.port"
                    )
                    assert_equal [[base_time + 3, base_time + 30, 3]],
                                 stream.samples.to_a
                end
            end
        end
    end
end
