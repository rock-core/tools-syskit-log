# frozen_string_literal: true

HAS_POLARS =
    begin
        require "polars"
        true
    rescue LoadError # rubocop:disable Lint/SuppressedException
    end

require "syskit/log/polars" if HAS_POLARS

module Syskit
    module Log
        module Polars
            describe DSL do
                before do
                    @__default_store = ENV["SYSKIT_LOG_STORE"]
                    ENV.delete("SYSKIT_LOG_STORE")

                    @root_path = Pathname.new(Dir.mktmpdir)
                    @datastore_path = @root_path + "datastore"
                    create_datastore(@datastore_path)
                end

                after do
                    @root_path.rmtree
                    if @__default_store
                        ENV["SYSKIT_LOG_STORE"] = @__default_store
                    else
                        ENV.delete("SYSKIT_LOG_STORE")
                    end
                end

                describe "#to_polars_frame" do
                    before do
                        skip "polars-ruby is not installed" unless HAS_POLARS

                        registry = Typelib::CXXRegistry.new
                        compound_t = registry.create_compound "/C" do |b|
                            b.t = "/uint64_t"
                            b.d = "/double"
                            b.i = "/int"
                        end

                        create_test_dataset(compound_t)

                        @context = make_context
                        @context.datastore_select @datastore_path
                        @context.dataset_select
                    end

                    it "creates a frame from a single stream" do
                        port = @context.task_test_task.port_test_port
                        frame = @context.to_polars_frame port do |f|
                            f.add_logical_time
                            f.add(&:d)
                        end

                        assert_equal [0, 1], frame["time"].to_a
                        assert_equal [0.1, 0.2], frame[".d"].to_a
                    end

                    it "allows overriding the column type" do
                        port = @context.task_test_task.port_test_port
                        port1 = @context.task_test1_task.port_test_port
                        frame = @context.to_polars_frame port, port1 do |a, b|
                            a.add_time_field("t", &:t)
                            a.add("a", dtype: :f32, &:d)
                            b.add("b", &:d)
                        end

                        assert_equal ::Polars::Float32, frame["a"].dtype
                    end

                    it "centers the time fields" do
                        port = @context.task_test_task.port_test_port
                        port1 = @context.task_test1_task.port_test_port
                        frame = @context.to_polars_frame port, port1 do |a, b|
                            a.add_time_field("t", &:t)
                            a.add("a", &:d)
                            b.add("b", &:d)
                        end

                        expected = ::Polars::DataFrame.new(
                            {
                                t: [-0.0005, 0.9995],
                                a: [0.1, 0.2],
                                b: [0.15, 0.25]
                            }, schema: nil
                        ) # 'schema' option for 2.7 compatibility
                        assert_polars_frame_near(expected, frame)
                    end

                    def assert_polars_frame_near(expected, actual)
                        diff = expected - actual
                        max = (diff.max.row(0).to_a + diff.min.row(0).to_a).map(&:abs).max
                        assert_operator max, :<, 1e-6
                    end

                    it "aligns different streams in a single frame" do
                        port = @context.task_test_task.port_test_port
                        port1 = @context.task_test1_task.port_test_port
                        frame = @context.to_polars_frame port, port1 do |a, b|
                            a.add_logical_time("a_time")
                            a.add("a", &:d)
                            b.add("b", &:d)
                            b.add_logical_time("b_time")
                        end

                        expected = ::Polars::DataFrame.new(
                            {
                                a_time: [0, 1],
                                a: [0.1, 0.2],
                                b: [0.15, 0.25],
                                b_time: [0.1, 0.9]
                            }, schema: nil
                        ) # schema for 2.7 compatibility
                        diff = expected - frame
                        max = (diff.max.row(0).to_a + diff.min.row(0).to_a).map(&:abs).max
                        assert_operator max, :<, 1e-6
                    end

                    it "handles datasets bigger than the chunk size" do
                        port = @context.task_test_task.port_test_port
                        port1 = @context.task_test1_task.port_test_port
                        frame = @context.to_polars_frame(
                            port, port1, chunk_size: 1
                        ) do |a, b|
                            a.add_logical_time("a_time")
                            a.add("a", &:d)
                            b.add("b", &:d)
                            b.add_logical_time("b_time")
                        end

                        expected = ::Polars::DataFrame.new(
                            {
                                a_time: [0, 1],
                                a: [0.1, 0.2],
                                b: [0.15, 0.25],
                                b_time: [0.1, 0.9]
                            }, schema: nil
                        ) # schema for 2.7 compatibility
                        diff = expected - frame
                        max = (diff.max.row(0).to_a + diff.min.row(0).to_a).map(&:abs).max
                        assert_operator max, :<, 1e-6
                    end
                end

                describe "polars_to_vega" do
                    before do
                        @context = make_context
                    end

                    it "converts NaN into nil" do
                        frame = ::Polars::DataFrame.new(
                            { "a" => [Float::NAN, 0.1] }, schema: nil
                        )
                        vega = @context.polars_to_vega(frame)
                        assert_equal [{ "a" => nil }, { "a" => 0.1 }], vega
                    end
                end

                def create_test_dataset(compound_t) # rubocop:disable Metrics/AbcSize
                    now_nsec = Time.now
                    now = Time.at(now_nsec.tv_sec, now_nsec.tv_usec)
                    now_usec = now.tv_sec * 1_000_000 + now.tv_usec

                    create_dataset "exists" do # rubocop:disable Metrics/BlockLength
                        create_logfile "test.0.log" do
                            create_logfile_stream(
                                "test", type: compound_t, metadata: {
                                    "rock_task_name" => "task_test",
                                    "rock_task_object_name" => "port_test",
                                    "rock_stream_type" => "port"
                                }
                            )
                            write_logfile_sample(
                                now, now, { t: now_usec - 500, d: 0.1, i: 1 }
                            )
                            write_logfile_sample(
                                now + 10, now + 1,
                                { t: now_usec + 999_500, d: 0.2, i: 2 }
                            )
                        end

                        create_logfile "test1.0.log" do
                            create_logfile_stream(
                                "test", type: compound_t, metadata: {
                                    "rock_task_name" => "task_test1",
                                    "rock_task_object_name" => "port_test",
                                    "rock_stream_type" => "port"
                                }
                            )
                            write_logfile_sample(
                                now, now + 0.1,
                                { t: now_usec + 100_000, d: 0.15, i: 3 }
                            )
                            write_logfile_sample(
                                now + 10, now + 0.9,
                                { t: now_usec + 900_000, d: 0.25, i: 4 }
                            )
                        end
                    end
                end

                def make_context
                    context = Object.new
                    context.extend Syskit::Log::DSL
                    context
                end
            end
        end
    end
end
