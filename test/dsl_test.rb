# frozen_string_literal: true

require "test_helper"
require "syskit/log/dsl"
require "daru"
require "iruby"

HAS_POLARS =
    begin
        require "polars"
        true
    rescue LoadError # rubocop:disable Lint/SuppressedException
    end

require "syskit/log/polars" if HAS_POLARS

module Syskit
    module Log # :nodoc:
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

            describe "datastore selection" do
                it "initializes the datastore to the default if there is one" do
                    ENV["SYSKIT_LOG_STORE"] = @datastore_path.to_s
                    assert_equal @datastore_path,
                                 make_context.datastore.datastore_path
                end

                it "does not initialize the datastore if the environment variable is unset" do
                    ENV.delete("SYSKIT_LOG_STORE")
                    assert_nil make_context.datastore
                end

                it "allows to explicit select one by path" do
                    ENV.delete("SYSKIT_LOG_STORE")
                    context = make_context
                    context.datastore_select @datastore_path
                    assert_equal @datastore_path,
                                 context.datastore.datastore_path
                end
            end

            describe "dataset selection" do
                before do
                    @dataset = create_dataset("exists") {}

                    @context = make_context
                    @context.datastore_select @datastore_path
                end

                it "selects it by complete digest" do
                    @context.dataset_select "exists"
                    assert_equal "exists", @context.dataset.digest
                end

                it "selects it by a partial digest" do
                    @context.dataset_select "exi"
                    assert_equal "exists", @context.dataset.digest
                end

                it "selects it by metadata" do
                    @dataset.metadata_set "key", "value"
                    @dataset.metadata_write_to_file
                    @context.dataset_select "key" => "value"
                    assert_equal "exists", @context.dataset.digest
                end

                it "runs an interactive picker if more than one dataset matches "\
                   "the metadata" do
                    @dataset.metadata_set "key", "value"
                    @dataset.metadata_write_to_file

                    other = create_dataset("something") {}
                    other.metadata_set "key", "value"
                    other.metadata_write_to_file

                    expected_paths = [@dataset, other].map(&:dataset_path).to_set
                    actual_paths = nil
                    flexmock(@context)
                        .should_receive(:__dataset_user_select)
                        .with(->(sets) { actual_paths = sets.map(&:dataset_path) })
                        .and_return(other)

                    @context.dataset_select "key" => "value"
                    assert_equal expected_paths, actual_paths.to_set
                    assert_equal other.digest, @context.dataset.digest
                end

                it "raises if no datasets matches the digest" do
                    assert_raises(ArgumentError) do
                        @context.dataset_select "bla"
                    end
                end

                it "raises if no datasets matches the metadata" do
                    assert_raises(ArgumentError) do
                        @context.dataset_select "bla" => "blo"
                    end
                end

                it "initializes the interval to the dataset interval" do
                    now = create_simple_dataset "with_logs"
                    @context.dataset_select "with_logs"
                    assert_equal now, @context.interval_start
                    assert_equal now + 1, @context.interval_end
                    assert_equal now, @context.interval_zero_time
                end
            end

            describe "#interval_select" do
                before do
                    @context = make_context
                    @context.datastore_select @datastore_path

                    @index = RobySQLIndex::Index.create(logfile_pathname("roby.sql"))
                    @index.add_roby_log(roby_log_path("accessors"))
                    create_simple_dataset "exists"
                    @context.dataset_select "exists"
                    flexmock(@context.dataset).should_receive(roby_sql_index: @index)
                end

                it "resolves two times as an interval" do
                    from, to = @context.interval_select(Time.at(1), Time.at(2))
                    assert_equal from, Time.at(1)
                    assert_equal to, Time.at(2)
                end

                it "resolves a single time as an interval grown by 30s by default" do
                    from, to = @context.interval_select(Time.at(0))
                    assert_equal from, Time.at(0) - 30
                    assert_equal to, Time.at(0) + 30
                end

                it "resolves two event emissions as an interval" do
                    task = @context.roby.Namespace.M.each_task.first
                    from, to = @context.interval_select(
                        task.start_event, task.stop_event
                    )
                    assert_equal from, task.start_event.first.time
                    assert_equal to, task.stop_event.first.time
                end

                it "resolves a single event emission as an interval grown "\
                   "by DEFAULT_GROW" do
                    task = @context.roby.Namespace.M.each_task.first
                    from, to = @context.interval_select(task.start_event.first)
                    assert_equal(
                        from, task.start_event.first.time - DSL::INTERVAL_DEFAULT_GROW
                    )
                    assert_equal(
                        to, task.start_event.first.time + DSL::INTERVAL_DEFAULT_GROW
                    )
                end

                it "grows the interval from a single event emission by the grow "\
                   "parameter if given" do
                    task = @context.roby.Namespace.M.each_task.first
                    from, to = @context.interval_select(task.start_event.first, grow: 10)
                    assert_equal from, task.start_event.first.time - 10
                    assert_equal to, task.start_event.first.time + 10
                end

                it "uses the start event of a task used as first parameter" do
                    task = @context.roby.Namespace.M.each_task.first
                    from, to = @context.interval_select(task, Time.at(200))
                    assert_equal from, task.start_event.first.time
                    assert_equal to, Time.at(200)
                end

                it "uses the stop event of a task used as first parameter" do
                    task = @context.roby.Namespace.M.each_task.first
                    from, to = @context.interval_select(Time.at(0), task)
                    assert_equal from, Time.at(0)
                    assert_equal to, task.stop_event.first.time
                end

                it "uses the start and stop event of a task used as sole parameter" do
                    task = @context.roby.Namespace.M.each_task.first
                    from, to = @context.interval_select(task)
                    assert_equal from, task.start_event.first.time
                    assert_equal to, task.stop_event.first.time
                end

                it "lets the user choose between multiple tasks" do
                    model = @context.roby.Namespace.M
                    flexmock(model)
                        .should_receive(:each_task)
                        .and_return(
                            [flexmock(id: "t1", interval_lg: [Time.at(1), Time.at(2)]),
                             flexmock(id: "t2", interval_lg: [Time.at(3), Time.at(4)])]
                        )
                    flexmock(IRuby).should_receive(:form)
                                   .and_return(
                                       { selected_task: "t2 #{Time.at(3)} #{Time.at(4)}" }
                                   )

                    from, to = @context.interval_select(model)
                    assert_equal from, Time.at(3)
                    assert_equal to, Time.at(4)
                end

                it "lets the user choose between multiple event emissions" do
                    model = @context.roby.Namespace.M.start_event
                    flexmock(model)
                        .should_receive(:each_emission)
                        .and_return(
                            [flexmock(full_name: "e1", time: Time.at(2)),
                             flexmock(full_name: "e2", time: Time.at(3))]
                        )
                    flexmock(IRuby).should_receive(:form)
                                   .and_return(
                                       { selected_event: "e2 #{Time.at(3)}" },
                                       { selected_event: "e1 #{Time.at(2)}" }
                                   )

                    from, to = @context.interval_select(model, model)
                    assert_equal from, Time.at(3)
                    assert_equal to, Time.at(2)
                end
            end

            describe "#samples_of" do
                attr_reader :now

                before do
                    now_nsec = Time.now
                    @now = Time.at(now_nsec.tv_sec, now_nsec.tv_usec)
                    create_dataset "exists" do
                        create_logfile "test.0.log" do
                            create_logfile_stream(
                                "test", metadata: {
                                    "rock_task_name" => "task_test",
                                    "rock_task_object_name" => "port_test",
                                    "rock_stream_type" => "port"
                                }
                            )
                            write_logfile_sample now, now, 10
                            write_logfile_sample now + 10, now + 1, 20
                        end
                    end
                end

                it "returns a port's samples" do
                    @context = make_context
                    @context.datastore_select @datastore_path
                    @context.dataset_select
                    port = @context.task_test_task.port_test_port
                    samples = @context.samples_of(port)
                    expected = [
                        [now, now, 10],
                        [now + 10, now + 1, 20]
                    ]
                    assert_equal expected, samples.enum_for(:each).to_a
                end

                it "restricts the returned object to the defined interval "\
                   "if there is one" do
                    @context = make_context
                    @context.datastore_select @datastore_path
                    @context.dataset_select

                    port = @context.task_test_task.port_test_port
                    @context.interval_select(port)
                    @context.interval_shift_start(0.1)
                    samples = @context.samples_of(port)
                    expected = [
                        [now + 10, now + 1, 20]
                    ]
                    assert_equal expected, samples.enum_for(:each).to_a
                end
            end

            describe "#to_daru_frame" do
                before do
                    now_nsec = Time.now
                    now = Time.at(now_nsec.tv_sec, now_nsec.tv_usec)

                    registry = Typelib::CXXRegistry.new
                    compound_t = registry.create_compound "/C" do |b|
                        b.d = "/double"
                        b.i = "/int"
                    end
                    create_dataset "exists" do
                        create_logfile "test.0.log" do
                            create_logfile_stream(
                                "test", type: compound_t, metadata: {
                                    "rock_task_name" => "task_test",
                                    "rock_task_object_name" => "port_test",
                                    "rock_stream_type" => "port"
                                }
                            )
                            write_logfile_sample now, now, { d: 0.1, i: 1 }
                            write_logfile_sample now + 10, now + 1, { d: 0.2, i: 2 }
                        end

                        create_logfile "test1.0.log" do
                            create_logfile_stream(
                                "test", type: compound_t, metadata: {
                                    "rock_task_name" => "task_test1",
                                    "rock_task_object_name" => "port_test",
                                    "rock_stream_type" => "port"
                                }
                            )
                            write_logfile_sample now, now + 0.1, { d: 0.15, i: 3 }
                            write_logfile_sample now + 10, now + 0.9, { d: 0.25, i: 4 }
                        end
                    end

                    @context = make_context
                    @context.datastore_select @datastore_path
                    @context.dataset_select
                end

                it "creates a frame from a single stream" do
                    port = @context.task_test_task.port_test_port
                    frame = @context.to_daru_frame port do |f|
                        f.add_logical_time
                        f.add(&:d)
                    end

                    assert_equal [0, 1], frame["time"].to_a
                    assert_equal [0.1, 0.2], frame[".d"].to_a
                end

                it "aligns different streams in a single frame" do
                    port = @context.task_test_task.port_test_port
                    port1 = @context.task_test1_task.port_test_port
                    frame = @context.to_daru_frame port, port1 do |a, b|
                        a.add_logical_time("a_time")
                        a.add("a", &:d)
                        b.add("b", &:d)
                        b.add_logical_time("b_time")
                    end

                    assert_equal [0, 1], frame["a_time"].to_a
                    assert_equal [0.1, 0.2], frame["a"].to_a
                    assert_equal [0.1, 0.9], frame["b_time"].to_a
                    assert_equal [0.15, 0.25], frame["b"].to_a
                end
            end

            describe "#to_polars_frame" do
                before do
                    skip "polars-ruby is not installed" unless HAS_POLARS

                    now_nsec = Time.now
                    now = Time.at(now_nsec.tv_sec, now_nsec.tv_usec)
                    now_usec = now.tv_sec * 1_000_000 + now.tv_usec

                    registry = Typelib::CXXRegistry.new
                    compound_t = registry.create_compound "/C" do |b|
                        b.t = "/uint64_t"
                        b.d = "/double"
                        b.i = "/int"
                    end
                    create_dataset "exists" do
                        create_logfile "test.0.log" do
                            create_logfile_stream(
                                "test", type: compound_t, metadata: {
                                    "rock_task_name" => "task_test",
                                    "rock_task_object_name" => "port_test",
                                    "rock_stream_type" => "port"
                                }
                            )
                            write_logfile_sample now, now, { t: now_usec - 500, d: 0.1, i: 1 }
                            write_logfile_sample(
                                now + 10, now + 1, { t: now_usec + 999_500, d: 0.2, i: 2 }
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
                                now, now + 0.1, { t: now_usec + 100_000, d: 0.15, i: 3 }
                            )
                            write_logfile_sample(
                                now + 10, now + 0.9,
                                { t: now_usec + 900_000, d: 0.25, i: 4 }
                            )
                        end
                    end

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
                    frame = @context.to_polars_frame port, port1, chunk_size: 1 do |a, b|
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

            describe "daru_to_vega" do
                before do
                    @context = make_context
                end

                it "converts a daru frame into vega's representation" do
                    frame = ::Daru::DataFrame.new(
                        a: [1, 2, 3],
                        b: [4, 5, 6]
                    )
                    expected = [
                        { a: 1, b: 4 },
                        { a: 2, b: 5 },
                        { a: 3, b: 6 }
                    ]
                    assert_equal expected, @context.daru_to_vega(frame)
                end

                it "converts NaNs into nils" do
                    frame = ::Daru::DataFrame.new(
                        a: [1.0, Float::NAN, 3.0],
                        b: [4.0, 5.0, Float::NAN]
                    )
                    expected = [
                        { a: 1.0, b: 4.0 },
                        { a: nil, b: 5.0 },
                        { a: 3.0, b: nil }
                    ]
                    assert_equal expected, @context.daru_to_vega(frame)
                end

                it "deals with nils in the columns" do
                    frame = ::Daru::DataFrame.new(
                        a: [1.0, Float::NAN, nil],
                        b: [4.0, 5.0, Float::NAN]
                    )
                    expected = [
                        { a: 1.0, b: 4.0 },
                        { a: nil, b: 5.0 },
                        { a: nil, b: nil }
                    ]
                    assert_equal expected, @context.daru_to_vega(frame)
                end

                it "detects floating-point columns even if they start with nil" do
                    frame = ::Daru::DataFrame.new(
                        a: [nil, Float::NAN, 3.0],
                        b: [nil, 5.0, Float::NAN]
                    )
                    expected = [
                        { a: nil, b: nil },
                        { a: nil, b: 5.0 },
                        { a: 3.0, b: nil }
                    ]
                    assert_equal expected, @context.daru_to_vega(frame)
                end

                it "allows to do a simple resampling of the data" do
                    frame = ::Daru::DataFrame.new(
                        a: [nil, Float::NAN, 3.0, 4.0, 5.0],
                        b: [nil, 5.0, Float::NAN, 6.0, 7.0]
                    )
                    expected = [
                        { a: nil, b: nil },
                        { a: 3.0, b: nil },
                        { a: 5.0, b: 7.0 }
                    ]
                    assert_equal expected, @context.daru_to_vega(frame, every: 2)
                end
            end

            describe "#export_to_single_file" do
                before do
                    now_nsec = Time.now
                    @now = now = Time.at(now_nsec.tv_sec, now_nsec.tv_usec)

                    registry = Typelib::CXXRegistry.new
                    @compound_t = compound_t = registry.create_compound "/C" do |b|
                        b.d = "/double"
                        b.i = "/int"
                    end
                    create_dataset "exists" do
                        create_logfile "test.0.log" do
                            create_logfile_stream(
                                "test", type: compound_t, metadata: {
                                    "rock_task_name" => "task_test",
                                    "rock_task_object_name" => "port_test",
                                    "rock_stream_type" => "port"
                                }
                            )
                            write_logfile_sample now, now, { d: 0.1, i: 1 }
                            write_logfile_sample now + 10, now + 1, { d: 0.2, i: 2 }
                        end

                        create_logfile "test1.0.log" do
                            create_logfile_stream(
                                "test", type: compound_t, metadata: {
                                    "rock_task_name" => "task_test1",
                                    "rock_task_object_name" => "port_test",
                                    "rock_stream_type" => "port"
                                }
                            )
                            write_logfile_sample now, now + 0.1, { d: 0.15, i: 3 }
                            write_logfile_sample now + 10, now + 0.9, { d: 0.25, i: 4 }
                        end
                    end

                    @context = make_context
                    @context.datastore_select @datastore_path
                    @context.dataset_select

                    @exported_path = @root_path + "output.0.log"
                end

                it "creates a valid empty file if given no streams at all" do
                    @context.export_to_single_file(@exported_path, upgrade: false) do |f|
                        f.add_logical_time
                        f.add(&:d)
                    end

                    logfile = Pocolog::Logfiles.open(@exported_path.to_s)
                    assert logfile.each_stream.empty?
                end

                it "copies a single stream" do
                    port = @context.task_test_task.port_test_port
                    @context.export_to_single_file(
                        @exported_path, port, upgrade: false
                    ) do |f|
                        f.add("whole_stream")
                    end

                    logfile = Pocolog::Logfiles.open(@exported_path.to_s)
                    stream = logfile.stream("whole_stream")
                    expected = port.samples.map { |_rt, lg, s| [lg, lg, s] }
                    assert_equal expected, stream.samples.to_a
                end

                it "copies the stream metadata" do
                    port = @context.task_test_task.port_test_port
                    @context.export_to_single_file(
                        @exported_path, port, upgrade: false
                    ) do |f|
                        f.add("whole_stream")
                    end

                    logfile = Pocolog::Logfiles.open(@exported_path.to_s)
                    stream = logfile.stream("whole_stream")
                    assert_equal port.metadata, stream.metadata
                end

                it "aligns different streams" do
                    port_a = @context.task_test_task.port_test_port
                    port_b = @context.task_test1_task.port_test_port
                    @context.export_to_single_file(
                        @exported_path, port_a, port_b, upgrade: false
                    ) do |a, b|
                        a.add("a")
                        b.add("b")
                    end

                    logfile = Pocolog::Logfiles.open(@exported_path.to_s)
                    result = logfile.raw_each.to_a
                    expected =
                        (port_a.raw_each.map { |_, time, sample| [0, time, sample] } +
                         port_b.raw_each.map { |_, time, sample| [1, time, sample] })
                        .sort_by { |_, time, _| time }

                    assert_equal expected, result
                end

                it "allows to select subfields" do
                    port = @context.task_test_task.port_test_port
                    @context.export_to_single_file(
                        @exported_path, port, upgrade: false
                    ) do |f|
                        f.add("subfield", &:d)
                    end

                    logfile = Pocolog::Logfiles.open(@exported_path.to_s)
                    stream = logfile.stream("subfield")
                    assert_equal "/double", stream.type.name
                    assert_equal port.samples.map { |_rt, lg, s| [lg, lg, s.d] },
                                 stream.samples.to_a
                end

                it "allows to perform arbitrary transformations" do
                    port = @context.task_test_task.port_test_port
                    registry = Typelib::CXXRegistry.new
                    string_t = registry.get("/std/string")
                    @context.export_to_single_file(
                        @exported_path, port, upgrade: false
                    ) do |f|
                        f.add("transformed") do |s|
                            s.transform(to: string_t) { |c| c.d.to_s }
                        end
                    end

                    logfile = Pocolog::Logfiles.open(@exported_path.to_s)
                    stream = logfile.stream("transformed")
                    assert_equal "/std/string", stream.type.name
                    assert_equal port.samples.map { |_rt, lg, s| [lg, lg, s.d.to_s] },
                                 stream.samples.to_a
                end

                it "upgrades samples to their latest if required" do
                    upgraded_registry = Typelib::CXXRegistry.new
                    upgraded_compound_t = upgraded_registry.create_compound "/D" do |b|
                        b.d = "/std/string"
                    end
                    flexmock(Roby.app.default_loader)
                        .should_receive(:resolve_type)
                        .with("/C").and_return(upgraded_compound_t)
                    @context
                        .datastore
                        .upgrade_converter_registry
                        .add(@now + 1, @compound_t, upgraded_compound_t) do |to, from|
                            to.d = (from.d * 2).to_s
                        end

                    port = @context.task_test_task.port_test_port
                    @context.export_to_single_file(@exported_path, port) do |f|
                        f.add("upgraded")
                    end

                    logfile = Pocolog::Logfiles.open(@exported_path.to_s)
                    stream = logfile.stream("upgraded")
                    assert_equal "/D", stream.type.name
                    expected = port.samples.map do |_rt, lg, s|
                        s = upgraded_compound_t.new(d: (s.d * 2).to_s)
                        [lg, lg, s]
                    end
                    assert_equal expected, stream.samples.to_a
                end
            end

            def make_context
                context = Object.new
                context.extend DSL
                context
            end

            def roby_log_path(name)
                Pathname(__dir__) + "roby-logs" + "#{name}-events.log"
            end

            def create_simple_dataset(name)
                now_nsec = Time.now
                now = Time.at(now_nsec.tv_sec, now_nsec.tv_usec)

                create_dataset(name) do
                    create_logfile "test.0.log" do
                        create_logfile_stream(
                            "test", metadata: {
                                "rock_task_name" => "task_test",
                                "rock_task_object_name" => "port_test",
                                "rock_stream_type" => "port"
                            }
                        )
                        write_logfile_sample now, now, 10
                        write_logfile_sample now + 10, now + 1, 20
                    end
                end

                now
            end
        end
    end
end
