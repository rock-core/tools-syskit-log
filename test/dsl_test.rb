# frozen_string_literal: true

require "test_helper"
require "syskit/log/dsl"
require "daru"
require "iruby"

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
