# frozen_string_literal: true

require "test_helper"
require "syskit/log/dsl"

module Syskit
    module Log # :nodoc:
        describe DSL::Summary do
            before do
                @context = Object.new
                @context.extend DSL
                @templates_path = Pathname.new(make_tmpdir)
                @datastore_path = Pathname.new(make_tmpdir)
                @datastore = create_datastore(@datastore_path)

                create_dataset("something") do
                    create_logfile "bla.0.log" do
                        create_logfile_stream(
                            "test", [Time.at(1), Time.at(2)],
                            [Time.at(10), Time.at(20)], [10, 20],
                            metadata: {
                                "rock_task_name" => "task_test",
                                "rock_task_object_name" => "port_test",
                                "rock_stream_type" => "port"
                            }
                        )
                    end
                end
                @context.datastore_select @datastore_path
                @context.dataset_select

                # We test the templates, which are timezone
                # dependent
                @tz = ENV["TZ"]
                ENV["TZ"] = "America/Sao_Paulo"
            end

            after do
                ENV["TZ"] = @tz
            end

            it "renders a template based on the object type" do
                create_template "fixture", "something"
                result = @context.summarize(
                    nil, type: "fixture", templates_path: @templates_path
                )
                assert_equal ["text/html", "something"], result.to_iruby
            end

            it "sets a local variable of the object's type with the object itself" do
                create_template "fixture", "<%= fixture %>"
                result = @context.summarize(
                    "something", type: "fixture", templates_path: @templates_path
                )
                assert_equal ["text/html", "something"], result.to_iruby
            end

            it "provides #summarize to other objects from within the template" do
                create_template(
                    "fixture", "<%= summarize fixture.result, type: 'fixture2' %>"
                )
                create_template("fixture2", "<%= fixture2 %>")
                result = @context.summarize(
                    Struct.new(:result).new("something"),
                    type: "fixture", templates_path: @templates_path
                )
                assert_equal ["text/html", "something"], result.to_iruby
            end

            it "forwards arbitrary options to the template" do
                create_template "fixture", "<%= options[:text] %>"
                result = @context.summarize(
                    nil, type: "fixture", templates_path: @templates_path,
                         text: "something"
                )
                assert_equal ["text/html", "something"], result.to_iruby
            end

            describe "array summary" do
                before do
                    copy_stock_template "array"
                    @fixture_m = Struct.new(:value)
                end

                it "guesses the element types and uses it if the array is small" do
                    create_template "fixture_array",
                                    "<%= fixture_array.map(&:value).inject(&:+) %>"
                    summary = DSL::Summary.new(
                        [@fixture_m.new(22), @fixture_m.new(20)], Time.now,
                        templates_path: @templates_path,
                        array_type_matchers: { @fixture_m => "fixture_array" }
                    )

                    iruby = summary.to_iruby
                    assert_equal "text/html", iruby[0]
                    assert_equal "42", iruby[1].strip
                end

                it "renders each objects in turn in a div if the array is big" do
                    create_template "fixture", "<%= fixture.value %>"
                    type_matchers = DSL::Summary::TYPE_MATCHERS
                                    .merge({ @fixture_m => "fixture" })
                    summary = DSL::Summary.new(
                        [@fixture_m.new(22), @fixture_m.new(20)], Time.now,
                        templates_path: @templates_path, type_matchers: type_matchers,
                        guess_array_type_limit: 0
                    )

                    iruby = summary.to_iruby
                    assert_equal "text/html", iruby[0]
                    assert_equal "<div>22</div><div>20</div>", iruby[1].gsub(/\s/, "")
                end

                it "renders each objects in turn if the array is heterogeneous" do
                    create_template "fixture", "<%= fixture.value %>"
                    create_template "fixture2", "<%= fixture2 * 2 %>"
                    array_type_matchers =
                        { Numeric => "fixture2", @fixture_m => "fixture" }
                    type_matchers =
                        DSL::Summary::TYPE_MATCHERS.merge(array_type_matchers)

                    summary = DSL::Summary.new(
                        [@fixture_m.new(22), 20], Time.now,
                        templates_path: @templates_path,
                        array_type_matchers: array_type_matchers,
                        type_matchers: type_matchers
                    )

                    iruby = summary.to_iruby
                    assert_equal "text/html", iruby[0]
                    assert_equal "<div>22</div><div>40</div>", iruby[1].gsub(/\s/, "")
                end
            end

            it "summarizes a LazyDataStream" do
                mime, body =
                    @context.summarize(@context.task_test_task.port_test_port).to_iruby
                assert_equal "text/html", mime
                assert_equal read_snapshot("summary_data_stream"), body
            end

            it "summarizes a Pocolog::DataStream" do
                create_logfile "bla.0.log" do
                    create_logfile_stream(
                        "test", [Time.at(1), Time.at(2)], [Time.at(10), Time.at(20)],
                        [10, 20]
                    )
                end

                stream = open_logfile_stream "bla.0.log", "test"

                mime, body = @context.summarize(stream).to_iruby
                assert_equal "text/html", mime
                assert_equal read_snapshot("summary_data_stream"), body
            end

            it "summarizes a data stream" do
                create_logfile "bla.0.log" do
                    create_logfile_stream(
                        "test", [Time.at(1), Time.at(2)], [Time.at(10), Time.at(20)],
                        [10, 20]
                    )
                end

                stream = open_logfile_stream "bla.0.log", "test"

                mime, body = @context.summarize(stream).to_iruby
                assert_equal "text/html", mime
                assert_equal read_snapshot("summary_data_stream"), body
            end

            it "summarizes a typelib type" do
                registry = Typelib::CXXRegistry.new
                type = registry.create_compound("/C") do |builder|
                    builder.add "field", "/int32_t"
                end

                mime, body = @context.summarize(type).to_iruby
                assert_equal "text/html", mime
                expected =
                    "<p><prestyle=\"font-size:80%\">/C{field</int32_t>}</pre></p>"
                assert_equal expected, body.gsub(/\s/, "")
            end

            it "summarizes a roby event model by listing its propagations" do
                index = RobySQLIndex::Index.create(logfile_pathname("roby.sql"))
                index.add_roby_log(roby_log_path("accessors"))
                root = RobySQLIndex::Accessors::Root.new(index)

                mime, body = @context.summarize(root.Namespace.M.start_event).to_iruby
                assert_equal "text/html", mime
                assert_equal read_snapshot("summary_event_model").gsub(/\s/, ""),
                             body.gsub(/\s/, "")
            end

            it "allows to optionally list task arguments "\
               "when summarizing an event model" do
                index = RobySQLIndex::Index.create(logfile_pathname("roby.sql"))
                index.add_roby_log(roby_log_path("accessors"))
                root = RobySQLIndex::Accessors::Root.new(index)

                mime, body = @context.summarize(
                    root.Namespace.M.start_event, task_arguments: [:arg]
                ).to_iruby
                assert_equal "text/html", mime
                assert_equal(
                    read_snapshot("summary_event_model_with_arguments").gsub(/\s/, ""),
                    body.gsub(/\s/, "")
                )
            end

            it "summarizes a roby task model by summarizing its instances" do
                index = RobySQLIndex::Index.create(logfile_pathname("roby.sql"))
                index.add_roby_log(roby_log_path("accessors"))
                root = RobySQLIndex::Accessors::Root.new(index)

                mime, body = @context.summarize(root.Namespace.M).to_iruby
                assert_equal "text/html", mime
                assert_equal read_snapshot("summary_task_model").gsub(/\s/, ""),
                             body.gsub(/\s/, "")
            end

            it "allows to optionally list the task arguments "\
               "when summarizing a task model" do
                index = RobySQLIndex::Index.create(logfile_pathname("roby.sql"))
                index.add_roby_log(roby_log_path("accessors"))
                root = RobySQLIndex::Accessors::Root.new(index)

                mime, body = @context.summarize(
                    root.Namespace.M, task_arguments: [:arg]
                ).to_iruby
                assert_equal "text/html", mime
                assert_equal(
                    read_snapshot("summary_task_model_with_arguments").gsub(/\s/, ""),
                    body.gsub(/\s/, "")
                )
            end

            it "summarizes a TaskStreams" do
                port_metadata = {
                    "rock_task_name" => "task", "rock_task_model" => "some::Task",
                    "rock_stream_type" => "port"
                }
                property_metadata = {
                    "rock_task_name" => "task", "rock_task_model" => "some::Task",
                    "rock_stream_type" => "property"
                }
                base_time = Time.at(10_000, 100, :usec)
                create_logfile "test.0.log" do
                    stream = create_logfile_stream "/port0", metadata:
                        port_metadata.merge({ "rock_task_object_name" => "object0" })
                    stream.write base_time + 0, base_time + 10, 0
                    stream.write base_time + 1, base_time + 11, 1
                    create_logfile_stream "/port1", metadata:
                        port_metadata.merge({ "rock_task_object_name" => "object1" })
                    stream = create_logfile_stream "/property0", metadata:
                        property_metadata.merge({ "rock_task_object_name" => "object0" })
                    stream.write base_time + 2, base_time + 12, 0
                    stream.write base_time + 3, base_time + 13, 1
                    create_logfile_stream "/property1", metadata:
                        property_metadata.merge({ "rock_task_object_name" => "object1" })
                end
                streams = Streams.from_dir(logfile_pathname)
                task_streams = streams.find_task_by_name("task")

                mime, body = @context.summarize(task_streams).to_iruby
                assert_equal "text/html", mime
                assert_equal(
                    read_snapshot("summary_task_streams").gsub(/\s/, ""),
                    body.gsub(/\s/, "")
                )
            end

            def copy_stock_template(name)
                src = DSL::Summary::TEMPLATES_PATH
                      .join("summary_#{name}.html.erb").to_s
                FileUtils.cp src.to_s, @templates_path.to_s
            end

            def create_template(name, contents)
                @templates_path.join("summary_#{name}.html.erb").write(contents)
            end

            def read_snapshot(name)
                Pathname.new(__dir__).join("#{name}.snapshot").read
            end
        end
    end
end
