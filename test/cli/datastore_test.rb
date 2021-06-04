# frozen_string_literal: true

require "test_helper"
require "syskit/log/cli/datastore"

module Syskit::Log
    module CLI
        describe Datastore do
            attr_reader :root_path, :datastore_path, :datastore
            before do
                @root_path = Pathname.new(Dir.mktmpdir)
                move_logfile_path((root_path + "logs" + "test").to_s)
                @datastore_path = root_path + "datastore"
                @datastore = datastore_m.create(datastore_path)
            end

            def datastore_m
                Syskit::Log::Datastore
            end

            after do
                root_path.rmtree
            end

            def capture_io
                FlexMock.use(TTY::Color) do |tty_color|
                    tty_color.should_receive(:color?).and_return(false)
                    super do
                        yield
                    end
                end
            end

            # Helper method to call a CLI subcommand
            def call_cli(*args, silent: true)
                extra_args = []
                extra_args << "--colors=f" << "--progress=f"
                extra_args << "--silent" if silent
                Datastore.start([*args, *extra_args], debug: true)
            end

            describe "#import" do
                it "imports a single dataset into the store" do
                    incoming_path = datastore_path + "incoming" + "0"
                    flexmock(datastore_m::Import)
                        .new_instances.should_receive(:normalize_dataset)
                        .with(
                            [logfile_pathname], incoming_path + "core",
                            on do |h|
                                h[:cache_path] == incoming_path + "cache" &&
                                h[:reporter].kind_of?(Pocolog::CLI::NullReporter)
                            end
                        )
                        .once.pass_thru
                    expected_dataset = lambda do |s|
                        assert_equal incoming_path + "core", s.dataset_path
                        assert_equal incoming_path + "cache", s.cache_path
                        true
                    end
                    flexmock(datastore_m::Import)
                        .new_instances.should_receive(:move_dataset_to_store)
                        .with(expected_dataset)
                        .once.pass_thru

                    call_cli("import", "--min-duration=0",
                             "--store", datastore_path.to_s, logfile_pathname.to_s,
                             silent: true)
                end

                it "optionally sets tags, description and arbitraty metadata" do
                    call_cli("import", "--min-duration=0",
                             "--store", datastore_path.to_s, logfile_pathname.to_s,
                             "some description", "--tags", "test", "tags",
                             "--metadata", "key0=value0a", "key0=value0b", "key1=value1",
                             silent: true)

                    dataset = Syskit::Log::Datastore.new(datastore_path)
                                                    .each_dataset.first
                    assert_equal ["some description"],
                                 dataset.metadata_fetch_all("description").to_a
                    assert_equal %w[test tags],
                                 dataset.metadata_fetch_all("tags").to_a
                    assert_equal %w[value0a value0b],
                                 dataset.metadata_fetch_all("key0").to_a
                    assert_equal %w[value1],
                                 dataset.metadata_fetch_all("key1").to_a
                end

                describe "--auto" do
                    it "creates the datastore path" do
                        datastore_path.rmtree
                        call_cli("import", "--auto", "--store", datastore_path.to_s,
                                 root_path.to_s)
                        assert datastore_path.exist?
                    end
                    it "auto-imports any directory that looks like a raw dataset" do
                        create_logfile("test.0.log") {}
                        FileUtils.touch logfile_path("test-events.log")
                        incoming_path = datastore_path + "incoming" + "0"
                        flexmock(datastore_m::Import)
                            .new_instances.should_receive(:normalize_dataset)
                            .with(
                                [logfile_pathname], incoming_path + "core",
                                on do |h|
                                    h[:cache_path] == incoming_path + "cache" &&
                                    h[:reporter].kind_of?(Pocolog::CLI::NullReporter)
                                end
                            )
                            .once.pass_thru
                        expected_dataset = lambda do |s|
                            assert_equal incoming_path + "core", s.dataset_path
                            assert_equal incoming_path + "cache", s.cache_path
                            true
                        end

                        flexmock(datastore_m::Import)
                            .new_instances.should_receive(:move_dataset_to_store)
                            .with(expected_dataset)
                            .once.pass_thru

                        call_cli("import", "--auto", "--min-duration=0",
                                 "--store", datastore_path.to_s,
                                 logfile_pathname.dirname.to_s, silent: true)
                        digest, = datastore_m::Import.find_import_info(logfile_pathname)
                        assert datastore.has?(digest)
                    end
                    it "ignores datasets that have already been imported" do
                        create_logfile("test.0.log") do
                            create_logfile_stream "test", metadata: Hash["rock_task_name" => "task", "rock_task_object_name" => "port"]
                            write_logfile_sample Time.now, Time.now, 10
                            write_logfile_sample Time.now + 10, Time.now + 1, 20
                        end
                        FileUtils.touch logfile_path("test-events.log")
                        call_cli("import", "--auto", "--min-duration=0",
                                 "--store", datastore_path.to_s,
                                 logfile_pathname.dirname.to_s, silent: true)
                        flexmock(datastore_m::Import)
                            .new_instances.should_receive(:normalize_dataset)
                            .never
                        flexmock(datastore_m::Import)
                            .new_instances.should_receive(:move_dataset_to_store)
                            .never
                        out, = capture_io do
                            call_cli("import", "--auto", "--min-duration=0",
                                     "--store", datastore_path.to_s,
                                     logfile_pathname.dirname.to_s, silent: false)
                        end
                        assert_match(/#{logfile_pathname} already seem to have been imported as .*Give --force/,
                                     out)
                    end
                    it "processes datasets that have already been imported if --force is given" do
                        create_logfile("test.0.log") do
                            create_logfile_stream "test", metadata: Hash["rock_task_name" => "task", "rock_task_object_name" => "port"]
                            write_logfile_sample Time.now, Time.now, 10
                            write_logfile_sample Time.now + 10, Time.now + 1, 20
                        end
                        FileUtils.touch logfile_path("test-events.log")
                        call_cli("import", "--auto", "--min-duration=0",
                                 "--store", datastore_path.to_s,
                                 logfile_pathname.dirname.to_s, silent: true)
                        flexmock(datastore_m::Import)
                            .new_instances.should_receive(:normalize_dataset)
                            .once.pass_thru
                        flexmock(datastore_m::Import)
                            .new_instances.should_receive(:move_dataset_to_store)
                            . once.pass_thru
                        capture_io do
                            call_cli("import", "--auto", "--min-duration=0", "--force",
                                     "--store", datastore_path.to_s,
                                     logfile_pathname.dirname.to_s, silent: false)
                        end
                    end
                    it "ignores datasets that do not seem to be already imported, but are" do
                        create_logfile("test.0.log") do
                            create_logfile_stream "test", metadata: Hash["rock_task_name" => "task", "rock_task_object_name" => "port"]
                            write_logfile_sample Time.now, Time.now, 10
                            write_logfile_sample Time.now + 10, Time.now + 1, 20
                        end
                        FileUtils.touch logfile_path("test-events.log")
                        call_cli("import", "--auto", "--min-duration=0",
                                 "--store", datastore_path.to_s,
                                 logfile_pathname.dirname.to_s, silent: true)
                        (logfile_pathname + datastore_m::Import::BASENAME_IMPORT_TAG)
                            .unlink
                        flexmock(datastore_m::Import)
                            .new_instances.should_receive(:normalize_dataset)
                            .once.pass_thru
                        flexmock(datastore_m::Import)
                            .new_instances.should_receive(:move_dataset_to_store)
                            .never
                        out, = capture_io do
                            call_cli("import", "--auto", "--min-duration=0",
                                     "--store", datastore_path.to_s,
                                     logfile_pathname.dirname.to_s, silent: false)
                        end
                        assert_match(/#{logfile_pathname} already seem to have been imported as .*Give --force/,
                                     out)
                    end
                    it "imports datasets that do not seem to be already imported, but are if --force is given" do
                        create_logfile("test.0.log") do
                            create_logfile_stream "test", metadata: Hash["rock_task_name" => "task", "rock_task_object_name" => "port"]
                            write_logfile_sample Time.now, Time.now, 10
                            write_logfile_sample Time.now + 10, Time.now + 1, 20
                        end
                        FileUtils.touch logfile_path("test-events.log")
                        call_cli("import", "--auto", "--min-duration=0",
                                 "--store", datastore_path.to_s,
                                 logfile_pathname.dirname.to_s, silent: true)
                        digest, _ = datastore_m::Import.find_import_info(logfile_pathname)
                        marker_path = datastore.core_path_of(digest) + "marker"
                        FileUtils.touch(marker_path)
                        (logfile_pathname + datastore_m::Import::BASENAME_IMPORT_TAG).unlink
                        flexmock(datastore_m::Import)
                            .new_instances.should_receive(:normalize_dataset)
                            .once.pass_thru
                        flexmock(datastore_m::Import)
                            .new_instances.should_receive(:move_dataset_to_store)
                            .once.pass_thru
                        out, = capture_io do
                            call_cli("import", "--auto", "--force", "--min-duration=0",
                                     "--store", datastore_path.to_s,
                                     logfile_pathname.dirname.to_s, silent: false)
                        end
                        assert_match(/Replacing existing dataset #{digest} with new one/, out)
                        refute marker_path.exist?
                    end
                    it "ignores an empty dataset after normalization if --min-duration "\
                       "is non-zero" do
                        create_logfile("test.0.log") {}
                        FileUtils.touch logfile_path("test-events.log")
                        flexmock(datastore_m::Import)
                            .new_instances.should_receive(:normalize_dataset)
                            .once.pass_thru
                        flexmock(datastore_m::Import)
                            .new_instances.should_receive(:move_dataset_to_store)
                            .never

                        call_cli("import", "--auto", "--min-duration=1",
                                 "--store", datastore_path.to_s,
                                 logfile_pathname.dirname.to_s, silent: true)
                    end
                    it "ignores datasets whose logical duration is "\
                       "lower than --min-duration" do
                        create_logfile("test.0.log") do
                            create_logfile_stream(
                                "test", metadata: { "rock_task_name" => "task",
                                                    "rock_task_object_name" => "port" }
                            )
                            write_logfile_sample Time.now, Time.now, 10
                            write_logfile_sample Time.now + 10, Time.now + 1, 20
                        end
                        FileUtils.touch logfile_path("test-events.log")
                        flexmock(datastore_m::Import)
                            .new_instances.should_receive(:normalize_dataset)
                            .once.pass_thru
                        flexmock(datastore_m::Import)
                            .new_instances.should_receive(:move_dataset_to_store)
                            .never

                        out, = capture_io do
                            call_cli("import", "--auto", "--min-duration=5",
                                     "--store", datastore_path.to_s,
                                     logfile_pathname.dirname.to_s,
                                     silent: false)
                        end
                        assert_match(/#{logfile_pathname} lasts only 1.0s, ignored/, out)
                    end
                end
            end

            describe "#normalize" do
                it "normalizes the logfiles in the input directory into the directory provided as 'out'" do
                    create_logfile("test.0.log") {}
                    out_path = root_path + "normalized"
                    flexmock(Syskit::Log::Datastore).should_receive(:normalize)
                                                    .with([logfile_pathname("test.0.log")], hsh(output_path: out_path))
                                                    .once.pass_thru
                    call_cli("normalize", logfile_pathname.to_s, "--out=#{out_path}", silent: true)
                end
                it "reports progress without --silent" do
                    create_logfile("test.0.log") {}
                    out_path = root_path + "normalized"
                    flexmock(Syskit::Log::Datastore).should_receive(:normalize)
                                                    .with([logfile_pathname("test.0.log")], hsh(output_path: out_path))
                                                    .once.pass_thru
                    capture_io do
                        call_cli("normalize", logfile_pathname.to_s, "--out=#{out_path}", silent: false)
                    end
                end
            end

            describe "#index" do
                before do
                    create_dataset "a" do
                        create_logfile("test.0.log") {}
                    end
                    create_dataset "b" do
                        create_logfile("test.0.log") {}
                    end
                end

                def expected_store
                    ->(store) { store.datastore_path == datastore_path }
                end

                def expected_dataset(digest)
                    ->(dataset) { dataset.dataset_path == datastore.get(digest).dataset_path }
                end

                it "runs the indexer on all datasets of the store if none are provided on the command line" do
                    flexmock(Syskit::Log::Datastore)
                        .should_receive(:index_build)
                        .with(expected_store, expected_dataset("a"), Hash).once
                        .pass_thru
                    flexmock(Syskit::Log::Datastore)
                        .should_receive(:index_build)
                        .with(expected_store, expected_dataset("b"), Hash).once
                        .pass_thru
                    call_cli("index", "--store", datastore_path.to_s)
                end
                it "runs the indexer on the datasets of the store specified on the command line" do
                    flexmock(Syskit::Log::Datastore)
                        .should_receive(:index_build)
                        .with(expected_store, expected_dataset("a"), Hash).once
                        .pass_thru
                    call_cli("index", "--store", datastore_path.to_s, "a")
                end
            end

            describe "#path" do
                before do
                    @a0ea_dataset = create_dataset(
                        "a0ea", metadata: {
                            "description" => "first",
                            "test" => %w[2], "common" => %w[tag],
                            "array_test" => %w[a b]
                        }
                    ) {}
                    @a0fa_dataset = create_dataset(
                        "a0fa", metadata: {
                            "test" => %w[1], "common" => %w[tbg],
                            "array_test" => %w[c d]
                        }
                    ) {}
                end

                it "lists the path to the given dataset digest" do
                    out, = capture_io do
                        call_cli("path", "--store", datastore_path.to_s,
                                 "a0ea", silent: false)
                    end
                    assert_equal "a0ea #{@a0ea_dataset.dataset_path}", out.chomp
                end

                it "lists all matching datasets" do
                    out, = capture_io do
                        call_cli("path", "--store", datastore_path.to_s,
                                 "common~t.g", silent: false)
                    end
                    assert_equal <<~OUTPUT, out
                        a0ea #{@a0ea_dataset.dataset_path}
                        a0fa #{@a0fa_dataset.dataset_path}
                    OUTPUT
                end
            end

            describe "#list" do
                attr_reader :show_a0ea, :show_a0fa, :base_time
                before do
                    @base_time = Time.at(34200, 234)
                    create_dataset "a0ea", metadata: Hash["description" => "first", "test" => ["2"], "array_test" => %w[a b]] do
                        create_logfile("test.0.log") do
                            create_logfile_stream "test", metadata: Hash["rock_stream_type" => "port", "rock_task_name" => "task0", "rock_task_object_name" => "port0", "rock_task_model" => "test::Task"]
                            write_logfile_sample base_time, base_time, 0
                            write_logfile_sample base_time + 1, base_time + 10, 1
                        end
                        create_logfile("test_property.0.log") do
                            create_logfile_stream "test_property", metadata: Hash["rock_stream_type" => "property", "rock_task_name" => "task0", "rock_task_object_name" => "property0", "rock_task_model" => "test::Task"]
                            write_logfile_sample base_time, base_time + 1, 2
                            write_logfile_sample base_time + 1, base_time + 9, 3
                        end
                    end
                    create_dataset "a0fa", metadata: Hash["test" => ["1"], "array_test" => %w[c d]] do
                        create_logfile("test.0.log") do
                            create_logfile_stream "test", metadata: Hash["rock_stream_type" => "port", "rock_task_name" => "task0", "rock_task_object_name" => "port0", "rock_task_model" => "test::Task"]
                        end
                        create_logfile("test_property.0.log") do
                            create_logfile_stream "test_property", metadata: Hash["rock_stream_type" => "property", "rock_task_name" => "task0", "rock_task_object_name" => "property0", "rock_task_model" => "test::Task"]
                        end
                    end
                    @show_a0ea = <<-EOF
a0ea first
  test: 2
  array_test:
  - a
  - b
  timestamp: 34200
                    EOF
                    @show_a0fa = <<-EOF
a0fa <no description>
  test: 1
  array_test:
  - c
  - d
  timestamp: 0
                    EOF
                end

                it "raises if the query is invalid" do
                    assert_raises(Syskit::Log::Datastore::Dataset::InvalidDigest) do
                        call_cli("list", "--store", datastore_path.to_s,
                                 "not_a_sha", silent: false)
                    end
                end

                it "lists all datasets if given only the datastore path" do
                    out, _err = capture_io do
                        call_cli("list", "--store", datastore_path.to_s, silent: false)
                    end
                    assert_equal [show_a0fa, show_a0ea].join, out
                end
                it "lists only the short digests if --digest is given" do
                    out, _err = capture_io do
                        call_cli("list", "--store", datastore_path.to_s,
                                 "--digest", silent: false)
                    end
                    assert_equal "a0fa\na0ea\n", out
                end
                it "lists only the short digests if --digest --long-digests are given" do
                    out, _err = capture_io do
                        call_cli("list", "--store", datastore_path.to_s,
                                 "--digest", "--long-digests", silent: false)
                    end
                    assert_equal "a0fa\na0ea\n", out
                end
                it "accepts a digest prefix as argument" do
                    out, _err = capture_io do
                        call_cli("list", "--store", datastore_path.to_s,
                                 "a0e", silent: false)
                    end
                    assert_equal show_a0ea, out
                end
                it "can match metadata exactly" do
                    out, _err = capture_io do
                        call_cli("list", "--store", datastore_path.to_s,
                                 "test=1", silent: false)
                    end
                    assert_equal show_a0fa, out
                end
                it "can match metadata with a regexp" do
                    out, _err = capture_io do
                        call_cli("list", "--store", datastore_path.to_s,
                                 "array_test~[ac]", silent: false)
                    end
                    assert_equal [show_a0fa, show_a0ea].join, out
                end

                describe "--pocolog" do
                    it "shows the pocolog stream information" do
                        out, _err = capture_io do
                            call_cli("list", "--store", datastore_path.to_s,
                                     "a0e", "--pocolog", silent: false)
                        end
                        pocolog_info = <<-EOF
  1 oroGen tasks in 2 streams
    task0[test::Task]: 1 ports and 1 properties
    Ports:
      port0:     2 samples from 1970-01-01 06:30:00.000234 -0300 to 1970-01-01 06:30:10.000234 -0300 [   0:00:10.000000]
    Properties:
      property0: 2 samples from 1970-01-01 06:30:01.000234 -0300 to 1970-01-01 06:30:09.000234 -0300 [   0:00:08.000000]
                        EOF
                        assert_equal (show_a0ea + pocolog_info), out
                    end
                    it "handles empty streams gracefully" do
                        out, _err = capture_io do
                            call_cli("list", "--store", datastore_path.to_s,
                                     "a0f", "--pocolog", silent: false)
                        end
                        pocolog_info = <<-EOF
  1 oroGen tasks in 2 streams
    task0[test::Task]: 1 ports and 1 properties
    Ports:
      port0:     empty
    Properties:
      property0: empty
                        EOF
                        assert_equal (show_a0fa + pocolog_info), out
                    end
                end
            end

            describe "#metadata" do
                before do
                    create_dataset "a0ea", metadata: Hash["test" => ["a"]] do
                        create_logfile("test.0.log") {}
                    end
                    create_dataset "a0fa", metadata: Hash["test" => ["b"]] do
                        create_logfile("test.0.log") {}
                    end
                end

                it "raises if the query is invalid" do
                    assert_raises(Syskit::Log::Datastore::Dataset::InvalidDigest) do
                        call_cli("metadata", "--store", datastore_path.to_s,
                                 "not_a_sha", "--get", silent: false)
                    end
                end

                describe "--set" do
                    it "sets metadata on the given dataset" do
                        call_cli("metadata", "--store", datastore_path.to_s,
                                 "a0e", "--set", "debug=true", silent: false)
                        assert_equal Set["true"], datastore.get("a0ea").metadata["debug"]
                        assert_nil datastore.get("a0fa").metadata["debug"]
                    end
                    it "sets metadata on matching datasets" do
                        call_cli("metadata", "--store", datastore_path.to_s, "test=b", "--set", "debug=true", silent: false)
                        assert_nil datastore.get("a0ea").metadata["debug"]
                        assert_equal Set["true"], datastore.get("a0fa").metadata["debug"]
                    end
                    it "sets metadata on all datasets if no query is given" do
                        call_cli("metadata", "--store", datastore_path.to_s, "--set", "debug=true", silent: false)
                        assert_equal Set["true"], datastore.get("a0ea").metadata["debug"]
                        assert_equal Set["true"], datastore.get("a0fa").metadata["debug"]
                    end
                    it "collects all set arguments with the same key" do
                        call_cli("metadata", "--store", datastore_path.to_s, "--set", "test=a", "test=b", "test=c", silent: false)
                        assert_equal Set["a", "b", "c"], datastore.get("a0ea").metadata["test"]
                    end
                    it "raises if the argument to set is not a key=value association" do
                        assert_raises(ArgumentError) do
                            call_cli("metadata", "--store", datastore_path.to_s, "a0ea", "--set", "debug", silent: false)
                        end
                    end
                end

                describe "--get" do
                    it "lists all metadata on all datasets if no query is given" do
                        call_cli("metadata", "--store", datastore_path.to_s, "a0ea", "--set", "test=a,b", silent: false)
                        out, _err = capture_io do
                            call_cli("metadata", "--store", datastore_path.to_s, "--get", silent: false)
                        end
                        assert_equal "a0ea test=a,b\na0fa test=b\n", out
                    end
                    it "displays the short digest by default" do
                        flexmock(Syskit::Log::Datastore).new_instances.should_receive(:short_digest)
                                                        .and_return { |dataset| dataset.digest[0, 3] }
                        out, _err = capture_io do
                            call_cli("metadata", "--store", datastore_path.to_s, "--get", silent: false)
                        end
                        assert_equal "a0e test=a\na0f test=b\n", out
                    end
                    it "displays the long digest if --long-digest is given" do
                        flexmock(datastore).should_receive(:short_digest).never
                        out, _err = capture_io do
                            call_cli("metadata", "--store", datastore_path.to_s, "--get", "--long-digest", silent: false)
                        end
                        assert_equal "a0ea test=a\na0fa test=b\n", out
                    end
                    it "lists the requested metadata of the matching datasets" do
                        call_cli("metadata", "--store", datastore_path.to_s, "a0ea", "--set", "test=a,b", "debug=true", silent: false)
                        out, _err = capture_io do
                            call_cli("metadata", "--store", datastore_path.to_s, "a0ea", "--get", "test", silent: false)
                        end
                        assert_equal "a0ea test=a,b\n", out
                    end
                    it "replaces requested metadata that are unset by <unset>" do
                        out, _err = capture_io do
                            call_cli("metadata", "--store", datastore_path.to_s, "a0ea", "--get", "debug", silent: false)
                        end
                        assert_equal "a0ea debug=<unset>\n", out
                    end
                end

                it "raises if both --get and --set are provided" do
                    assert_raises(ArgumentError) do
                        call_cli("metadata", "--store", datastore_path.to_s, "a0ea", "--get", "debug", "--set", "test=10", silent: false)
                    end
                end

                it "raises if neither --get nor --set are provided" do
                    assert_raises(ArgumentError) do
                        call_cli("metadata", "--store", datastore_path.to_s, "a0ea", silent: false)
                    end
                end
            end

            describe "#find-streams" do
                attr_reader :show_a0ea, :show_a0fa, :base_time
                before do
                    @base_time = Time.at(34200, 234)
                    create_dataset "a0ea", metadata: Hash["description" => "first", "test" => ["2"], "array_test" => %w[a b]] do
                        create_logfile("test.0.log") do
                            create_logfile_stream "test", metadata: Hash["rock_stream_type" => "port", "rock_task_name" => "task0", "rock_task_object_name" => "port0", "rock_task_model" => "test::Task"]
                            write_logfile_sample base_time, base_time, 0
                            write_logfile_sample base_time + 1, base_time + 10, 1
                        end
                        create_logfile("test_property.0.log") do
                            create_logfile_stream "test_property", metadata: Hash["rock_stream_type" => "property", "rock_task_name" => "task0", "rock_task_object_name" => "property0", "rock_task_model" => "test::Task"]
                            write_logfile_sample base_time, base_time + 1, 2
                            write_logfile_sample base_time + 1, base_time + 9, 3
                        end
                    end
                    create_dataset "a0fa", metadata: Hash["test" => ["1"], "array_test" => %w[c d]] do
                        create_logfile("test.0.log") do
                            create_logfile_stream "test", metadata: Hash["rock_stream_type" => "port", "rock_task_name" => "task0", "rock_task_object_name" => "port0", "rock_task_model" => "test::Task"]
                        end
                        create_logfile("test_property.0.log") do
                            create_logfile_stream "test_property", metadata: Hash["rock_stream_type" => "property", "rock_task_name" => "task0", "rock_task_object_name" => "property0", "rock_task_model" => "test::Task"]
                        end
                    end
                end

                it "lists all streams that match the query" do
                    out, _err = capture_io do
                        call_cli("find-streams", "--store", datastore_path.to_s,
                                 "object_name=port0", silent: false)
                    end
                    assert_equal <<~EXPECTED.chomp, out.split("\n").map(&:strip).join("\n")
                        test: 2 samples from 1970-01-01 06:30:00.000234 -0300 to 1970-01-01 06:30:10.000234 -0300 [   0:00:10.000000]
                        test: empty
                    EXPECTED
                end

                it "filters ports" do
                    out, _err = capture_io do
                        call_cli("find-streams", "--store", datastore_path.to_s,
                                 "ports", silent: false)
                    end
                    assert_equal <<~EXPECTED.chomp, out.split("\n").map(&:strip).join("\n")
                        test: 2 samples from 1970-01-01 06:30:00.000234 -0300 to 1970-01-01 06:30:10.000234 -0300 [   0:00:10.000000]
                        test: empty
                    EXPECTED
                end

                it "filters properties" do
                    out, _err = capture_io do
                        call_cli("find-streams", "--store", datastore_path.to_s,
                                 "properties", silent: false)
                    end
                    assert_equal <<~EXPECTED.chomp, out.split("\n").map(&:strip).join("\n")
                        test_property: 2 samples from 1970-01-01 06:30:01.000234 -0300 to 1970-01-01 06:30:09.000234 -0300 [   0:00:08.000000]
                        test_property: empty
                    EXPECTED
                end

                it "handles partial matches" do
                    out, _err = capture_io do
                        call_cli("find-streams", "--store", datastore_path.to_s,
                                 "object_name~^p", silent: false)
                    end
                    assert_equal <<~EXPECTED.chomp, out.split("\n").map(&:strip).join("\n")
                        test:          2 samples from 1970-01-01 06:30:00.000234 -0300 to 1970-01-01 06:30:10.000234 -0300 [   0:00:10.000000]
                        test_property: 2 samples from 1970-01-01 06:30:01.000234 -0300 to 1970-01-01 06:30:09.000234 -0300 [   0:00:08.000000]
                        test:          empty
                        test_property: empty
                    EXPECTED
                end
            end

            describe "#repair" do
                before do
                    fixture_store_path =
                        Pathname.new(__dir__)
                                .join("..", "datastore", "fixtures", "repair")
                    @store_path = @root_path + "store"
                    FileUtils.cp_r fixture_store_path, @store_path

                    @datastore = datastore_m.new(@store_path)
                end

                it "repairs the old roby-events.log name" do
                    o = "dfbaf485f019ade11bfc9c11aeed90e2510c9994cbe6dade52b031679aafd624"
                    n = "049d95329290cfa6aebe917ae0037fa8fd619f3dacce083e94b7b3e5002bb2f2"
                    run_repair o
                    @datastore.get(n).validate_identity_metadata
                end

                it "adds roby-events.?.log files to the identity when they are missing" do
                    o = "ad86e1e4fef4ef75cb502a3839e61c4e2284c31a9260ba4e7bf0beb4467be419"
                    n = "2428534953b1f78249e136164c54f76298e45b03a96350851b0830359f84efb4"
                    run_repair o
                    @datastore.get(n).validate_identity_metadata
                end

                it "creates a timestamp metadata entry based on the roby time" do
                    d = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    ds = @datastore.get(d)
                    ds.metadata_set "roby:time", "20201103-2343"
                    ds.metadata_delete "timestamp"
                    ds.metadata_write_to_file

                    run_repair d
                    ds = @datastore.get(d)
                    assert_equal Time.utc(2020, 11, 3, 23, 43).tv_sec,
                                 ds.metadata_fetch("timestamp")
                end

                it "creates a timestamp metadata entry based on the pocolog time "\
                   "if there is no roby time" do
                    d = "8bed98f3ce3ff08487675280844b51d9e3c564313bb8ded1854c7a70255da5a8"
                    ds = @datastore.get(d)
                    ds.metadata_delete "roby:time"
                    ds.metadata_delete "timestamp"
                    ds.metadata_write_to_file

                    run_repair d
                    ds = @datastore.get(d)
                    assert_equal 1621889928,
                                 ds.metadata_fetch("timestamp")
                end

                def run_repair(digest)
                    call_cli("repair", "--store", @store_path, digest)
                end
            end
        end
    end
end
