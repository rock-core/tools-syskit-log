# frozen_string_literal: true

require "test_helper"
require "syskit/log/datastore/import"
require "tmpdir"
require "timecop"

module Syskit::Log
    class Datastore
        describe Import do
            attr_reader :root_path, :datastore_path, :import, :datastore

            before do
                @root_path = Pathname.new(Dir.mktmpdir)
                @datastore_path = root_path + "datastore"
                datastore_path.mkpath
                @datastore = Datastore.create(datastore_path)

                @output_path = @root_path + "normalize-out"
                @cache_path = @root_path + "cache-out"
                @output_path.mkpath
                @cache_path.mkpath
                @import = Import.new(
                    @output_path, compress: compress?, cache_path: @cache_path
                )
            end

            def compress?
                ENV["SYSKIT_LOG_TEST_COMPRESS"] == "1"
            end

            def file_ext
                ".zst" if compress?
            end

            after do
                root_path.rmtree
            end

            describe "#prepare_import" do
                it "lists the pocolog files that should be copied, in normalized order" do
                    FileUtils.touch(file0_1 = logfile_pathname("file0.1.log"))
                    FileUtils.touch(file0_0 = logfile_pathname("file0.0.log"))
                    FileUtils.touch(file1_0 = logfile_pathname("file1.0.log"))
                    assert_equal [[file0_0, file0_1, file1_0], [], [], []],
                                 import.prepare_import(logfile_pathname)
                end
                it "lists the test files that should be copied" do
                    FileUtils.touch(path = logfile_pathname("file0.txt"))
                    assert_equal [[], [path], [], []],
                                 import.prepare_import(logfile_pathname)
                end
                it "lists the Roby log files that should be copied" do
                    path = create_roby_logfile("test-events.log")
                    assert_equal [[], [], [path], []],
                                 import.prepare_import(logfile_pathname)
                end
                it "raises if more than one file looks like a roby log file" do
                    create_roby_logfile("test-events.log")
                    create_roby_logfile("test2-events.log")
                    e = assert_raises(ArgumentError) do
                        import.prepare_import(logfile_pathname)
                    end
                    assert_match "more than one Roby event log found", e.message
                end
                it "ignores pocolog's index files" do
                    FileUtils.touch(path = logfile_pathname("file0.1.log"))
                    FileUtils.touch(logfile_pathname("file0.1.idx"))
                    FileUtils.touch(logfile_pathname("file0.1.idx"))
                    assert_equal [[path], [], [], []],
                                 import.prepare_import(logfile_pathname)
                end
                it "ignores Roby index files" do
                    path = create_roby_logfile("test2-events.log")
                    FileUtils.touch logfile_pathname("test2-events.idx")
                    assert_equal [[], [], [path], []],
                                 import.prepare_import(logfile_pathname)
                end
                it "lists unrecognized files" do
                    FileUtils.touch(path = logfile_pathname("not_matching"))
                    assert_equal [[], [], [], [path]],
                                 import.prepare_import(logfile_pathname)
                end
                it "lists unrecognized directories" do
                    (path = logfile_pathname("not_matching")).mkpath
                    assert_equal [[], [], [], [path]], import.prepare_import(logfile_pathname)
                end
            end

            describe "#normalize_dataset" do
                before do
                    create_logfile "test.0.log" do
                        create_logfile_stream(
                            "test",
                            metadata: {
                                "rock_task_name" => "task0",
                                "rock_task_object_name" => "port"
                            }
                        )
                    end
                    create_roby_logfile("test-events.log")
                    logfile_pathname("test.txt").write("")
                    logfile_pathname("not_recognized_file").write("")
                    logdir_pathname("not_recognized_dir").mkpath
                    logfile_pathname("not_recognized_dir", "test").write("")
                end

                def tty_reporter
                    Pocolog::CLI::TTYReporter.new("", color: false, progress: false)
                end

                it "normalizes an empty folder" do
                    Dir.mktmpdir do |dir|
                        import.normalize_dataset([Pathname.new(dir)])
                    end
                end

                it "normalizes the pocolog logfiles" do
                    expected_normalize_args = hsh(
                        output_path: @output_path + "pocolog",
                        index_dir: @cache_path + "pocolog"
                    )

                    flexmock(Syskit::Log::Datastore)
                        .should_receive(:normalize)
                        .with([logfile_pathname("test.0.log")], expected_normalize_args)
                        .once.pass_thru
                    dataset = import.normalize_dataset([logfile_pathname])
                    expected_file =
                        dataset.dataset_path + "pocolog" + "task0::port.0.log#{file_ext}"
                    assert expected_file.exist?
                end
                it "calculates the dataset digest" do
                    dataset = import.normalize_dataset([logfile_pathname])
                    identity = dataset.compute_dataset_identity_from_files
                    assert_equal dataset.digest, dataset.compute_dataset_digest(identity)
                end
                it "copies the text files" do
                    import_dir = import.normalize_dataset([logfile_pathname]).dataset_path
                    assert logfile_pathname("test.txt").exist?
                    assert (import_dir + "text" + "test.txt#{file_ext}").exist?
                end
                it "copies the roby log files into roby-events.N.log" do
                    import_dir = import.normalize_dataset([logfile_pathname]).dataset_path
                    assert logfile_pathname("test-events.log").exist?
                    assert (import_dir + "roby-events.0.log#{file_ext}").exist?
                end
                it "copies the unrecognized files" do
                    import_dir = import.normalize_dataset([logfile_pathname]).dataset_path

                    assert(
                        (import_dir + "ignored" + "not_recognized_file#{file_ext}")
                        .exist?
                    )
                    assert (import_dir + "ignored" + "not_recognized_dir").exist?
                    assert(
                        import_dir
                            .join("ignored", "not_recognized_dir", "test#{file_ext}")
                            .exist?
                    )
                end
                it "imports the Roby metadata" do
                    roby_metadata = Array[Hash["app_name" => "test"]]
                    write_logfile("info.yml", YAML.dump(roby_metadata))
                    dataset = import.normalize_dataset([logfile_pathname])
                    assert_equal({ "roby:app_name" => Set["test"],
                                   "timestamp" => Set[0],
                                   "digest" => Set[dataset.digest] }, dataset.metadata)
                    assert_equal({ "roby:app_name" => Set["test"],
                                   "timestamp" => Set[0],
                                   "digest" => Set[dataset.digest] },
                                 Dataset.new(dataset.dataset_path).metadata)
                end
                it "ignores the Roby metadata if it cannot be loaded" do
                    logfile_pathname("info.yml").open("w") do |io|
                        io.write "%invalid_yaml"
                    end

                    imported = nil
                    _out, err = capture_io do
                        imported = import.normalize_dataset([logfile_pathname])
                    end
                    assert_match(/failed to load Roby metadata/, err)
                    assert_equal({ "timestamp" => Set[0], "digest" => Set[imported.digest] },
                                 imported.metadata)
                    assert_equal({ "timestamp" => Set[0], "digest" => Set[imported.digest] },
                                 Dataset.new(imported.dataset_path).metadata)
                end
                it "rebuilds a valid Roby index" do
                    skip if compress?

                    copy_in_file(roby_log_path("model_registration"), "test-events.log")

                    mtime = Time.now - 10
                    FileUtils.touch logfile_pathname("test-events.log"), mtime: mtime
                    imported = import.normalize_dataset([logfile_pathname])

                    log_path = imported.dataset_path + "roby-events.0.log#{file_ext}"
                    assert_equal mtime, log_path.stat.mtime
                    index_path = imported.cache_path + "roby-events.0.idx"

                    assert (imported.dataset_path + "roby.sql").exist?
                    assert index_path.exist?

                    index = Roby::DRoby::Logfile::Index.read(index_path)
                    assert index.valid_for?(log_path)
                end
                it "skips the roby index if it fails to load" do
                    copy_in_file(roby_log_path("model_registration"), "test-events.log")

                    mtime = Time.now - 10
                    FileUtils.touch logfile_pathname("test-events.log"),
                                    mtime: mtime
                    flexmock(RobySQLIndex::Index)
                        .new_instances
                        .should_receive(:add_one_cycle).and_raise(RuntimeError)
                    imported = import.normalize_dataset([logfile_pathname])

                    log_path = imported.dataset_path + "roby-events.0.log#{file_ext}"
                    assert_equal mtime, log_path.stat.mtime

                    refute (imported.dataset_path + "roby.sql").exist?
                    refute (imported.cache_path + "roby-events.0.idx").exist?
                end
                it "handles truncated Roby logs" do
                    Tempfile.open do |io|
                        data = roby_log_path("model_registration").read
                        io.write data[0..-2]
                        io.flush
                        copy_in_file(Pathname.new(io.path), "test-events.log")
                    end

                    mtime = Time.now - 10
                    FileUtils.touch logfile_pathname("test-events.log"),
                                    mtime: mtime
                    imported = import.normalize_dataset([logfile_pathname])

                    log_path = imported.dataset_path + "roby-events.0.log#{file_ext}"
                    assert_equal mtime, log_path.stat.mtime
                    index_path = imported.cache_path + "roby-events.0.idx"

                    assert (imported.dataset_path + "roby.sql").exist?

                    unless compress?
                        assert (imported.cache_path + "roby-events.0.idx").exist?
                        index = Roby::DRoby::Logfile::Index.read(index_path)
                        assert index.valid_for?(log_path)
                    end
                end
            end

            describe ".move_dataset_to_store" do
                it "moves the results under the dataset's ID" do
                    dataset = Dataset.new(
                        @output_path,
                        cache: @cache_path, digest: "ABCDEF"
                    )
                    dataset = Import.move_dataset_to_store(dataset, @datastore)
                    assert_equal(
                        datastore_path + "core" + "ABCDEF",
                        dataset.dataset_path
                    )
                end

                it "raises if the dataset's core and cache paths are the same" do
                    dataset = Dataset.new(@output_path, digest: "ABCDEF")
                    e = assert_raises(ArgumentError) do
                        Import.move_dataset_to_store(dataset, @datastore)
                    end
                    assert_match(
                        /cannot move a dataset that has identical cache and data paths/,
                        e.message
                    )
                end

                it "ignores if the cache path does not exist" do
                    dataset = Dataset.new(
                        @output_path,
                        cache: Pathname("/does/not/exist"), digest: "ABCDEF"
                    )
                    Import.move_dataset_to_store(dataset, @datastore)
                end
            end

            describe "#find_import_info" do
                it "returns nil for a directory that has not been imported" do
                    assert_nil Import.find_import_info(logfile_pathname)
                end

                it "returns the import information of an imported directory" do
                    dataset = flexmock(dataset_path: logfile_pathname,
                                       digest: "something")

                    dir = Pathname(make_tmpdir)
                    Import.save_import_info(dir, dataset, time: (t = Time.now))
                    digest, time = Import.find_import_info(dir)
                    assert_equal digest, dataset.digest
                    assert_equal t, time
                end
            end
        end
    end
end
