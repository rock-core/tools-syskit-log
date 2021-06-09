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
                @import = Import.new(datastore)
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
                    assert_equal [[path], [], [], []],
                                 import.prepare_import(logfile_pathname)
                end
                it "ignores Roby index files" do
                    path = create_roby_logfile("test2-events.log")
                    FileUtils.touch path.sub_ext(".idx")
                    assert_equal [[], [], [path], []],
                                 import.prepare_import(logfile_pathname)
                end
                it "lists unrecognized files" do
                    FileUtils.touch(path = logfile_pathname("not_matching"))
                    assert_equal [[], [], [], [path]], import.prepare_import(logfile_pathname)
                end
                it "lists unrecognized directories" do
                    (path = logfile_pathname("not_matching")).mkpath
                    assert_equal [[], [], [], [path]], import.prepare_import(logfile_pathname)
                end
            end

            describe "#import" do
                before do
                    create_logfile "test.0.log" do
                        create_logfile_stream "test",
                                              metadata: Hash["rock_task_name" => "task0", "rock_task_object_name" => "port"]
                    end
                    create_roby_logfile("test-events.log")
                    FileUtils.touch logfile_pathname("test.txt")
                    FileUtils.touch logfile_pathname("not_recognized_file")
                    logfile_pathname("not_recognized_dir").mkpath
                    FileUtils.touch logfile_pathname("not_recognized_dir", "test")
                end

                def tty_reporter
                    Pocolog::CLI::TTYReporter.new("", color: false, progress: false)
                end

                it "can import an empty folder" do
                    Dir.mktmpdir do |dir|
                        import.import([Pathname.new(dir)])
                    end
                end

                it "moves the results under the dataset's ID" do
                    flexmock(Dataset).new_instances.should_receive(:compute_dataset_digest)
                                     .and_return("ABCDEF")
                    import_dir = import.import([logfile_pathname]).dataset_path
                    assert_equal(datastore_path + "core" + "ABCDEF", import_dir)
                end
                it "raises if the target dataset ID already exists" do
                    flexmock(Dataset).new_instances.should_receive(:compute_dataset_digest)
                                     .and_return("ABCDEF")
                    (datastore_path + "core" + "ABCDEF").mkpath
                    assert_raises(Import::DatasetAlreadyExists) do
                        import.import([logfile_pathname])
                    end
                end
                it "replaces the current dataset by the new one if the ID already exists but 'force' is true" do
                    digest = "ABCDEF"
                    flexmock(Dataset)
                        .new_instances.should_receive(:compute_dataset_digest)
                        .and_return(digest)
                    (datastore_path + "core" + digest).mkpath
                    FileUtils.touch(datastore_path + "core" + digest + "file")
                    out, = capture_io do
                        import.import(
                            [logfile_pathname], reporter: tty_reporter, force: true
                        )
                    end
                    assert_match(/Replacing existing dataset #{digest} with new one/, out)
                    assert !(datastore_path + digest + "file").exist?
                end
                it "reports its progress" do
                    # This is not really a unit test. It just exercises the code
                    # path that reports progress, but checks nothing except the lack
                    # of exceptions
                    capture_io do
                        import.import([logfile_pathname])
                    end
                end
                it "normalizes the pocolog logfiles" do
                    expected_normalize_args = hsh(
                        output_path: datastore_path + "incoming" + "0" + "core" + "pocolog",
                        index_dir: datastore_path + "incoming" + "0" + "cache" + "pocolog"
                    )

                    flexmock(Syskit::Log::Datastore).should_receive(:normalize)
                                                    .with([logfile_pathname("test.0.log")], expected_normalize_args).once
                                                    .pass_thru
                    dataset = import.import([logfile_pathname])
                    assert (dataset.dataset_path + "pocolog" + "task0::port.0.log").exist?
                end
                it "copies the text files" do
                    import_dir = import.import([logfile_pathname]).dataset_path
                    assert logfile_pathname("test.txt").exist?
                    assert (import_dir + "text" + "test.txt").exist?
                end
                it "copies the roby log files into roby-events.N.log" do
                    import_dir = import.import([logfile_pathname]).dataset_path
                    assert logfile_pathname("test-events.log").exist?
                    assert (import_dir + "roby-events.0.log").exist?
                end
                it "copies the unrecognized files" do
                    import_dir = import.import([logfile_pathname]).dataset_path

                    assert logfile_pathname("not_recognized_file").exist?
                    assert logfile_pathname("not_recognized_dir").exist?
                    assert logfile_pathname("not_recognized_dir", "test").exist?

                    assert (import_dir + "ignored" + "not_recognized_file").exist?
                    assert (import_dir + "ignored" + "not_recognized_dir").exist?
                    assert (import_dir + "ignored" + "not_recognized_dir" + "test").exist?
                end
                it "imports the Roby metadata" do
                    roby_metadata = Array[Hash["app_name" => "test"]]
                    logfile_pathname("info.yml").open("w") do |io|
                        YAML.dump(roby_metadata, io)
                    end
                    dataset = import.import([logfile_pathname])
                    assert_equal({ "roby:app_name" => Set["test"],
                                   "timestamp" => Set[0] }, dataset.metadata)
                    assert_equal({ "roby:app_name" => Set["test"],
                                   "timestamp" => Set[0] },
                                 Dataset.new(dataset.dataset_path).metadata)
                end
                it "ignores the Roby metadata if it cannot be loaded" do
                    logfile_pathname("info.yml").open("w") do |io|
                        io.write "%invalid_yaml"
                    end

                    imported = nil
                    _out, err = capture_io do
                        imported = import.import([logfile_pathname])
                    end
                    assert_match(/failed to load Roby metadata/, err)
                    assert_equal({ "timestamp" => Set[0] }, imported.metadata)
                    assert_equal({ "timestamp" => Set[0] }, Dataset.new(imported.dataset_path).metadata)
                end
                it "rebuilds a valid Roby index" do
                    FileUtils.cp roby_log_path("model_registration"),
                                 logfile_pathname + "test-events.log"

                    mtime = Time.now - 10
                    FileUtils.touch logfile_pathname + "test-events.log",
                                    mtime: mtime
                    imported = nil
                    capture_io do
                        imported = import.import([logfile_pathname])
                    end

                    log_path = imported.dataset_path + "roby-events.0.log"
                    assert_equal mtime, log_path.stat.mtime
                    index_path = imported.cache_path + "roby-events.0.idx"

                    assert (imported.cache_path + "roby.sql").exist?
                    assert (imported.cache_path + "roby-events.0.idx").exist?

                    index = Roby::DRoby::Logfile::Index.read(index_path)
                    assert index.valid_for?(log_path)
                end
                it "handles truncated Roby logs" do
                    in_log_path = logfile_pathname + "test-events.log"
                    FileUtils.cp roby_log_path("model_registration"), in_log_path
                    File.truncate(in_log_path, in_log_path.stat.size - 1)

                    mtime = Time.now - 10
                    FileUtils.touch logfile_pathname + "test-events.log",
                                    mtime: mtime
                    imported = nil
                    capture_io do
                        imported = import.import([logfile_pathname])
                    end

                    log_path = imported.dataset_path + "roby-events.0.log"
                    assert_equal mtime, log_path.stat.mtime
                    index_path = imported.cache_path + "roby-events.0.idx"

                    assert (imported.cache_path + "roby.sql").exist?
                    assert (imported.cache_path + "roby-events.0.idx").exist?

                    index = Roby::DRoby::Logfile::Index.read(index_path)
                    assert index.valid_for?(log_path)
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
