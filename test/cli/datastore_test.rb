require 'test_helper'
require 'syskit/pocolog/cli/datastore'

module Syskit::Pocolog
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
                Syskit::Pocolog::Datastore
            end

            after do
                root_path.rmtree
            end

            # Helper method to call a CLI subcommand
            def call_cli(*args, silent: true)
                extra_args = Array.new
                if silent
                    extra_args << '--silent'
                end
                Datastore.start([*args, *extra_args], debug: true)
            end

            describe "#import" do
                it "imports a single dataset into the store" do
                    incoming_path = datastore_path + 'incoming' + '0'
                    flexmock(datastore_m::Import).new_instances.should_receive(:normalize_dataset).
                        with(logfile_pathname, incoming_path + "core", cache_path: incoming_path + "cache", silent: true).
                        once.pass_thru
                    expected_dataset = lambda do |s|
                        assert_equal incoming_path + "core", s.dataset_path
                        assert_equal incoming_path + "cache", s.cache_path
                        true
                    end
                    flexmock(datastore_m::Import).new_instances.should_receive(:move_dataset_to_store).
                        with(logfile_pathname, expected_dataset, force: false, silent: true).
                        once.pass_thru

                    call_cli('import', '--min-duration=0', datastore_path.to_s, logfile_pathname.to_s, silent: true)
                end

                describe '--auto' do
                    it "creates the datastore path" do
                        datastore_path.rmtree
                        call_cli('import', '--auto', datastore_path.to_s, root_path.to_s)
                        assert datastore_path.exist?
                    end
                    it "auto-imports any directory that looks like a raw dataset" do
                        create_logfile('test.0.log') {}
                        FileUtils.touch logfile_path('test-events.log')
                        incoming_path = datastore_path + 'incoming' + '0'
                        flexmock(datastore_m::Import).new_instances.should_receive(:normalize_dataset).
                            with(logfile_pathname, incoming_path + "core", cache_path: incoming_path + "cache", silent: true).
                            once.pass_thru
                        expected_dataset = lambda do |s|
                            assert_equal incoming_path + "core", s.dataset_path
                            assert_equal incoming_path + "cache", s.cache_path
                            true
                        end

                        flexmock(datastore_m::Import).new_instances.should_receive(:move_dataset_to_store).
                            with(logfile_pathname, expected_dataset, force: false, silent: true).
                            once.pass_thru

                        call_cli('import', '--auto', '--min-duration=0', datastore_path.to_s, logfile_pathname.dirname.to_s, silent: true)
                        digest, time = datastore_m::Import.find_import_info(logfile_pathname)
                        assert datastore.has?(digest)
                    end
                    it "ignores datasets that have already been imported" do
                        create_logfile('test.0.log') do
                            create_logfile_stream 'test', metadata: Hash['rock_task_name' => 'task', 'rock_task_object_name' => 'port']
                            write_logfile_sample Time.now, Time.now, 10
                            write_logfile_sample Time.now + 10, Time.now + 1, 20
                        end
                        FileUtils.touch logfile_path('test-events.log')
                        call_cli('import', '--auto', '--min-duration=0', datastore_path.to_s, logfile_pathname.dirname.to_s, silent: true)
                        flexmock(datastore_m::Import).new_instances.should_receive(:normalize_dataset).
                            never
                        flexmock(datastore_m::Import).new_instances.should_receive(:move_dataset_to_store).
                            never
                        _out, err = capture_io do
                            call_cli('import', '--auto', '--min-duration=0', datastore_path.to_s, logfile_pathname.dirname.to_s, silent: false)
                        end
                        assert_match /#{logfile_pathname} already seem to have been imported as .*Give --force/,
                            err
                    end
                    it "processes datasets that have already been imported if --force is given" do
                        create_logfile('test.0.log') do
                            create_logfile_stream 'test', metadata: Hash['rock_task_name' => 'task', 'rock_task_object_name' => 'port']
                            write_logfile_sample Time.now, Time.now, 10
                            write_logfile_sample Time.now + 10, Time.now + 1, 20
                        end
                        FileUtils.touch logfile_path('test-events.log')
                        call_cli('import', '--auto', '--min-duration=0', datastore_path.to_s, logfile_pathname.dirname.to_s, silent: true)
                        flexmock(datastore_m::Import).new_instances.should_receive(:normalize_dataset).
                            once.pass_thru
                        flexmock(datastore_m::Import).new_instances.should_receive(:move_dataset_to_store).
                            once.pass_thru
                        _out, err = capture_io do
                            call_cli('import', '--auto', '--min-duration=0', '--force', datastore_path.to_s, logfile_pathname.dirname.to_s, silent: false)
                        end
                        assert_match /#{logfile_pathname} seem to have already been imported but --force is given, overwriting/,
                            err
                    end
                    it "ignores datasets that do not seem to be already imported, but are" do
                        create_logfile('test.0.log') do
                            create_logfile_stream 'test', metadata: Hash['rock_task_name' => 'task', 'rock_task_object_name' => 'port']
                            write_logfile_sample Time.now, Time.now, 10
                            write_logfile_sample Time.now + 10, Time.now + 1, 20
                        end
                        FileUtils.touch logfile_path('test-events.log')
                        call_cli('import', '--auto', '--min-duration=0', datastore_path.to_s, logfile_pathname.dirname.to_s, silent: true)
                        (logfile_pathname + datastore_m::Import::BASENAME_IMPORT_TAG).unlink
                        flexmock(datastore_m::Import).new_instances.should_receive(:normalize_dataset).
                            once.pass_thru
                        flexmock(datastore_m::Import).new_instances.should_receive(:move_dataset_to_store).
                            once.pass_thru
                        _out, err = capture_io do
                            call_cli('import', '--auto', '--min-duration=0', datastore_path.to_s, logfile_pathname.dirname.to_s, silent: false)
                        end
                        assert_match /#{logfile_pathname} already seem to have been imported as .*Give --force/,
                            err
                    end
                    it "imports datasets that do not seem to be already imported, but are if --force is given" do
                        create_logfile('test.0.log') do
                            create_logfile_stream 'test', metadata: Hash['rock_task_name' => 'task', 'rock_task_object_name' => 'port']
                            write_logfile_sample Time.now, Time.now, 10
                            write_logfile_sample Time.now + 10, Time.now + 1, 20
                        end
                        FileUtils.touch logfile_path('test-events.log')
                        call_cli('import', '--auto', '--min-duration=0', datastore_path.to_s, logfile_pathname.dirname.to_s, silent: true)
                        digest, _ = datastore_m::Import.find_import_info(logfile_pathname)
                        marker_path = datastore.core_path_of(digest) + "marker"
                        FileUtils.touch(marker_path)
                        (logfile_pathname + datastore_m::Import::BASENAME_IMPORT_TAG).unlink
                        flexmock(datastore_m::Import).new_instances.should_receive(:normalize_dataset).
                            once.pass_thru
                        flexmock(datastore_m::Import).new_instances.should_receive(:move_dataset_to_store).
                            once.pass_thru
                        _out, err = capture_io do
                            call_cli('import', '--auto', '--force', '--min-duration=0', datastore_path.to_s, logfile_pathname.dirname.to_s, silent: false)
                        end
                        assert_match /Replacing existing dataset #{digest} with new one/, err
                        refute marker_path.exist?
                    end
                    it "ignores an empty dataset if --min-duration is non-zero" do
                        create_logfile('test.0.log') {}
                        FileUtils.touch logfile_path('test-events.log')
                        incoming_path = datastore_path + 'incoming' + '0'
                        flexmock(datastore_m::Import).new_instances.should_receive(:normalize_dataset).
                            once.pass_thru
                        flexmock(datastore_m::Import).new_instances.should_receive(:move_dataset_to_store).
                            never

                        call_cli('import', '--auto', '--min-duration=1', datastore_path.to_s, logfile_pathname.dirname.to_s, silent: true)
                    end
                    it "ignores datasets whose logical duration is lower than --min-duration" do
                        create_logfile('test.0.log') do
                            create_logfile_stream 'test', metadata: Hash['rock_task_name' => 'task', 'rock_task_object_name' => 'port']
                            write_logfile_sample Time.now, Time.now, 10
                            write_logfile_sample Time.now + 10, Time.now + 1, 20
                        end
                        FileUtils.touch logfile_path('test-events.log')
                        incoming_path = datastore_path + 'incoming' + '0'
                        flexmock(datastore_m::Import).new_instances.should_receive(:normalize_dataset).
                            once.pass_thru
                        flexmock(datastore_m::Import).new_instances.should_receive(:move_dataset_to_store).
                            never

                        _out, err = capture_io do
                            call_cli('import', '--auto', '--min-duration=5', datastore_path.to_s, logfile_pathname.dirname.to_s, silent: false)
                        end
                        assert_match /#{logfile_pathname} lasts only 1.0s, ignored/, err
                    end
                end
            end

            describe "#normalize" do
                it "normalizes the logfiles in the input directory into the directory provided as 'out'" do
                    create_logfile('test.0.log') {}
                    out_path = root_path + "normalized"
                    flexmock(Syskit::Pocolog::Datastore).should_receive(:normalize).
                        with([logfile_pathname('test.0.log')], hsh(output_path: out_path)).
                        once.pass_thru
                    call_cli('normalize', logfile_pathname.to_s, "--out=#{out_path}", silent: true)
                end
                it "reports progress without --silent" do
                    create_logfile('test.0.log') {}
                    out_path = root_path + "normalized"
                    flexmock(Syskit::Pocolog::Datastore).should_receive(:normalize).
                        with([logfile_pathname('test.0.log')], hsh(output_path: out_path)).
                        once.pass_thru
                    capture_io do
                        call_cli('normalize', logfile_pathname.to_s, "--out=#{out_path}", silent: false)
                    end
                end
            end

            describe "#index" do
                before do
                    create_dataset "A" do
                        create_logfile('test.0.log') {}
                    end
                    create_dataset "B" do
                        create_logfile('test.0.log') {}
                    end
                end

                def expected_store
                    ->(store) { store.datastore_path == datastore_path }
                end

                def expected_dataset(digest)
                    ->(dataset) { dataset.dataset_path == datastore.get(digest).dataset_path }
                end

                it "runs the indexer on all datasets of the store if none are provided on the command line" do
                    flexmock(Syskit::Pocolog::Datastore).
                        should_receive(:index_build).
                        with(expected_store, expected_dataset('A'), force: false).once.
                        pass_thru
                    flexmock(Syskit::Pocolog::Datastore).
                        should_receive(:index_build).
                        with(expected_store, expected_dataset('B'), force: false).once.
                        pass_thru
                    call_cli('index', datastore_path.to_s)
                end
                it "runs the indexer on the datasets of the store specified on the command line" do
                    flexmock(Syskit::Pocolog::Datastore).
                        should_receive(:index_build).
                        with(expected_store, expected_dataset('A'), force: false).once.
                        pass_thru
                    call_cli('index', datastore_path.to_s, 'A')
                end
            end

            describe "#list" do
                attr_reader :show_a0fa, :show_a0ga
                before do
                    create_dataset "a0fa", metadata: Hash['description' => 'first', 'test' => ['2'], 'array_test' => ['a', 'b']] do
                        create_logfile('test.0.log') {}
                    end
                    create_dataset "a0ga", metadata: Hash['test' => ['1'], 'array_test' => ['c', 'd']] do
                        create_logfile('test.0.log') {}
                    end
                    @show_a0fa = <<-EOF
a0fa first
  test: 2
  array_test:
  - a
  - b
                    EOF
                    @show_a0ga = <<-EOF
a0ga <no description>
  test: 1
  array_test:
  - c
  - d
                    EOF


                end

                it "raises if the query is invalid" do
                    assert_raises(Syskit::Pocolog::Datastore::Dataset::InvalidDigest) do
                        call_cli('list', datastore_path.to_s, 'not_a_sha', silent: false)
                    end
                end

                it "lists all datasets if given only the datastore path" do
                    out, _err = capture_io do
                        call_cli('list', datastore_path.to_s, silent: false)
                    end
                    assert_equal [show_a0fa, show_a0ga].join, out
                end
                it "lists only the short digests if --digest is given" do
                    out, _err = capture_io do
                        call_cli('list', datastore_path.to_s, '--digest', silent: false)
                    end
                    assert_equal "a0ea\na0fa\n", out
                end
                it "lists only the short digests if --digest --long-digests are given" do
                    out, _err = capture_io do
                        call_cli('list', datastore_path.to_s, '--digest', '--long-digests', silent: false)
                    end
                    assert_equal "a0ea\na0fa\n", out
                end
                it "accepts a digest prefix as argument" do
                    out, _err = capture_io do
                        call_cli('list', datastore_path.to_s, 'a0f', silent: false)
                    end
                    assert_equal show_a0fa, out
                end
                it "can match metadata exactly" do
                    out, _err = capture_io do
                        call_cli('list', datastore_path.to_s, 'test=1', silent: false)
                    end
                    assert_equal show_a0ga, out
                end
                it "can match metadata with a regexp" do
                    out, _err = capture_io do
                        call_cli('list', datastore_path.to_s, 'array_test~[ac]', silent: false)
                    end
                    assert_equal [show_a0fa, show_a0ga].join, out
                end
            end

            describe "#metadata" do
                before do
                    create_dataset "a0fa", metadata: Hash['test' => ['a']] do
                        create_logfile('test.0.log') {}
                    end
                    create_dataset "a0ga", metadata: Hash['test' => ['b']] do
                        create_logfile('test.0.log') {}
                    end
                end

                it "raises if the query is invalid" do
                    assert_raises(Syskit::Pocolog::Datastore::Dataset::InvalidDigest) do
                        call_cli('metadata', datastore_path.to_s, 'not_a_sha', '--get', silent: false)
                    end
                end

                describe '--set' do
                    it "sets metadata on the given dataset" do
                        call_cli('metadata', datastore_path.to_s, 'a0f', '--set', 'debug=true', silent: false)
                        assert_equal Set['true'], datastore.get('a0fa').metadata['debug']
                        assert_nil datastore.get('a0ga').metadata['debug']
                    end
                    it "sets metadata on matching datasets" do
                        call_cli('metadata', datastore_path.to_s, 'test=b', '--set', 'debug=true', silent: false)
                        assert_nil datastore.get('a0fa').metadata['debug']
                        assert_equal Set['true'], datastore.get('a0ga').metadata['debug']
                    end
                    it "sets metadata on all datasets if no query is given" do
                        call_cli('metadata', datastore_path.to_s, '--set', 'debug=true', silent: false)
                        assert_equal Set['true'], datastore.get('a0fa').metadata['debug']
                        assert_equal Set['true'], datastore.get('a0ga').metadata['debug']
                    end
                    it "collects all set arguments with the same key" do
                        call_cli('metadata', datastore_path.to_s, '--set', 'test=a', 'test=b', 'test=c', silent: false)
                        assert_equal Set['a', 'b', 'c'], datastore.get('a0fa').metadata['test']
                    end
                    it "raises if the argument to set is not a key=value association" do
                        assert_raises(ArgumentError) do
                            call_cli('metadata', datastore_path.to_s, 'a0fa', '--set', 'debug', silent: false)
                        end
                    end
                end

                describe '--get' do
                    it "lists all metadata on all datasets if no query is given" do
                        call_cli('metadata', datastore_path.to_s, 'a0fa', '--set', 'test=a,b', silent: false)
                        out, _err = capture_io do
                            call_cli('metadata', datastore_path.to_s, '--get', silent: false)
                        end
                        assert_equal "a0fa test=a,b\na0ga test=b\n", out
                    end
                    it "displays the short digest by default" do
                        flexmock(Syskit::Pocolog::Datastore).new_instances.should_receive(:short_digest).
                            and_return { |dataset| dataset.digest[0, 3] }
                        out, _err = capture_io do
                            call_cli('metadata', datastore_path.to_s, '--get', silent: false)
                        end
                        assert_equal "a0f test=a\na0g test=b\n", out
                    end
                    it "displays the long digest if --long-digest is given" do
                        flexmock(datastore).should_receive(:short_digest).never
                        out, _err = capture_io do
                            call_cli('metadata', datastore_path.to_s, '--get', '--long-digest', silent: false)
                        end
                        assert_equal "a0fa test=a\na0ga test=b\n", out
                    end
                    it "lists the requested metadata of the matching datasets" do
                        call_cli('metadata', datastore_path.to_s, 'a0fa', '--set', 'test=a,b', 'debug=true', silent: false)
                        out, _err = capture_io do
                            call_cli('metadata', datastore_path.to_s, 'a0fa', '--get', 'test', silent: false)
                        end
                        assert_equal "a0fa test=a,b\n", out
                    end
                    it "replaces requested metadata that are unset by <unset>" do
                        out, _err = capture_io do
                            call_cli('metadata', datastore_path.to_s, 'a0fa', '--get', 'debug', silent: false)
                        end
                        assert_equal "a0fa debug=<unset>\n", out
                    end
                end

                it "raises if both --get and --set are provided" do
                    assert_raises(ArgumentError) do
                        call_cli('metadata', datastore_path.to_s, 'a0fa', '--get', 'debug', '--set', 'test=10', silent: false)
                    end
                end

                it "raises if neither --get nor --set are provided" do
                    assert_raises(ArgumentError) do
                        call_cli('metadata', datastore_path.to_s, 'a0fa', silent: false)
                    end
                end
            end
        end
    end
end

