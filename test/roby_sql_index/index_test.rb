# frozen_string_literal: true

require "test_helper"

module Syskit
    module Log
        module RobySQLIndex
            describe Index do
                before do
                    @index = Index.create(logfile_pathname("roby.sql"))
                end

                describe "model management" do
                    before do
                        @index.add_roby_log(roby_log_path("model_registration"))
                    end

                    it "registers a task instance model" do
                        assert_equal "Namespace::ChildModel",
                                     @index.models.by_name("Namespace::ChildModel")
                                           .one!.name
                    end
                end

                describe "event emission" do
                    before do
                        @index.add_roby_log(roby_log_path("event_emission"))
                    end

                    it "registers an emitted event" do
                        event = @index.emitted_events.by_name(:start).one!
                        assert_equal "start", event.name
                    end

                    it "builds its full name" do
                        event = @index.emitted_events.by_name(:start).one!
                        assert_equal "M.start_event", @index.event_full_name(event)
                    end

                    it "allows to get a task history" do
                        task = @index.tasks.one!
                        events = @index.history_of(task).to_a
                        assert(events.find { |ev| ev.name == "start" })
                        assert(events.find { |ev| ev.name == "stop" })
                    end
                end

                describe "truncated log files" do
                    describe "tasks without a stop" do
                        before do
                            @index.add_roby_log(roby_log_path("event_emission_truncated"))
                        end

                        it "does not return the stop event" do
                            task = @index.tasks.one!
                            events = @index.history_of(task).to_a
                            refute(events.find { |ev| ev.name == "stop" })
                        end

                        it "returns the end time of the log itself "\
                           "as the end time of the task" do
                            task = @index.task_by_id(@index.tasks.one!.id)
                            assert_equal @index.time_end, task.stop_time
                        end
                    end
                end

                describe "log metadata" do
                    before do
                        @index.add_roby_log(roby_log_path("event_emission"))
                    end

                    it "raises if the log metadata does not exist" do
                        assert_raises(Index::NoSuchLogfile) do
                            @index.log_metadata_for("does not exist")
                        end
                    end

                    it "registers global information about the whole log" do
                        metadata = @index.log_metadata_for("event_emission-events.log")
                        assert_equal 21, metadata.cycle_count
                        assert_equal Time.parse("2020-09-21 15:16:55.148644 -0300"),
                                     metadata.time_start
                        assert_equal Time.parse("2020-09-21 15:16:57.363323 -0300"),
                                     metadata.time_end
                    end

                    it "returns global start/stop time" do
                        @index.add_roby_log(roby_log_path("model_registration"))
                        assert_equal Time.parse("2020-09-21 15:16:55.148644 -0300"),
                                     @index.time_start
                        assert_equal Time.parse("2020-09-21 15:54:24.774132 -0300"),
                                     @index.time_end
                    end
                end
            end
        end
    end
end
