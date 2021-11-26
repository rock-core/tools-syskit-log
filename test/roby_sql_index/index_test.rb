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
                        event = @index.event_propagations.emissions.by_name(:start).one!
                        assert_equal "start", event.name
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
