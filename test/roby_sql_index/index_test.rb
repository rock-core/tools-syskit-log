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

                    it "registers emitted events" do
                        emissions =
                            @index.event_propagations
                                  .by_name(:start).by_kind(EVENT_PROPAGATION_EMIT)
                                  .to_a
                        assert_equal 2, emissions.size
                        assert_equal %w[start start], emissions.map(&:name)

                        emissions.each do |e|
                            @index.event_propagations
                                  .by_name(:start).by_kind(EVENT_PROPAGATION_CALL)
                                  .by_task_id(e.task_id)
                                  .one!
                        end
                    end

                    it "registers an event that fails to start" do
                        failure =
                            @index.event_propagations
                                  .by_kind(EVENT_PROPAGATION_EMIT_FAILED).one!
                        assert_equal "start", failure.name

                        # The corresponding call should exist
                        @index.event_propagations
                              .by_name("start")
                              .by_kind(EVENT_PROPAGATION_CALL)
                              .by_task_id(failure.task_id).one!
                    end

                    it "registers event context" do
                        assert(
                            @index.event_propagations
                                  .by_name(:stop).by_kind(EVENT_PROPAGATION_EMIT)
                                  .to_a.find { |e| JSON.parse(e.context) == [42] }
                        )
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
                        assert_equal Time.parse("2022-03-16 11:06:05.828987 -0300"),
                                     metadata.time_start
                        assert_equal Time.parse("2022-03-16 11:06:08.033058 -0300"),
                                     metadata.time_end
                    end

                    it "returns global start/stop time" do
                        @index.add_roby_log(roby_log_path("model_registration"))
                        assert_equal Time.parse("2020-09-21 15:54:22.56023 -0300"),
                                     @index.time_start
                        assert_equal Time.parse("2022-03-16 11:06:08.033058 -0300"),
                                     @index.time_end
                    end
                end
            end
        end
    end
end
