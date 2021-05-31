# frozen_string_literal: true

require "test_helper"

module Syskit::Log
    describe ShellInterface do
        attr_reader :replay_manager, :subject

        before do
            @replay_manager = app.plan.execution_engine.pocolog_replay_manager
            @subject = ShellInterface.new(app)
        end

        it "gives access to the current replay" do
            flexmock(replay_manager).should_receive(:time).and_return(time = flexmock)
            assert_equal time, subject.time
        end
    end
end
