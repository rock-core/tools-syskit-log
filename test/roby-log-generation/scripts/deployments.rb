# frozen_string_literal: true

class T < Syskit::RubyTaskContext
end

require "roby/schedulers/temporal"
Roby.plan.execution_engine.scheduler = Roby::Schedulers::Temporal.new

Syskit.conf.use_ruby_tasks T => "something"

Robot.controller do
    task = Roby.plan.add_permanent_task(T)
    task.start_event.on do |_|
        Roby.app.quit
    end
end
