# frozen_string_literal: true

# A simple task model that goes through standard lifecycle events
class SimpleEmissions < Roby::Task
    terminates

    poll do
        plan.make_useless(self) if lifetime > 2
    end
end

# A task model that always fails to start
class FailToStart < Roby::Task
    terminates

    event :start do |_|
        raise "fail to start"
    end
end

Robot.controller do
    Roby.plan.add_permanent_task(task = SimpleEmissions.new)
    task.start!
    task.stop_event.on do |_|
        Roby.app.quit
    end

    Roby.plan.add_permanent_task(task = FailToStart.new)
    task.start!

    Roby.plan.add_permanent_task(task = SimpleEmissions.new)
    task.start!
    task.start_event.on do |_|
        task.stop_event.emit 42
    end
end
