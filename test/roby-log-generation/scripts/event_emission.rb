# frozen_string_literal: true

# A single task model
class M < Roby::Task
    terminates

    poll do
        plan.make_useless(self) if lifetime > 2
    end
end

Robot.controller do
    Roby.plan.add_permanent_task(task = M.new)
    task.start!
    task.stop_event.on do |_|
        Roby.app.quit
    end
end
