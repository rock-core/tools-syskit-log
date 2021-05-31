# frozen_string_literal: true

module Namespace
    task_service "ParentTaskService"
    class ParentModel < Roby::Task
        terminates

        provides ParentTaskService
    end

    task_service "ChildTaskService"
    class ChildModel < ParentModel
        terminates

        provides ChildTaskService
    end
end

Robot.controller do
    Roby.plan.add_permanent_task(task = Namespace::ChildModel.new)
    # We need one event emitted to get anything done
    task.start!
    task.poll do
        Roby.plan.make_useless(task) if task.lifetime > 2
    end
    task.stop_event.on do
        Roby.app.quit
    end
end
