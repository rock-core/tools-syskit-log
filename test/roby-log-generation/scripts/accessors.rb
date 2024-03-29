# frozen_string_literal: true

# A single task model
module Namespace
    class M < Roby::Task
        terminates

        argument :arg, default: nil

        poll do
            plan.make_useless(self) if lifetime > 2
        end

        class Submodel < Roby::Task
            terminates
        end
    end
end

Robot.controller do
    Roby.plan.add_permanent_task(task = Namespace::M::Submodel.new)
    task.start!
    Roby.plan.add_permanent_task(task = Namespace::M.new(arg: 10))
    task.start!

    task.stop_event.on do |_|
        Roby.app.quit
    end
end
