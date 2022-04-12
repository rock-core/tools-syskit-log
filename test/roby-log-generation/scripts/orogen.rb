# frozen_string_literal: true

using_task_library "orogen_syskit_tests"

class C < Syskit::Composition
    argument :base, default: 0
    argument :limit, default: 10

    add(OroGen.orogen_syskit_tests.Echo.deployed_as("echo"), as: "echo")

    data_writer echo_child.in_port, as: "echo"
    poll do
        super()

        echo_writer.write(base + lifetime * 10)
        stop_event.emit if lifetime > limit
    end
end

def spawn(**arguments, &block)
    c = C.with_arguments(**arguments).as_plan
    c.stop_event.on(&block)
    Roby.plan.add_permanent_task(c)
    c.planning_task.start!
end

Robot.controller do
    spawn do
        Roby.execution_engine.delayed(1) do
            spawn(base: 1000) do
                Roby.app.quit
            end
        end
    end
end
