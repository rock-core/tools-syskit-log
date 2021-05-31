# frozen_string_literal: true

module Syskit::Log
    module Extensions
        # Extension of the Roby::ExecutionEngine class to add the engine's
        # single replay manager
        module ExecutionEngine
            # The unique replay manager used to replay data for this engine
            def pocolog_replay_manager
                @pocolog_replay_manager ||= ReplayManager.new(self)
            end
        end
    end
end
