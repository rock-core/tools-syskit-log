# frozen_string_literal: true

require "erb"
require "json"
require "pathname"

require "syskit/log/dsl"

require "syskit/log/reports/notebooks"
require "syskit/log/reports/report_description"

module Syskit
    module Log
        # Tooling related to generating reports from log datasets
        module Reports
            # Exception raised in {ReportDescription#to_json} if no notebooks
            # were added
            class EmptyReport < RuntimeError
            end
        end
    end
end
