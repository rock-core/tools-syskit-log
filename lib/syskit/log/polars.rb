# frozen_string_literal: true

require "polars"
require "syskit/log/polars/frame_builder"
require "syskit/log/polars/column_builder"
require "syskit/log/polars/column_resolved_field_builder"
require "syskit/log/polars/column_logical_time_builder"
require "syskit/log/polars/path_builder"
require "syskit/log/polars/create_aligned_frame"
require "syskit/log/polars/dsl"

Syskit::Log::DSL.include(Syskit::Log::Polars::DSL)
