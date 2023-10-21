# frozen_string_literal: true

require "minitest/spec"
require "minitest/autorun"
require "tmpdir"

require "syskit/log/reports"

# Helper functions for tests in tw/logs/reporting
module TestHelpers
    def setup
        super

        @tmpdirs = []
    end

    def teardown
        super

        @tmpdirs.each(&:rmtree)
    end

    def make_tmppath
        dir = Pathname.new(Dir.mktmpdir)
        @tmpdirs << dir
        dir
    end

    def create_notebook(dir, name, cells: [], **metadata)
        (dir / name).open("w") do |io|
            io << JSON.generate(
                {
                    "cells" => cells,
                    "metadata" => metadata,
                    "nbformat" => 4,
                    "nbformat_minor" => 4
                }
            )
        end
    end
end
