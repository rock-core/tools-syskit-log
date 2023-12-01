# frozen_string_literal: true

require_relative "helpers"

module Syskit
    module Log
        module Reports
            describe ReportDescription do
                include TestHelpers

                before do
                    @tmpdir = make_tmppath
                end

                it "reports an error if there are no notebooks" do
                    report = ReportDescription.new
                    assert_raises(EmptyReport) do
                        report.to_json
                    end
                end

                it "concatenates the added notebooks" do
                    report = ReportDescription.new
                    create_notebook @tmpdir, "report1", cells: ["1"]
                    create_notebook @tmpdir, "report2", cells: ["2"]

                    report.add_notebook @tmpdir / "report1"
                    report.add_notebook @tmpdir / "report2"
                    result = report.to_json

                    assert_equal %w[1 2], result["cells"]
                end

                it "evaluates the added notebooks with ERB "\
                   "using variables passed to 'set'" do
                    report = ReportDescription.new
                    report.set "some", "data"
                    create_notebook @tmpdir, "report1", cells: ["<%= some %>"]
                    report.add_notebook @tmpdir / "report1"
                    result = report.to_json

                    assert_equal %w[data], result["cells"]
                end

                it "overrides variables passed to 'set' using the variables passed "\
                   "to add_notebook" do
                    report = ReportDescription.new
                    report.set "some", "data"
                    create_notebook @tmpdir, "report1", cells: ["<%= some %>"]
                    report.add_notebook @tmpdir / "report1", some: "42"
                    result = report.to_json

                    assert_equal %w[42], result["cells"]
                end

                it "raises on evaluation if a variable does not exist" do
                    report = ReportDescription.new
                    create_notebook @tmpdir, "report1", cells: ["<%= does_not_exist %>"]
                    report.add_notebook @tmpdir / "report1"
                    assert_raises(NameError) do
                        report.to_json
                    end
                end

                it "allows to load the report description from a DSL-like file" do
                    create_notebook @tmpdir, "report1", cells: ["<%= everything %>"]
                    create_notebook @tmpdir, "report2", cells: ["<%= half_of_it %>"]
                    (@tmpdir / "report.rb").write <<~REPORT
                        set "everything", "42"
                        add_notebook "report1"
                        add_notebook "report2"
                    REPORT

                    report = ReportDescription.new
                    report.set :half_of_it, 21
                    report.load(@tmpdir / "report.rb")
                    result = report.to_json
                    assert_equal %w[42 21], result["cells"]
                end

                it "provides with a class method to create and load the report object" do
                    create_notebook @tmpdir, "report1", cells: ["<%= everything %>"]
                    create_notebook @tmpdir, "report2", cells: ["<%= half_of_it %>"]
                    (@tmpdir / "report.rb").write <<~REPORT
                        set "everything", "42"
                        add_notebook "report1"
                        add_notebook "report2"
                    REPORT

                    report = ReportDescription.load(
                        @tmpdir / "report.rb", half_of_it: "21"
                    )
                    result = report.to_json
                    assert_equal %w[42 21], result["cells"]
                end
            end
        end
    end
end
