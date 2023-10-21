# frozen_string_literal: true

require_relative "helpers"

module Syskit
    module Log
        module Reports
            describe "notebooks" do
                describe ".notebooks_load_and_concatenate" do
                    include TestHelpers

                    before do
                        @tmp = make_tmppath
                    end

                    it "interprets the givens paths as ERB and concatenates the cells" do
                        one = make_template("1.txt", { "cells" => ["<%= 1 %>"] })
                        two = make_template("2.txt", { "cells" => ["<%= 2 %>"] })

                        result = Reports.notebooks_load_and_concatenate(one, two)
                        assert_equal %w[1 2], result["cells"]
                    end

                    it "uses the metadata from the first" do
                        one = make_template(
                            "1.txt",
                            { "cells" => ["<%= 1 %>"],
                              "metadata" => { "some" => "thing" } }
                        )
                        two = make_template(
                            "2.txt",
                            { "cells" => ["<%= 2 %>"],
                              "metadata" => { "some" => "thingelse" } }
                        )

                        result = Reports.notebooks_load_and_concatenate(one, two)
                        assert_equal({ "some" => "thing" }, result["metadata"])
                    end

                    it "passes the given variables to the template" do
                        one = make_template("1.txt", { "cells" => ["<%= one %>"] })
                        two = make_template("2.txt", { "cells" => ["<%= two %>"] })

                        result = Reports.notebooks_load_and_concatenate(
                            one, two, one: 1, two: 2
                        )
                        assert_equal %w[1 2], result["cells"]
                    end

                    it "errors if some variables do not exist" do
                        one = make_template("1.txt", { "cells" => ["<%= one %>"] })

                        e = assert_raises(NameError) do
                            Reports.notebooks_load_and_concatenate(one)
                        end
                        assert_equal :one, e.name
                    end

                    # Create a temporary template file and return its full path
                    def make_template(name, json)
                        path = @tmp / name
                        path.write(JSON.dump(json))
                        path
                    end
                end
            end
        end
    end
end
