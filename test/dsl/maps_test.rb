# frozen_string_literal: true

require "test_helper"
require "syskit/log/dsl"

module Syskit
    module Log
        module DSL
            describe "nwu_to_latlon" do
                it "converts NWU coordinates to lat/lon using the local origin" do
                    context = make_context
                    context.define_local_origin 7_463_783, 309_848, 23

                    lat, lon = context.nwu_to_latlon 0, 0, reference_latitude: -22
                    assert_in_delta(-22.9232, lat)
                    assert_in_delta(-43.1458, lon)

                    lat, lon = context.nwu_to_latlon 2000, -1000, reference_latitude: -22
                    assert_in_delta(-22.9050, lat)
                    assert_in_delta(-43.1363, lon)
                end

                it "can mass-convert NWU coordinates to lat/lon using the local origin" do
                    context = make_context
                    context.define_local_origin 7_463_783, 309_848, 23

                    x = GSL::Vector[0, 2000]
                    y = GSL::Vector[0, -1000]
                    lat, lon = context.nwu_to_latlon x, y, reference_latitude: -22

                    assert_kind_of GSL::Vector, lat
                    assert_kind_of GSL::Vector, lat

                    assert_in_delta(-22.9232, lat[0])
                    assert_in_delta(-43.1458, lon[0])
                    assert_in_delta(-22.9050, lat[1])
                    assert_in_delta(-43.1363, lon[1])
                end

                def make_context
                    context = Object.new
                    context.extend DSL
                    context
                end
            end
        end
    end
end
