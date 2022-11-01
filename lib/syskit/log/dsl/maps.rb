# frozen_string_literal: true

require "geoutm"

module Syskit
    module Log
        module DSL # rubocop:disable Style/Documentation
            # Create a Vega spec that displays a map
            #
            # The returned spec has the right projection set up for latitude/longitude
            # information. You may add layers to it with {#layer!}.
            #
            # @param [Float] latitude_deg the latitude of the map in degrees
            # @param [Float] longitude_deg the latitude of the map in degrees
            # @param [Float] zoon the level of zoom (1 is essentially the whole earth)
            def vega_map_layer(
                latitude_deg:, longitude_deg:, zoom:
            )
                unless @vega_map_template
                    path = File.join(__dir__, "maps.json.erb")
                    @vega_map_template = ERB.new(File.read(path))
                    @vega_map_template.location = "maps.json.erb"
                end

                Vega.lite(JSON.parse(@vega_map_template.result(binding)))
            end

            LocalOrigin = Struct.new :nwu_x, :nwu_y, :utm_zone

            # Define the local origin in NWU coordinates used in {#nwu_to_latlon}
            def define_local_origin(nwu_x, nwu_y, utm_zone)
                @local_origin = LocalOrigin.new(nwu_x, nwu_y, utm_zone)
            end

            # Convert Rock's NWU coordinates into latitude and longitude
            #
            # @param [Float,#each] easting the easting coordinates, either as vectors
            #   (e.g. GSL::Vector) or scalars
            # @param [Float,#each] northing the northing coordinates, either as vectors
            #   (e.g. GSL::Vector) or scalars
            # @return [[(Float,#each)]] the latitude and longitude coordinates, either as
            #   vectors (one for each) or scalars, depending on the type of arguments
            def nwu_to_utm(x, y, local_origin: @local_origin)
                if x.kind_of?(::Daru::Vector)
                    x = x.to_gsl
                    y = y.to_gsl
                end

                northing = x + local_origin.nwu_x
                easting = 1_000_000 - y - local_origin.nwu_y
                [easting, northing]
            end

            # Convert Rock's NWU coordinates into latitude and longitude
            #
            # @param [Float,#each] easting the easting coordinates, either as vectors
            #   (e.g. GSL::Vector) or scalars
            # @param [Float,#each] northing the northing coordinates, either as vectors
            #   (e.g. GSL::Vector) or scalars
            # @return [[(Float,#each)]] the latitude and longitude coordinates, either as
            #   vectors (one for each) or scalars, depending on the type of arguments
            def nwu_to_latlon(x, y, reference_latitude: nil, local_origin: @local_origin)
                easting, northing = nwu_to_utm(x, y, local_origin: local_origin)

                zone = local_origin.utm_zone.to_s
                if reference_latitude
                    zone_letter =
                        GeoUtm::UTMZones.calc_utm_default_letter(reference_latitude)
                    zone += zone_letter
                end

                utm_to_latlon(easting, northing, zone)
            end

            # Convert UTM coordinates into latitude and longitude
            #
            # @param [Float,#each] easting the easting coordinates, either as vectors
            #   (e.g. GSL::Vector) or scalars
            # @param [Float,#each] northing the northing coordinates, either as vectors
            #   (e.g. GSL::Vector) or scalars
            # @return [[(Float,#each)]] the latitude and longitude coordinates, either as
            #   vectors (one for each) or scalars, depending on the type of arguments
            def utm_to_latlon(easting, northing, zone)
                if northing.respond_to?(:each)
                    utm_to_latlon_vec(easting, northing, zone)
                else
                    utm = GeoUtm::UTM.new(zone, easting, northing)
                    latlon = utm.to_lat_lon
                    [latlon.lat, latlon.lon]
                end
            end

            # @api private
            #
            # Convert vectors of UTM coordinates into latitude and longitude
            def utm_to_latlon_vec(easting, northing, zone)
                lat = northing.dup
                lon = northing.dup
                northing.size.times.map do |i|
                    n = northing[i]
                    e = easting[i]
                    utm = GeoUtm::UTM.new(zone, e, n)
                    latlon = utm.to_lat_lon

                    lat[i] = latlon.lat
                    lon[i] = latlon.lon
                end

                [lat, lon]
            end
        end
    end
end
