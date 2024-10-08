# frozen_string_literal: true

module Syskit
    module Log
        module Polars
            # Polars-specific functionality from {Log::DSL}
            module DSL
                # Convert a Daru frame into a vega data array
                def polars_to_vega(frame)
                    frame.fill_nan(nil).to_a
                end

                # Convert fields of a data stream into a Polars frame
                #
                # @param [Array] streams an array if objects that can be converted to
                #   samples using {#samples_of}
                # @param [Boolean] accurate prefer accuracy over speed (see below)
                # @param [Float,nil] timeout how long, since the last received sample
                #   from a stream, the method will start introducing NAs to replace
                #   the stream's values (NA is either NAN for float values, or nil)
                # @yield a {FrameBuilder} object used to describe the frame to be built
                #
                # This method uses the first given stream as a "master" stream, and
                # attempts to load the value of the remaining columns at the same
                # real time than the value from the master stream.
                #
                # How the method deals with resampling (when {#interval_sample_every} has
                # been called) depends on the `accurate` parameter. When `false`,
                # the streams are first re-sampled and then aligned. When doing coarse
                # sampling, this can introduce significant misalignments. When true,
                # the method resamples only the first stream, and then aligns the
                # other full non-resampled streams. accurante: false is significantly
                # faster for very dense streams (w.r.t. the sampling period)
                def to_polars_frame(
                    *streams, accurate: false, timeout: nil,
                    chunk_size: Polars::CHUNK_SIZE
                )
                    return ::Polars::DataFrame.new if streams.empty?

                    samples =
                        if accurate
                            to_polars_frame_accurate_samples(streams)
                        else
                            to_polars_frame_samples(streams)
                        end

                    builders = streams.map { |s| Polars::FrameBuilder.new(s.type) }
                    yield(*builders)

                    center_time = @interval_zero_time || streams.first.interval_lg[0]

                    to_polars_frame_execute(
                        builders, center_time, samples,
                        timeout: timeout, chunk_size: chunk_size
                    )
                end

                # @api private
                def to_polars_frame_execute(
                    builders, center_time, samples, timeout:, chunk_size:
                )
                    if builders.size == 1
                        builders.first.to_polars_frame(
                            center_time, samples.first,
                            timeout: timeout, chunk_size: chunk_size
                        )
                    else
                        joint_stream = Pocolog::StreamAligner.new(false, *samples)
                        Polars.create_aligned_frame(
                            center_time, builders, joint_stream,
                            timeout: timeout, chunk_size: chunk_size
                        )
                    end
                end

                # @api private
                #
                # Create the samples enumeration objects for to_polars_frame in the
                # accurate case
                def to_polars_frame_accurate_samples(streams)
                    interval_start, interval_end = to_polars_frame_interval(streams)
                    first_samples =
                        samples_of(streams[0], from: interval_start, to: interval_end)

                    [first_samples] + streams[1..-1].map do |s|
                        samples_of(s, from: interval_start, to: interval_end,
                                      every_samples: nil, every_seconds: nil)
                    end
                end

                # @api private
                #
                # Create the samples enumeration objects for to_polars_frame in the
                # normal case
                def to_polars_frame_samples(streams)
                    interval_start, interval_end = to_polars_frame_interval(streams)
                    streams.map do |s|
                        samples_of(s, from: interval_start, to: interval_end)
                    end
                end

                # @api private
                #
                # Compute the frame interval for to_polars_frame
                def to_polars_frame_interval(streams)
                    interval_start, interval_end = streams.map(&:interval_lg).transpose
                    interval_start = interval_start.min
                    interval_end = interval_end.max
                    interval_start = [interval_start, @interval[0]].max if @interval[0]
                    interval_end = [interval_end, @interval[1]].min if @interval[1]
                    [interval_start, interval_end]
                end
            end
        end
    end
end
