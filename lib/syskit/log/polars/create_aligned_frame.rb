# frozen_string_literal: true

module Syskit
    module Log
        module Polars # :nodoc:
            CHUNK_SIZE = 8_192

            def self.create_aligned_frame(
                center_time, builders, joint_stream, timeout: nil, chunk_size: CHUNK_SIZE
            )
                state = CreateAlignedFrame.new(
                    builders, timeout: timeout, chunk_size: chunk_size
                )
                joint_stream.raw_each do |index, time, sample|
                    trigger = state.update_current_samples(index, time, sample)
                    state.push_current_samples if trigger
                end

                state.push_chunks
                state.recenter_time_series(center_time)
                state.df
            end

            # @api private
            #
            # Implementation of algorithm steps and state for
            # {Polars.create_aligned_frame}
            class CreateAlignedFrame
                attr_reader :df

                def initialize(builders, timeout: nil, chunk_size: CHUNK_SIZE)
                    @builders = builders
                    @current_samples = Array.new(builders.size)
                    @chunks = builders.map do |b|
                        b.create_chunks(CHUNK_SIZE)
                    end
                    @chunk_size = chunk_size

                    @df = ::Polars::DataFrame.new(
                        builders.flat_map { |b| b.create_series([]) }
                    )

                    @row_count = 0
                    @initialized = false
                    @master_deadline = nil
                    @timeout = timeout
                end

                def update_current_samples(index, time, sample)
                    deadline = time + @timeout if @timeout
                    @current_samples[index] = [time, sample, deadline]
                    @master_deadline = deadline if index == 0
                    if @initialized
                        index == 0 && (!@master_deadline || time < @master_deadline)
                    else
                        @initialized = !@current_samples.index(nil)
                    end
                end

                def push_current_samples
                    ref_time = @current_samples[0][0]
                    @current_samples
                        .each_with_index do |(v_time, v_sample, v_deadline), v_index|
                            if v_deadline && (v_deadline < ref_time)
                                update_current_row_na(v_index)
                            else
                                update_current_row(v_index, v_time, v_sample)
                            end
                        end

                    @row_count += 1
                    push_chunks if @row_count == @chunk_size
                end

                def update_current_row_na(index)
                    @builders[index].update_row_na(@chunks[index], @row_count)
                end

                def update_current_row(index, time, sample)
                    @builders[index].update_row(@chunks[index], @row_count, time, sample)
                end

                def self.truncate_chunks(chunks, size)
                    chunks.map do |builder_chunks|
                        builder_chunks.map { |a| a[0, size] }
                    end
                end

                def self.create_dataframe(builders, chunks)
                    series = builders.zip(chunks).flat_map do |b, b_chunks|
                        b.create_series(b_chunks)
                    end
                    ::Polars::DataFrame.new(series)
                end

                def push_chunks
                    return @df if @row_count == 0

                    chunks = CreateAlignedFrame.truncate_chunks(@chunks, @row_count)
                    chunk_df = CreateAlignedFrame.create_dataframe(@builders, chunks)

                    @row_count = 0
                    @df = @df.vstack(chunk_df)
                end

                def recenter_time_series(center_time)
                    @builders.each do |b|
                        b.recenter_time_series(@df, center_time)
                    end
                end
            end
        end
    end
end
