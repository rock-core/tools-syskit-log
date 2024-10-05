# frozen_string_literal: true

module Syskit
    module Log
        module Polars
            def self.create_dataframe(builders, chunks)
                series = builders.zip(chunks).flat_map do |b, b_chunks|
                    b.create_series(b_chunks)
                end
                ::Polars::DataFrame.new(series)
            end

            CHUNK_SIZE = 10_000

            def self.create_aligned_frame(
                center_time, builders, joint_stream, timeout: nil
            )
                current_row = Array.new(builders.size)
                initialized = false

                chunk = builders.map do |b|
                    b.create_chunks(CHUNK_SIZE)
                end

                df = ::Polars::DataFrame.new(
                    builders.flat_map { |b| b.create_series([]) }
                )

                row_count = 0
                master_deadline = nil
                joint_stream.raw_each do |index, time, sample|
                    if row_count == CHUNK_SIZE
                        chunk_df = create_dataframe(builders, chunk)
                        df = df.vstack(chunk_df)
                        row_count = 0
                    end

                    deadline = time + timeout if timeout
                    current_row[index] = [time, sample, deadline]
                    master_deadline = deadline if index == 0

                    if initialized
                        if index != 0
                            next unless master_deadline && master_deadline < time
                        end
                    elsif current_row.index(nil)
                        next
                    end
                    initialized = true

                    ref_time = current_row[0][0]
                    current_row.each_with_index do |(v_time, v_sample, v_deadline), v_index|
                        if v_deadline && (v_deadline < ref_time)
                            builders[v_index].update_row_na(chunk[v_index], row_count)
                        else
                            builders[v_index].update_row(
                                chunk[v_index], row_count, v_time, v_sample
                            )
                        end
                    end

                    row_count += 1
                end

                if row_count > 0
                    chunk = chunk.map do |builder_chunks|
                        builder_chunks.map { |a| a[0, row_count] }
                    end
                    chunk_df = create_dataframe(builders, chunk)
                    df = df.vstack(chunk_df)
                end

                # Resize the vectors
                builders.each do |b|
                    b.recenter_time_series(df, center_time)
                end

                df
            end
        end
    end
end
