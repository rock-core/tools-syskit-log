# frozen_string_literal: true

module Syskit
    module Log
        module Daru
            def self.build_aligned_vectors(center_time, builders, joint_stream, size,
                                           timeout: nil)
                current_row = Array.new(builders.size)
                initialized = false

                vectors = builders.map { |b| b.create_vectors(size) }
                na = builders.map(&:na_values)

                row_count = 0
                master_deadline = nil
                joint_stream.raw_each do |index, time, sample|
                    if row_count == size
                        size, vectors = aligned_vectors_grow(size, vectors, builders)
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
                            builders[v_index].update_row_na(
                                vectors[v_index], row_count, na[v_index]
                            )
                        else
                            builders[v_index].update_row(
                                vectors[v_index], row_count, v_time, v_sample
                            )
                        end
                    end

                    row_count += 1
                end

                # Resize the vectors
                vectors = builders.zip(vectors).map do |b, v|
                    v = b.truncate_vectors(v, row_count)
                    b.recenter_time_vectors(v, center_time)
                    v
                end

                vectors.flatten(1)
            end

            def self.create_aligned_frame(center_time, builders, joint_stream, size, timeout: nil)
                vectors = build_aligned_vectors(center_time, builders, joint_stream, size, timeout: timeout)
                names = builders.flat_map(&:column_names)

                ::Daru::DataFrame.new(Hash[names.zip(vectors)])
            end

            def self.aligned_vectors_grow(current_size, vectors, builders)
                size =
                    if current_size < 128
                        current_size * 2
                    else
                        (current_size * 6 - 1) / 4
                    end

                vectors = vectors.each_with_index.map do |v, i|
                    builders[i].resize_vectors(v, size)
                end
                [size, vectors]
            end
        end
    end
end
