# frozen_string_literal: true

module Syskit
    module Log
        module DSL # :nodoc:
            # Create the output streams to export the given alignment builders
            #
            # @param [Pocolog::Logfiles] logfile
            # @param [Array<AlignmentBuilder>] builders
            # @return [Array<Array<DataStream>>] per-builder output streams. For a
            #   given builder, the streams are ordered as the outputs are defined
            #   in the builder itself ({AlignmentBuilder#each_output_stream})
            def self.export_to_single_file_create_output_streams(logfile, builders)
                builders.map do |b|
                    b.each_output_stream.map do |output_stream|
                        logfile.create_stream(
                            output_stream.name, output_stream.type, output_stream.metadata
                        )
                    end
                end
            end

            # Create the output streams to export the given alignment builders
            #
            # @param [Pocolog::Logfiles] logfile
            # @param [Array<AlignmentBuilder>] builders
            # @return [Array<Array<DataStream>>] per-builder output streams. For a
            #   given builder, the streams are ordered as the outputs are defined
            #   in the builder itself ({AlignmentBuilder#each_output_stream})
            def self.export_to_single_file_process(
                builders, log_file_streams, joint_stream
            )
                joint_stream.raw_each do |index, time, raw_sample|
                    this_builder = builders[index]
                    this_streams = log_file_streams[index]
                    this_builder
                        .process(time, raw_sample)
                        .each_with_index do |(processed_time, processed_sample), i|
                            this_streams[i].write(
                                processed_time, processed_time, processed_sample
                            )
                        end
                end
            end
        end
    end
end
