# frozen_string_literal: true

require "test_helper"
require "syskit/log/daru"

module Syskit
    module Log
        module Daru # :nodoc:
            describe FrameBuilder do
                before do
                    @registry = Typelib::Registry.new
                    @uint64_t = @registry.create_numeric "/uint64_t", 8, :uint
                    @base_time_t = @registry.create_compound "/base/Time" do |c|
                        c.microseconds = "/uint64_t"
                    end

                    @compound_t = @registry.create_compound "/Compound" do |c|
                        c.time = "/base/Time"
                        c.value = "/uint64_t"
                    end
                end

                it "extracts data into a column" do
                    samples = mock_samples do |i|
                        @compound_t.new(
                            time: { microseconds: i * 1_000_000 + 3 },
                            value: i
                        )
                    end

                    builder = FrameBuilder.new(@compound_t)
                    builder.add(&:value)

                    start_time = Time.at(0, 3)
                    frame = builder.to_daru_frame(start_time, samples)

                    assert_equal (0..9).to_a, frame.index.to_a
                    assert_equal (1..10).to_a, frame[".value"].to_a
                end

                it "allows to override the column name" do
                    samples = mock_samples do |i|
                        @compound_t.new(
                            time: { microseconds: i * 1_000_000 + 3 },
                            value: i
                        )
                    end

                    builder = FrameBuilder.new(@compound_t)
                    builder.add("name", &:value)

                    start_time = Time.at(0, 3)
                    frame = builder.to_daru_frame(start_time, samples)

                    assert_equal (0..9).to_a, frame.index.to_a
                    assert_equal (1..10).to_a, frame["name"].to_a
                end

                it "applies the transform block if one is defined" do
                    samples = mock_samples do |i|
                        @compound_t.new(
                            time: { microseconds: i * 1_000_000 + 3 },
                            value: i * 2
                        )
                    end

                    builder = FrameBuilder.new(@compound_t)
                    builder.add { |b| b.value.transform { |i| i * 2 } }

                    start_time = Time.at(0, 3)
                    frame = builder.to_daru_frame(start_time, samples)

                    assert_equal (0..9).to_a, frame.index.to_a
                    assert_equal (4..40).step(4).to_a, frame[".value"].to_a
                end

                it "converts to seconds and centers columns declared as time fields" do
                    samples = mock_samples do |i|
                        @compound_t.new(
                            time: { microseconds: i * 1_000_000 + 3 },
                            value: i * 2
                        )
                    end

                    builder = FrameBuilder.new(@compound_t)
                    builder.add_time_field(".time") { |b| b.time.microseconds }

                    start_time = Time.at(0, 3)
                    frame = builder.to_daru_frame(start_time, samples)

                    assert_equal (0..9).to_a, frame.index.to_a
                    assert_equal (1..10).to_a, frame[".time"].to_a
                end

                it "allow to create a column from the sample's logical time" do
                    compound_t = @registry.create_compound "/C" do |c|
                        c.value = "/uint64_t"
                    end

                    samples = mock_samples do |i|
                        compound_t.new(value: i * 2)
                    end

                    builder = FrameBuilder.new(compound_t)
                    builder.add_logical_time
                    builder.add(&:value)

                    start_time = Time.at(0, 2)
                    frame = builder.to_daru_frame(start_time, samples)

                    assert_equal (0..9).to_a, frame.index.to_a
                    assert_equal (1..10).to_a, frame["time"].to_a
                    assert_equal (2..20).step(2).to_a, frame[".value"].to_a
                end

                it "lets us set the logical time's column name" do
                    compound_t = @registry.create_compound "/C" do |c|
                        c.value = "/uint64_t"
                    end

                    samples = mock_samples do |i|
                        compound_t.new(value: i * 2)
                    end

                    builder = FrameBuilder.new(compound_t)
                    builder.add_logical_time("lg")
                    builder.add(&:value)

                    start_time = Time.at(0, 2)
                    frame = builder.to_daru_frame(start_time, samples)

                    assert_equal (0..9).to_a, frame.index.to_a
                    assert_equal (1..10).to_a, frame["lg"].to_a
                    assert_equal (2..20).step(2).to_a, frame[".value"].to_a
                end

                def mock_samples
                    samples = flexmock
                    iterations = (1..10).map do |i|
                        rt = Time.at(i, 1)
                        lg = Time.at(i, 2)
                        [rt, lg, yield(i)]
                    end

                    samples.should_receive(:raw_each)
                           .and_iterates(*iterations)
                    samples.should_receive(:size)
                           .and_return(10)
                    samples
                end
            end
        end
    end
end
