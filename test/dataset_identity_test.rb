# frozen_string_literal: true

require "test_helper"

module Syskit::Log
    describe DatasetIdentity do
        describe ".validate_encoded_short_digest" do
            attr_reader :sha2
            before do
                @sha2 = Digest::SHA2.hexdigest("TEST")
            end
            it "returns the digest unmodified if it is shorted than a full SHA2 digest" do
                assert_equal sha2[0..-2],
                             DatasetIdentity.validate_encoded_short_digest(sha2[0..-2])
            end
            it "raises if the string is too long" do
                assert_raises(InvalidDigest) do
                    DatasetIdentity.validate_encoded_short_digest(sha2 + " ")
                end
            end
            it "raises if the string contains invalid characters for base64" do
                sha2[3, 1] = "_"
                assert_raises(InvalidDigest) do
                    DatasetIdentity.validate_encoded_short_digest(sha2)
                end
            end
            it "returns the digest unmodified if it is valid" do
                assert_equal sha2, DatasetIdentity.validate_encoded_short_digest(sha2)
            end
        end

        describe ".valid_encoded_sha2?" do
            attr_reader :sha2
            before do
                @sha2 = Digest::SHA2.hexdigest("TEST")
            end
            it "returns false if the string is too short" do
                refute DatasetIdentity.valid_encoded_sha2?(sha2[0..-2])
            end
            it "raises if the string is too long" do
                refute DatasetIdentity.valid_encoded_sha2?(sha2 + " ")
            end
            it "raises if the string contains invalid characters for base64" do
                sha2[3, 1] = "_"
                refute DatasetIdentity.valid_encoded_sha2?(sha2)
            end
            it "returns true it is valid" do
                assert DatasetIdentity.valid_encoded_sha2?(sha2)
            end
        end

        describe ".validate_encoded_sha2" do
            attr_reader :sha2
            before do
                @sha2 = Digest::SHA2.hexdigest("TEST")
            end
            it "raises if the string is too short" do
                assert_raises(InvalidDigest) do
                    DatasetIdentity.validate_encoded_sha2(sha2[0..-2])
                end
            end
            it "raises if the string is too long" do
                assert_raises(InvalidDigest) do
                    DatasetIdentity.validate_encoded_sha2(sha2 + " ")
                end
            end
            it "raises if the string contains invalid characters for base64" do
                sha2[3, 1] = "_"
                assert_raises(InvalidDigest) do
                    DatasetIdentity.validate_encoded_sha2(sha2)
                end
            end
            it "returns the digest unmodified if it is valid" do
                assert_equal sha2, DatasetIdentity.validate_encoded_sha2(sha2)
            end
        end
    end
end
