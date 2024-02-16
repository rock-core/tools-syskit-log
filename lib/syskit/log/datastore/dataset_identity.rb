# frozen_string_literal: true

module Syskit::Log
    class InvalidDigest < ArgumentError; end

    # Helper methods related to computation of dataset identity (i.e. hashes)
    module DatasetIdentity
        # The way we encode digests into strings
        #
        # Make sure you change {ENCODED_DIGEST_LENGTH} and
        # {validate_encoded_digest} if you change this
        DIGEST_ENCODING_METHOD = :hexdigest

        # Length in characters of the digests once encoded in text form
        #
        # We're encoding a sha256 digest in hex, so that's 64 characters
        ENCODED_DIGEST_LENGTH = 64

        # @overload digest(string)
        #   Computes the digest of a string
        #
        # @overload digest
        #   Returns a Digest object that can be used to digest data
        def self.digest(string = nil)
            digest = Digest::SHA256.new
            digest.update(string) if string
            digest
        end

        # @overload string_digest(digest)
        #   Computes the string representation of a digest
        #
        # @overload string_digest(string)
        #   Computes the string representation of a string's digest
        def self.string_digest(object)
            object = digest(object) if object.respond_to?(:to_str)
            object.send(DIGEST_ENCODING_METHOD)
        end

        def self.validate_encoded_short_digest(digest)
            if digest.length > ENCODED_DIGEST_LENGTH
                raise InvalidDigest,
                      "#{digest} does not look like a valid SHA2 short digest "\
                      "encoded with #{DIGEST_ENCODING_METHOD}. Expected at most "\
                      "#{ENCODED_DIGEST_LENGTH} characters but got #{digest.length}"
            elsif digest !~ /^[0-9a-f]+$/
                raise InvalidDigest,
                      "#{digest} does not look like a valid SHA2 digest encoded "\
                      "with #{DIGEST_ENCODING_METHOD}. "\
                      "Expected characters in 0-9a-zA-Z+"
            end
            digest
        end

        # Validate that the given digest is a valid dataset ID
        #
        # See {valid_encoded_digest?} to for a true/false check
        #
        # @param [String]
        # @raise [InvalidDigest]
        def self.validate_encoded_digest(digest)
            validate_encoded_sha2(digest)
        end

        # @api private
        #
        # Implementation of {.validate_encoded_digest} for SHA2 hashes
        def self.validate_encoded_sha2(sha2)
            if sha2.length != ENCODED_DIGEST_LENGTH
                raise InvalidDigest,
                      "#{sha2} does not look like a valid SHA2 digest encoded "\
                      "with #{DIGEST_ENCODING_METHOD}. Expected "\
                      "#{ENCODED_DIGEST_LENGTH} characters but got #{sha2.length}"
            elsif sha2 !~ /^[0-9a-f]+$/
                raise InvalidDigest,
                      "#{sha2} does not look like a valid SHA2 digest encoded "\
                      "with #{DIGEST_ENCODING_METHOD}. "\
                      "Expected characters in 0-9a-zA-Z+/"
            end
            sha2
        end

        # Checks if the given digest is a valid dataset ID
        #
        # @see validate_encoded_digest
        def self.valid_encoded_digest?(digest)
            valid_encoded_sha2?(digest)
        end

        # @api private
        #
        # Implementation of {.valid_encoded_digest?} for SHA2 hashes
        def self.valid_encoded_sha2?(sha2)
            sha2.length == ENCODED_DIGEST_LENGTH &&
                /^[0-9a-f]+$/.match?(sha2)
        end

        # @api private
        #
        # Compute the encoded SHA2 digest of a file
        def self.compute_file_digest(io)
            digest = self.digest
            while (block = io.read(1024 * 1024))
                digest.update(block)
            end
            DatasetIdentity.string_digest(digest)
        end
    end
end
