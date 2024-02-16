# frozen_string_literal: true

module Syskit::Log
    # @api private
    #
    # An IO-looking object that computes the output's digest
    class DigestIO < SimpleDelegator
        attr_reader :digest

        DEFAULT_DIGEST = Digest::SHA256

        def initialize(io, digest = DEFAULT_DIGEST.new)
            super(io)
            @digest = digest
        end

        def read(*)
            data = super
            @digest.update(data) if data
            data
        end

        def write(string)
            super
            @digest.update(string)
        end

        def string_digest
            DatasetIdentity.string_digest(@digest)
        end
    end
end
