# frozen_string_literal: true

module Syskit::Log
    # Exception raised in resolution methods when one match was expected but more
    # than one was found
    class Ambiguous < ArgumentError
    end

    # Exception raised in resolution methods when no information can allow to
    # infer the result
    class Unknown < ArgumentError
    end

    # Exception raised when we were expecting an output port for a given stream
    # but did not get one
    class MissingStream < ArgumentError
    end

    # Exception raised when the type in the log streams do not match the type
    # expected by Syskit
    class MismatchingType < ArgumentError
        attr_reader :stream, :port

        def initialize(stream, port)
            @stream = stream
            @port = port
        end

        def pretty_print(pp)
            if stream.type.name == port.type.name
                pp.text "definition of #{stream.type.name} seem to have changed between the current system and the log streams"
                pp.text " while trying to replay stream #{stream.name} as port #{port}"
                pp.breakable
                pp.text "the log stream definition is"
                pp.breakable
                stream.type.pretty_print(pp, true)
                pp.breakable
                pp.text "the expected port type is"
                pp.breakable
                port.type.pretty_print(pp, true)
            end
        end
    end
end
