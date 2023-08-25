# frozen_string_literal: true

module Syskit
    module Log
        # Thin shim that provides the API necessary to process
        # zst-compressed files in some operations that do not require to
        # cache the result
        #
        # It is very limited to what the other datastore classes need
        class ZstdIO
            DECOMPRESS_READ_SIZE = 1024**2

            def initialize(io, read: true, write: false)
                @io = io
                @mode_read = read
                @mode_write = write
                @in_buffer = +"" if read
                rewind
            end

            def path
                @io.path
            end

            def rewind
                @io.rewind
                @tell = 0
                @buffer = +""
                @zstd_in = Zstd::StreamingDecompress.new if @mode_read
                @zstd_out = Zstd::StreamingCompress.new if @mode_write
            end

            def eof?
                @io.eof? && @buffer.empty?
            end

            def closed?
                @io.closed?
            end

            attr_reader :tell

            # Return the size when the file has been read in its entirety
            #
            # @raise [ArgumentError] if the file has not yet been read completely
            def size
                return @tell if @io.eof?

                raise ArgumentError,
                      "cannot know the size until the file has been read "\
                      "in its entirety"
            end

            # Read at most count bytes of decompressed data
            def read(count = nil)
                return read_all unless count

                eof_reached = read_in_buffer(count)

                ret = @buffer[0, count]
                @tell += ret.size
                @buffer = @buffer[ret.size..-1] || +""
                ret if !ret.empty? || !eof_reached
            end

            # @api private
            #
            # Read at least this many bytes of decompressed data in the internal buffer
            def read_in_buffer(count)
                while @buffer.size < count
                    break unless (data = @io.read(DECOMPRESS_READ_SIZE, @in_buffer))

                    @buffer.concat(@zstd_in.decompress(data))
                end

                !data
            end

            # @api private
            #
            # Read and return all file data
            def read_all
                Zstd.decompress(@io.read)
            end

            # Write this data in the compressed stream
            def write(buffer)
                raise ArgumentError, "not opened for writing" unless @zstd_out

                @tell += buffer.size
                compressed = @zstd_out.compress(buffer)
                @io.write(compressed)
            end

            # Seek in the IO. Can only seek forward
            def seek(pos, mode = IO::SEEK_SET)
                if mode == IO::SEEK_SET
                    raise ArgumentError, "cannot seek backwards" if pos < @tell

                    read(pos - @tell)
                elsif mode == IO::SEEK_CUR
                    raise ArgumentError, "cannot seek backwards" if pos < 0

                    read(pos)
                else
                    raise ArgumentError, "seek mode #{mode} not supported"
                end
            end

            def flush
                @io.write(@zstd_out.flush) if @zstd_out
                @io.flush
            end

            def close
                @io.write(@zstd_out.finish) if @zstd_out
                @io.close
            end
        end
    end
end
