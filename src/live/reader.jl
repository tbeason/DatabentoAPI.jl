# Background task that drains the TCP socket and pushes parsed DBN records onto the
# client's channel.

# DBN.read_header! and DBN.BufferedReader both call `position(io)`, which TCPSocket
# does not implement. CountingIO wraps any IO and tracks its own byte offset, giving
# BufferedReader something to delegate to.
mutable struct CountingIO{T<:IO} <: IO
    io::T
    pos::Int64
end
CountingIO(io::IO) = CountingIO(io, Int64(0))

Base.position(c::CountingIO) = c.pos
Base.eof(c::CountingIO)      = eof(c.io)
Base.isopen(c::CountingIO)   = isopen(c.io)
Base.close(c::CountingIO)    = close(c.io)
Base.bytesavailable(c::CountingIO) = bytesavailable(c.io)

function Base.read(c::CountingIO, ::Type{UInt8})
    v = read(c.io, UInt8); c.pos += 1; v
end

function Base.unsafe_read(c::CountingIO, p::Ptr{UInt8}, n::UInt)
    unsafe_read(c.io, p, n); c.pos += Int64(n); nothing
end

function Base.readbytes!(c::CountingIO, b::AbstractVector{UInt8}, n = length(b))
    r = readbytes!(c.io, b, n); c.pos += r; r
end

function Base.read(c::CountingIO, n::Integer)
    bs = read(c.io, n); c.pos += length(bs); bs
end

function _reader_loop(c::Live)
    try
        # Wrap with zstd decompressor first if the session was negotiated with
        # `compression=zstd`. Then layer CountingIO (for position tracking) and
        # BufferedReader (for syscall reduction) before handing to DBN.
        raw = c.compression == Compression.ZSTD ?
              TranscodingStream(ZstdDecompressor(), c.socket) :
              c.socket
        counting = CountingIO(raw)
        buffered = DBN.BufferedReader(counting)
        decoder  = DBN.DBNDecoder(buffered)
        DBN.read_header!(decoder)
        while !c.closed
            rec = try
                DBN.read_record(decoder)
            catch e
                if c.closed
                    break
                else
                    rethrow(e)
                end
            end
            # DBN.read_record returns `nothing` for either EOF or an unknown rtype.
            # We treat both as end-of-stream — TCP `isopen`/`eof` checks aren't
            # reliable enough on Windows for half-closed sockets to differentiate
            # safely without busy-looping.
            rec === nothing && break
            put!(c.channel, rec)
            # ErrorMsg is informational on this stream (e.g. SkippedRecords-
            # AfterSlowReading is a code-7 ErrorMsg signalling SKIP behavior, not a
            # fatal error). Forward it through the channel and let the consumer
            # decide; only EOF on the socket terminates the reader loop.
        end
    catch e
        if !c.closed
            try
                @error "Live reader task crashed" exception=(e, catch_backtrace())
            catch
            end
        end
    finally
        try
            isopen(c.channel) && close(c.channel)
        catch
        end
    end
    return nothing
end
