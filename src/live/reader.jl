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

"""
    _reader_loop_typed(c::Live)

Typed-mode reader loop. Reads records off the socket, routes data records
to the schema's typed channel and control records (ErrorMsg / SystemMsg /
SymbolMappingMsg) to the control channel. Unknown rtypes are silently
skipped, matching the untyped path.

Dispatch strategy: a flat `if/elseif` tree, one branch per supported
schema rtype, with per-branch concretely-typed channels resolved at task
start. This is verbose but type-stable — each `put!(ch, rec)` is
monomorphic, no dynamic dispatch per record.
"""
function _reader_loop_typed(c::Live)
    # Resolve channels per schema, typed concretely. `_typed_channel_or_nothing`
    # returns `Channel{T}` for a subscribed schema and `nothing` otherwise.
    ch_trades   = _typed_channel_or_nothing(c, DBN.TradeMsg)
    ch_mbo      = _typed_channel_or_nothing(c, DBN.MBOMsg)
    ch_mbp1     = _typed_channel_or_nothing(c, DBN.MBP1Msg)
    ch_mbp10    = _typed_channel_or_nothing(c, DBN.MBP10Msg)
    ch_ohlcv    = _typed_channel_or_nothing(c, DBN.OHLCVMsg)
    ch_def      = _typed_channel_or_nothing(c, DBN.InstrumentDefMsg)
    ch_status   = _typed_channel_or_nothing(c, DBN.StatusMsg)
    ch_imbal    = _typed_channel_or_nothing(c, DBN.ImbalanceMsg)
    ch_stat     = _typed_channel_or_nothing(c, DBN.StatMsg)
    ch_cmbp1    = _typed_channel_or_nothing(c, DBN.CMBP1Msg)
    ch_cbbo1s   = _typed_channel_or_nothing(c, DBN.CBBO1sMsg)
    ch_cbbo1m   = _typed_channel_or_nothing(c, DBN.CBBO1mMsg)
    ch_tcbbo    = _typed_channel_or_nothing(c, DBN.TCBBOMsg)
    ch_bbo1s    = _typed_channel_or_nothing(c, DBN.BBO1sMsg)
    ch_bbo1m    = _typed_channel_or_nothing(c, DBN.BBO1mMsg)

    ctrl_chan = c.control_channel

    try
        raw = c.compression == Compression.ZSTD ?
              TranscodingStream(ZstdDecompressor(), c.socket) :
              c.socket
        counting = CountingIO(raw)
        buffered = DBN.BufferedReader(counting)
        decoder  = DBN.DBNDecoder(buffered)
        DBN.read_header!(decoder)

        while !c.closed
            hd_result = try
                DBN.read_record_header(decoder.io)
            catch e
                if c.closed
                    break
                else
                    rethrow(e)
                end
            end

            if hd_result isa Tuple
                _, _, record_length = hd_result
                skip(decoder.io, record_length - 2)
                continue
            end

            hd = hd_result
            rt = hd.rtype

            # Hot path: data-record rtypes, type-stable put!. The nullness
            # check on the per-schema channel is FIRST in each branch so that
            # unsubscribed schemas short-circuit on a pointer comparison
            # (~1 cycle) before the slower rtype enum comparison. For a
            # single-schema subscription this collapses the 13 unsubscribed
            # branches to 13 fast nothing-checks instead of 13 enum
            # comparisons.
            if ch_trades !== nothing && rt == DBN.RType.MBP_0_MSG
                put!(ch_trades, DBN.read_trade_msg(decoder, hd))
                continue
            elseif ch_mbo !== nothing && rt == DBN.RType.MBO_MSG
                put!(ch_mbo, DBN.read_mbo_msg(decoder, hd))
                continue
            elseif ch_mbp1 !== nothing && rt == DBN.RType.MBP_1_MSG
                put!(ch_mbp1, DBN.read_mbp1_msg(decoder, hd))
                continue
            elseif ch_mbp10 !== nothing && rt == DBN.RType.MBP_10_MSG
                put!(ch_mbp10, DBN.read_mbp10_msg(decoder, hd))
                continue
            elseif ch_ohlcv !== nothing && (rt == DBN.RType.OHLCV_1S_MSG ||
                                            rt == DBN.RType.OHLCV_1M_MSG ||
                                            rt == DBN.RType.OHLCV_1H_MSG ||
                                            rt == DBN.RType.OHLCV_1D_MSG)
                put!(ch_ohlcv, DBN.read_ohlcv_msg(decoder, hd))
                continue
            elseif ch_status !== nothing && rt == DBN.RType.STATUS_MSG
                put!(ch_status, DBN.read_status_msg(decoder, hd))
                continue
            elseif ch_def !== nothing && rt == DBN.RType.INSTRUMENT_DEF_MSG
                put!(ch_def, DBN.read_instrument_def_msg(decoder, hd))
                continue
            elseif ch_imbal !== nothing && rt == DBN.RType.IMBALANCE_MSG
                put!(ch_imbal, DBN.read_imbalance_msg(decoder, hd))
                continue
            elseif ch_stat !== nothing && rt == DBN.RType.STAT_MSG
                put!(ch_stat, DBN.read_stat_msg(decoder, hd))
                continue
            elseif ch_cmbp1 !== nothing && rt == DBN.RType.CMBP_1_MSG
                put!(ch_cmbp1, DBN.read_cmbp1_msg(decoder, hd))
                continue
            elseif ch_cbbo1s !== nothing && rt == DBN.RType.CBBO_1S_MSG
                put!(ch_cbbo1s, DBN.read_cbbo1s_msg(decoder, hd))
                continue
            elseif ch_cbbo1m !== nothing && rt == DBN.RType.CBBO_1M_MSG
                put!(ch_cbbo1m, DBN.read_cbbo1m_msg(decoder, hd))
                continue
            elseif ch_tcbbo !== nothing && rt == DBN.RType.TCBBO_MSG
                put!(ch_tcbbo, DBN.read_tcbbo_msg(decoder, hd))
                continue
            elseif ch_bbo1s !== nothing && rt == DBN.RType.BBO_1S_MSG
                put!(ch_bbo1s, DBN.read_bbo1s_msg(decoder, hd))
                continue
            elseif ch_bbo1m !== nothing && rt == DBN.RType.BBO_1M_MSG
                put!(ch_bbo1m, DBN.read_bbo1m_msg(decoder, hd))
                continue
            end

            # Control rtypes → control channel via generic dispatch.
            if rt == DBN.RType.ERROR_MSG ||
               rt == DBN.RType.SYSTEM_MSG ||
               rt == DBN.RType.SYMBOL_MAPPING_MSG
                rec = DBN.read_record_dispatch(decoder, hd, rt)
                if rec !== nothing && ctrl_chan !== nothing && isopen(ctrl_chan)
                    put!(ctrl_chan, rec)
                end
                continue
            end

            # Unrecognised rtype or known rtype with no matching subscription
            # — consume the body and move on.
            skip(decoder.io, Int(hd.length) * DBN.LENGTH_MULTIPLIER - 16)
        end
    catch e
        if !c.closed
            try
                @error "Live typed reader task crashed" exception=(e, catch_backtrace())
            catch
            end
        end
    finally
        # Close every channel we own so any consumer task break out of its
        # take! loop. The Live.close() path will also try this, but doing it
        # here ensures cleanup on reader-side crash.
        for ch in values(c.typed_data_channels)
            try
                isopen(ch) && close(ch)
            catch
            end
        end
        try
            c.control_channel === nothing ||
                (isopen(c.control_channel) && close(c.control_channel))
        catch
        end
    end
    return nothing
end

# Resolve the subscribed typed channel for a concrete record type, type-stable.
# Returns `Channel{T}` if the matching schema was subscribed, else `nothing`.
function _typed_channel_or_nothing(c::Live, ::Type{T}) where {T}
    for (_, ch) in c.typed_data_channels
        if ch isa Channel{T}
            return ch::Channel{T}
        end
    end
    return nothing
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
