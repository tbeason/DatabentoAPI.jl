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
    # Resolve channels per SCHEMA (not per record type) so subscriptions
    # to schemas sharing a concrete record type get distinct channels.
    # OHLCV_1S/1M/1H/1D all have type OHLCVMsg but different rtypes; we
    # need a separate channel for each subscribed interval. TBBO uses the
    # MBP1Msg layout (same as MBP_1) but is conceptually a different
    # subscription. The dispatch tree below maps each rtype to the right
    # per-schema channel, broadcasting only where a single rtype can
    # legitimately serve multiple subscriptions (e.g. MBP_1 + TBBO both
    # tagged MBP_1_MSG).
    ch_trades    = _channel_for(c, Schema.TRADES,     DBN.TradeMsg)
    ch_mbo       = _channel_for(c, Schema.MBO,        DBN.MBOMsg)
    ch_mbp1      = _channel_for(c, Schema.MBP_1,      DBN.MBP1Msg)
    ch_tbbo      = _channel_for(c, Schema.TBBO,       DBN.MBP1Msg)
    ch_mbp10     = _channel_for(c, Schema.MBP_10,     DBN.MBP10Msg)
    ch_ohlcv_1s  = _channel_for(c, Schema.OHLCV_1S,   DBN.OHLCVMsg)
    ch_ohlcv_1m  = _channel_for(c, Schema.OHLCV_1M,   DBN.OHLCVMsg)
    ch_ohlcv_1h  = _channel_for(c, Schema.OHLCV_1H,   DBN.OHLCVMsg)
    ch_ohlcv_1d  = _channel_for(c, Schema.OHLCV_1D,   DBN.OHLCVMsg)
    ch_def       = _channel_for(c, Schema.DEFINITION, DBN.InstrumentDefMsg)
    ch_status    = _channel_for(c, Schema.STATUS,     DBN.StatusMsg)
    ch_imbal     = _channel_for(c, Schema.IMBALANCE,  DBN.ImbalanceMsg)
    ch_stat      = _channel_for(c, Schema.STATISTICS, DBN.StatMsg)
    ch_cmbp1     = _channel_for(c, Schema.CMBP_1,     DBN.CMBP1Msg)
    ch_cbbo1s    = _channel_for(c, Schema.CBBO_1S,    DBN.CBBO1sMsg)
    ch_cbbo1m    = _channel_for(c, Schema.CBBO_1M,    DBN.CBBO1mMsg)
    ch_tcbbo     = _channel_for(c, Schema.TCBBO,      DBN.TCBBOMsg)
    ch_bbo1s     = _channel_for(c, Schema.BBO_1S,     DBN.BBO1sMsg)
    ch_bbo1m     = _channel_for(c, Schema.BBO_1M,     DBN.BBO1mMsg)

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
                # See _reader_loop for the EOFError rationale.
                if c.closed || e isa EOFError
                    break
                else
                    rethrow(e)
                end
            end

            if hd_result isa Tuple
                # Unknown rtype. `record_length` is in 4-byte units; the
                # 2-byte header (length + rtype) is already consumed.
                _, _, record_length = hd_result
                skip(decoder.io, Int(record_length) * DBN.LENGTH_MULTIPLIER - 2)
                continue
            end

            hd = hd_result
            rt = hd.rtype

            # Hot path: data-record rtypes, type-stable put! to the
            # per-schema channel. Nullness check FIRST in each branch so
            # unsubscribed schemas short-circuit on a pointer comparison
            # before the slower rtype enum comparison. `_put_data!` updates
            # per-instrument replay timestamps on the Live client before
            # publishing, so the reconnect supervisor sees them.
            if ch_trades !== nothing && rt == DBN.RType.MBP_0_MSG
                _put_data!(c, ch_trades, DBN.read_trade_msg(decoder, hd))
                continue
            elseif ch_mbo !== nothing && rt == DBN.RType.MBO_MSG
                _put_data!(c, ch_mbo, DBN.read_mbo_msg(decoder, hd))
                continue
            elseif (ch_mbp1 !== nothing || ch_tbbo !== nothing) && rt == DBN.RType.MBP_1_MSG
                # MBP_1 and TBBO both use MBP_1_MSG rtype + MBP1Msg layout.
                # Broadcast so each subscribed channel receives the record.
                # Single replay-state update — same record content.
                rec = DBN.read_mbp1_msg(decoder, hd)
                _record_replay!(c, rec)
                ch_mbp1 !== nothing && put!(ch_mbp1, rec)
                ch_tbbo !== nothing && put!(ch_tbbo, rec)
                continue
            elseif ch_mbp10 !== nothing && rt == DBN.RType.MBP_10_MSG
                _put_data!(c, ch_mbp10, DBN.read_mbp10_msg(decoder, hd))
                continue
            elseif ch_ohlcv_1s !== nothing && rt == DBN.RType.OHLCV_1S_MSG
                _put_data!(c, ch_ohlcv_1s, DBN.read_ohlcv_msg(decoder, hd))
                continue
            elseif ch_ohlcv_1m !== nothing && rt == DBN.RType.OHLCV_1M_MSG
                _put_data!(c, ch_ohlcv_1m, DBN.read_ohlcv_msg(decoder, hd))
                continue
            elseif ch_ohlcv_1h !== nothing && rt == DBN.RType.OHLCV_1H_MSG
                _put_data!(c, ch_ohlcv_1h, DBN.read_ohlcv_msg(decoder, hd))
                continue
            elseif ch_ohlcv_1d !== nothing && rt == DBN.RType.OHLCV_1D_MSG
                _put_data!(c, ch_ohlcv_1d, DBN.read_ohlcv_msg(decoder, hd))
                continue
            elseif ch_status !== nothing && rt == DBN.RType.STATUS_MSG
                _put_data!(c, ch_status, DBN.read_status_msg(decoder, hd))
                continue
            elseif ch_def !== nothing && rt == DBN.RType.INSTRUMENT_DEF_MSG
                _put_data!(c, ch_def, DBN.read_instrument_def_msg(decoder, hd))
                continue
            elseif ch_imbal !== nothing && rt == DBN.RType.IMBALANCE_MSG
                _put_data!(c, ch_imbal, DBN.read_imbalance_msg(decoder, hd))
                continue
            elseif ch_stat !== nothing && rt == DBN.RType.STAT_MSG
                _put_data!(c, ch_stat, DBN.read_stat_msg(decoder, hd))
                continue
            elseif ch_cmbp1 !== nothing && rt == DBN.RType.CMBP_1_MSG
                _put_data!(c, ch_cmbp1, DBN.read_cmbp1_msg(decoder, hd))
                continue
            elseif ch_cbbo1s !== nothing && rt == DBN.RType.CBBO_1S_MSG
                _put_data!(c, ch_cbbo1s, DBN.read_cbbo1s_msg(decoder, hd))
                continue
            elseif ch_cbbo1m !== nothing && rt == DBN.RType.CBBO_1M_MSG
                _put_data!(c, ch_cbbo1m, DBN.read_cbbo1m_msg(decoder, hd))
                continue
            elseif ch_tcbbo !== nothing && rt == DBN.RType.TCBBO_MSG
                _put_data!(c, ch_tcbbo, DBN.read_tcbbo_msg(decoder, hd))
                continue
            elseif ch_bbo1s !== nothing && rt == DBN.RType.BBO_1S_MSG
                _put_data!(c, ch_bbo1s, DBN.read_bbo1s_msg(decoder, hd))
                continue
            elseif ch_bbo1m !== nothing && rt == DBN.RType.BBO_1M_MSG
                _put_data!(c, ch_bbo1m, DBN.read_bbo1m_msg(decoder, hd))
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
        # EOFError mid-body (e.g. read_trade_msg raises after the header
        # parsed cleanly because the peer closed during the body read) is
        # treated as a clean end-of-stream — no "crashed" log. Other
        # exceptions still surface.
        if !c.closed && !(e isa EOFError)
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

# Resolve a SCHEMA-specific typed channel, type-stable.
# Returns `Channel{T}` if the schema was subscribed (asserting T matches),
# else `nothing`. Caller passes both the schema and its concrete record
# type so the return narrows to `Channel{T}` for monomorphic put!.
@inline function _channel_for(c::Live, schema::Schema.T, ::Type{T}) where {T}
    ch = get(c.typed_data_channels, schema, nothing)
    return ch === nothing ? nothing : ch::Channel{T}
end

# Update per-instrument replay timestamps on the Live client. The supervisor
# (added in a later commit) reads these to compute the resubscribe `start=`
# value after a reconnect. `hasproperty` checks fold against the concrete
# record type at each typed-reader callsite — zero overhead for records that
# lack ts_recv (e.g. TradeMsg has only ts_event on .hd).
@inline function _record_replay!(c::Live, rec)
    if hasproperty(rec, :hd) && hasproperty(rec.hd, :instrument_id)
        iid = rec.hd.instrument_id
        if hasproperty(rec.hd, :ts_event)
            c.last_ts_event_by_id[iid] = rec.hd.ts_event
        end
        if hasproperty(rec, :ts_recv)
            c.last_ts_recv_by_id[iid] = rec.ts_recv
        end
    end
    return nothing
end

# Typed-mode helper: update replay state and put the record on its channel.
# Inlined so the put! stays monomorphic on the concrete channel eltype.
@inline function _put_data!(c::Live, ch::Channel, rec)
    _record_replay!(c, rec)
    put!(ch, rec)
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
                # Mid-record EOFError can happen on POSIX TCP loopback when the
                # peer closes after a finite payload and the kernel presents a
                # short read followed by EOF before all bytes arrive in
                # userspace. Treat it as clean end-of-stream so the consumer
                # gets whatever fully-decoded records did arrive instead of an
                # exception that closes the channel mid-drain. The real
                # Databento gateway streams continuously without a
                # finite-payload-then-close pattern, so this branch is
                # functionally only hit by tests / replay benches.
                if c.closed || e isa EOFError
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
            # Track per-instrument replay state on the Live client so the
            # reconnect supervisor sees timestamps from records that haven't
            # been consumed yet.
            _record_replay!(c, rec)
            put!(c.channel, rec)
            # ErrorMsg is informational on this stream (e.g. SkippedRecords-
            # AfterSlowReading is a code-7 ErrorMsg signalling SKIP behavior, not a
            # fatal error). Forward it through the channel and let the consumer
            # decide; only EOF on the socket terminates the reader loop.
        end
    catch e
        # Suppress the "crashed" log on mid-stream EOF (same rationale as
        # the EOFError handling inside the inner read loop).
        if !c.closed && !(e isa EOFError)
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
