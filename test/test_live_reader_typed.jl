using Test
using DatabentoAPI
using DatabentoAPI: read_text_frame, build_text_frame, cram_response
using DatabentoBinaryEncoding
import DatabentoBinaryEncoding as DBN
using Sockets

# Build a DBN payload that contains:
#   - 1 SystemMsg (control)
#   - 2 TradeMsg (data, schema TRADES)
#   - 1 SymbolMappingMsg (control)
#   - 3 TradeMsg (data)
#   - 1 ErrorMsg (control)
# Returns bytes + counts.
function _typed_mock_payload()
    md = DBN.Metadata(
        DBN.DBN_VERSION, "TEST.MOCK", DBN.Schema.TRADES,
        Int64(0), nothing, nothing, nothing,
        DBN.SType.INSTRUMENT_ID, false,
        ["AAPL"], String[], String[],
        Tuple{String,String,Int64,Int64}[],
    )
    ts0 = Int64(1_700_000_000_000_000_000)
    recs = DBN.DBNRecord[]

    sys_text = "session ready"
    sys_code = "0"
    sys_body = length(sys_text) + 1 + length(sys_code) + 1
    sys_units = UInt8(((16 + sys_body + 3) ÷ 4))
    push!(recs, DBN.SystemMsg(
        DBN.RecordHeader(sys_units, DBN.RType.SYSTEM_MSG,
                         UInt16(0), UInt32(0), ts0),
        sys_text, sys_code))

    for i in 1:2
        hd = DBN.RecordHeader(UInt8(0), DBN.RType.MBP_0_MSG,
                              UInt16(1), UInt32(100 + i), ts0 + i)
        push!(recs, DBN.TradeMsg(hd, Int64(150_000_000_000 + i * 1_000_000),
                                 UInt32(100), DBN.Action.TRADE, DBN.Side.ASK,
                                 UInt8(0), UInt8(1), ts0 + i, Int32(0), UInt32(i)))
    end

    push!(recs, DBN.SymbolMappingMsg(
        DBN.RecordHeader(UInt8(44), DBN.RType.SYMBOL_MAPPING_MSG,
                         UInt16(1), UInt32(100), ts0 + 3),
        DBN.SType.RAW_SYMBOL, "AAPL",
        DBN.SType.INSTRUMENT_ID, "100",
        ts0 + 3, ts0 + 10_000_000_000_000))

    for i in 3:5
        hd = DBN.RecordHeader(UInt8(0), DBN.RType.MBP_0_MSG,
                              UInt16(1), UInt32(100 + i), ts0 + i)
        push!(recs, DBN.TradeMsg(hd, Int64(150_000_000_000 + i * 1_000_000),
                                 UInt32(100), DBN.Action.TRADE, DBN.Side.ASK,
                                 UInt8(0), UInt8(1), ts0 + i, Int32(0), UInt32(i)))
    end

    err_text = "subscription rejected (test)"
    err_total = 16 + length(err_text) + 1
    err_units = UInt8(((err_total + 3) ÷ 4))
    push!(recs, DBN.ErrorMsg(
        DBN.RecordHeader(err_units, DBN.RType.ERROR_MSG,
                         UInt16(0), UInt32(0), ts0 + 10),
        err_text))

    tmp, io = mktemp(); close(io)
    try
        DBN.write_dbn(tmp, md, recs)
        return read(tmp), (n_trades = 5, n_system = 1, n_smap = 1, n_error = 1)
    finally
        rm(tmp; force = true)
    end
end

const _TEST_API_KEY_TYPED = "db-1234567890abcdef12345"
const _TEST_CHALLENGE_TYPED = "abcdef0123456789"

function _spawn_typed_mock(bytes::Vector{UInt8})
    server = Sockets.listen(Sockets.IPv4("127.0.0.1"), 0)
    port = Sockets.getsockname(server)[2]
    accept_task = @async begin
        try
            sock = Sockets.accept(server)
            try
                write(sock, build_text_frame(lsg_version = "0.9.0"))
                write(sock, build_text_frame(cram = _TEST_CHALLENGE_TYPED))
                flush(sock)
                read_text_frame(sock)   # auth
                write(sock, build_text_frame(success = "1", session_id = "typed-test"))
                flush(sock)
                # Read text frames (each subscribe + final start_session) until
                # we see "start_session" — works for both single- and
                # multi-schema tests without coupling the mock to the
                # subscription count.
                while true
                    frame = read_text_frame(sock)
                    haskey(frame, "start_session") && break
                end
                write(sock, bytes); flush(sock)
                # Close immediately — see bench_live_reader for why we don't sleep.
            finally
                isopen(sock) && Sockets.close(sock)
            end
        finally
            isopen(server) && Sockets.close(server)
        end
    end
    return (; port = Int(port), accept_task)
end

@testset "typed live reader — mock round trip" begin
    bytes, counts = _typed_mock_payload()
    mock = _spawn_typed_mock(bytes)

    client = DatabentoAPI.Live(_TEST_API_KEY_TYPED;
        dataset = "TEST.MOCK",
        gateway = "127.0.0.1", port = mock.port,
        channel_size = 64,
        typed = true,
        reconnect_policy = :none)
    DatabentoAPI.connect!(client)
    ch = DatabentoAPI.subscribe!(client;
        schema = Schema.TRADES, symbols = ["AAPL"],
        stype_in = SType.RAW_SYMBOL)
    @test ch isa Channel{DBN.TradeMsg}
    DatabentoAPI.start!(client)

    # Drain both channels concurrently — the reader will close them when
    # the mock socket closes.
    trades = DBN.TradeMsg[]
    controls = Any[]
    consumer_data = @async begin
        try
            while true
                push!(trades, take!(ch))
            end
        catch e
            e isa InvalidStateException || rethrow()
        end
    end
    consumer_ctrl = @async begin
        try
            while true
                push!(controls, take!(DatabentoAPI.control_channel(client)))
            end
        catch e
            e isa InvalidStateException || rethrow()
        end
    end

    try; wait(mock.accept_task); catch; end
    try; wait(consumer_data); catch; end
    try; wait(consumer_ctrl); catch; end

    @test length(trades) == counts.n_trades
    @test all(t -> t isa DBN.TradeMsg, trades)
    # Sequence numbers should reflect insertion order
    @test [t.sequence for t in trades] == UInt32[1, 2, 3, 4, 5]

    @test length(controls) == counts.n_system + counts.n_smap + counts.n_error
    @test any(c -> c isa DBN.SystemMsg, controls)
    @test any(c -> c isa DBN.SymbolMappingMsg, controls)
    @test any(c -> c isa DBN.ErrorMsg, controls)

    close(client)
end

@testset "typed live reader — multi-schema same Live" begin
    # Smaller payload: 2 TRADES + 2 MBP_1 records.
    md = DBN.Metadata(
        DBN.DBN_VERSION, "TEST.MOCK", DBN.Schema.MIX,
        Int64(0), nothing, nothing, nothing,
        DBN.SType.INSTRUMENT_ID, false,
        ["AAPL"], String[], String[],
        Tuple{String,String,Int64,Int64}[],
    )
    ts0 = Int64(1_700_000_000_000_000_000)
    recs = DBN.DBNRecord[]
    for i in 1:2
        hd = DBN.RecordHeader(UInt8(0), DBN.RType.MBP_0_MSG,
                              UInt16(1), UInt32(100 + i), ts0 + i)
        push!(recs, DBN.TradeMsg(hd, Int64(150_000_000_000), UInt32(100),
                                 DBN.Action.TRADE, DBN.Side.ASK,
                                 UInt8(0), UInt8(1), ts0 + i, Int32(0), UInt32(i)))
    end
    for i in 1:2
        hd = DBN.RecordHeader(UInt8(0), DBN.RType.MBP_1_MSG,
                              UInt16(1), UInt32(200 + i), ts0 + 10 + i)
        push!(recs, DBN.MBP1Msg(hd,
            Int64(100_000_000_000), UInt32(50),
            DBN.Action.MODIFY, DBN.Side.BID, UInt8(0), UInt8(1),
            ts0 + 10 + i, Int32(0), UInt32(i),
            DBN.BidAskPair(Int64(99_000), Int64(101_000),
                           UInt32(10), UInt32(20),
                           UInt32(1), UInt32(1))))
    end
    tmp, io = mktemp(); close(io)
    DBN.write_dbn(tmp, md, recs)
    bytes = read(tmp)
    rm(tmp; force = true)

    mock = _spawn_typed_mock(bytes)
    client = DatabentoAPI.Live(_TEST_API_KEY_TYPED;
        dataset = "TEST.MOCK",
        gateway = "127.0.0.1", port = mock.port,
        channel_size = 32,
        typed = true,
        reconnect_policy = :none)
    DatabentoAPI.connect!(client)
    ch_trades = DatabentoAPI.subscribe!(client;
        schema = Schema.TRADES, symbols = ["AAPL"], stype_in = SType.RAW_SYMBOL)
    ch_mbp1 = DatabentoAPI.subscribe!(client;
        schema = Schema.MBP_1, symbols = ["AAPL"], stype_in = SType.RAW_SYMBOL)
    @test ch_trades isa Channel{DBN.TradeMsg}
    @test ch_mbp1   isa Channel{DBN.MBP1Msg}
    DatabentoAPI.start!(client)

    trades = DBN.TradeMsg[]
    mbps = DBN.MBP1Msg[]
    t1 = @async begin
        try; while true; push!(trades, take!(ch_trades)); end
        catch e; e isa InvalidStateException || rethrow(); end
    end
    t2 = @async begin
        try; while true; push!(mbps, take!(ch_mbp1)); end
        catch e; e isa InvalidStateException || rethrow(); end
    end
    try; wait(mock.accept_task); catch; end
    try; wait(t1); catch; end
    try; wait(t2); catch; end

    @test length(trades) == 2
    @test length(mbps)   == 2
    @test all(t -> t isa DBN.TradeMsg, trades)
    @test all(m -> m isa DBN.MBP1Msg,  mbps)

    close(client)
end

@testset "typed live reader — distinct channels for OHLCV intervals" begin
    # Regression: OHLCV_1S and OHLCV_1M both produce OHLCVMsg records but
    # different rtypes (OHLCV_1S_MSG vs OHLCV_1M_MSG). Subscribing to both
    # must give two distinct channels, each receiving only its own rtype's
    # records. Pre-fix the reader looked up by Channel{OHLCVMsg} which
    # returned the dict's arbitrary-first match, sending all OHLCV records
    # to one channel and starving the other.

    # Build a stream with 2 OHLCV_1S records + 2 OHLCV_1M records.
    md = DBN.Metadata(
        DBN.DBN_VERSION, "TEST.MOCK", DBN.Schema.MIX,
        Int64(0), nothing, nothing, nothing,
        DBN.SType.INSTRUMENT_ID, false,
        ["AAPL"], String[], String[],
        Tuple{String,String,Int64,Int64}[],
    )
    ts0 = Int64(1_700_000_000_000_000_000)
    recs = DBN.DBNRecord[]
    for (rt_enum, i) in ((DBN.RType.OHLCV_1S_MSG, 1),
                         (DBN.RType.OHLCV_1S_MSG, 2),
                         (DBN.RType.OHLCV_1M_MSG, 3),
                         (DBN.RType.OHLCV_1M_MSG, 4))
        hd = DBN.RecordHeader(UInt8(0), rt_enum, UInt16(1), UInt32(100 + i), ts0 + i)
        push!(recs, DBN.OHLCVMsg(hd, Int64(100), Int64(110), Int64(95), Int64(105), UInt64(1000 + i)))
    end
    tmp, io = mktemp(); close(io)
    DBN.write_dbn(tmp, md, recs)
    bytes = read(tmp)
    rm(tmp; force = true)

    mock = _spawn_typed_mock(bytes)
    client = DatabentoAPI.Live(_TEST_API_KEY_TYPED;
        dataset = "TEST.MOCK",
        gateway = "127.0.0.1", port = mock.port,
        channel_size = 32,
        typed = true,
        reconnect_policy = :none)
    DatabentoAPI.connect!(client)
    ch_1s = DatabentoAPI.subscribe!(client;
        schema = Schema.OHLCV_1S, symbols = ["AAPL"], stype_in = SType.RAW_SYMBOL)
    ch_1m = DatabentoAPI.subscribe!(client;
        schema = Schema.OHLCV_1M, symbols = ["AAPL"], stype_in = SType.RAW_SYMBOL)
    @test ch_1s isa Channel{DBN.OHLCVMsg}
    @test ch_1m isa Channel{DBN.OHLCVMsg}
    @test ch_1s !== ch_1m   # different channel objects
    DatabentoAPI.start!(client)

    out_1s = DBN.OHLCVMsg[]; out_1m = DBN.OHLCVMsg[]
    t1 = @async begin
        try; while true; push!(out_1s, take!(ch_1s)); end
        catch e; e isa InvalidStateException || rethrow(); end
    end
    t2 = @async begin
        try; while true; push!(out_1m, take!(ch_1m)); end
        catch e; e isa InvalidStateException || rethrow(); end
    end
    try; wait(mock.accept_task); catch; end
    try; wait(t1); catch; end
    try; wait(t2); catch; end

    @test length(out_1s) == 2
    @test length(out_1m) == 2
    @test all(r -> r.hd.rtype == DBN.RType.OHLCV_1S_MSG, out_1s)
    @test all(r -> r.hd.rtype == DBN.RType.OHLCV_1M_MSG, out_1m)

    close(client)
end

@testset "typed live reader — MBP_1 + TBBO broadcast (shared rtype)" begin
    # Schema.MBP_1 and Schema.TBBO both produce MBP_1_MSG-tagged MBP1Msg
    # records. Subscribing to both on one Live: the reader can't tell
    # the records apart at the wire level, so it broadcasts each MBP_1_MSG
    # record to BOTH channels. (Better to deliver duplicates than drop
    # records from one of the subscriptions.)
    md = DBN.Metadata(
        DBN.DBN_VERSION, "TEST.MOCK", DBN.Schema.MIX,
        Int64(0), nothing, nothing, nothing,
        DBN.SType.INSTRUMENT_ID, false,
        ["AAPL"], String[], String[],
        Tuple{String,String,Int64,Int64}[],
    )
    ts0 = Int64(1_700_000_000_000_000_000)
    recs = DBN.DBNRecord[]
    for i in 1:3
        hd = DBN.RecordHeader(UInt8(0), DBN.RType.MBP_1_MSG, UInt16(1),
                              UInt32(100 + i), ts0 + i)
        push!(recs, DBN.MBP1Msg(hd, Int64(100), UInt32(50),
            DBN.Action.MODIFY, DBN.Side.BID, UInt8(0), UInt8(1),
            ts0 + i, Int32(0), UInt32(i),
            DBN.BidAskPair(Int64(99), Int64(101), UInt32(10), UInt32(20),
                           UInt32(1), UInt32(1))))
    end
    tmp, io = mktemp(); close(io)
    DBN.write_dbn(tmp, md, recs)
    bytes = read(tmp)
    rm(tmp; force = true)

    mock = _spawn_typed_mock(bytes)
    client = DatabentoAPI.Live(_TEST_API_KEY_TYPED;
        dataset = "TEST.MOCK",
        gateway = "127.0.0.1", port = mock.port,
        channel_size = 32,
        typed = true,
        reconnect_policy = :none)
    DatabentoAPI.connect!(client)
    ch_mbp1 = DatabentoAPI.subscribe!(client;
        schema = Schema.MBP_1, symbols = ["AAPL"], stype_in = SType.RAW_SYMBOL)
    ch_tbbo = DatabentoAPI.subscribe!(client;
        schema = Schema.TBBO,  symbols = ["AAPL"], stype_in = SType.RAW_SYMBOL)
    @test ch_mbp1 !== ch_tbbo
    DatabentoAPI.start!(client)

    a = DBN.MBP1Msg[]; b = DBN.MBP1Msg[]
    t1 = @async begin
        try; while true; push!(a, take!(ch_mbp1)); end
        catch e; e isa InvalidStateException || rethrow(); end
    end
    t2 = @async begin
        try; while true; push!(b, take!(ch_tbbo)); end
        catch e; e isa InvalidStateException || rethrow(); end
    end
    try; wait(mock.accept_task); catch; end
    try; wait(t1); catch; end
    try; wait(t2); catch; end

    @test length(a) == 3
    @test length(b) == 3   # broadcast — both channels got the same records

    close(client)
end

@testset "typed mode rejects untyped-only schemas" begin
    client = DatabentoAPI.Live(_TEST_API_KEY_TYPED;
        dataset = "TEST.MOCK",
        gateway = "127.0.0.1", port = 1,   # no real connection needed for the check
        typed = true,
        reconnect_policy = :none)
    # Mark connected so subscribe! validation runs (we still won't make
    # a real socket call because subscribe! checks the schema first).
    client.connected = true
    @test_throws ArgumentError DatabentoAPI.subscribe!(client;
        schema = Schema.MIX, symbols = ["AAPL"], stype_in = SType.RAW_SYMBOL)
    client.closed = true   # avoid finaliser errors
end
