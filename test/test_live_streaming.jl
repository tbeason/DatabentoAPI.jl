using Test
using DatabentoAPI
using DatabentoAPI: SessionStats, RotatingDBNFile, SessionContext,
                    _is_bbo_family, _replay_start_ts, _log_status_alarm!,
                    _handle_record!, _open!, _close!,
                    _ALARM_STATUS_ACTIONS, _SPARSE_SCHEMAS
using DatabentoBinaryEncoding
import DatabentoBinaryEncoding as DBN
using Sockets
using CodecZstd
using TranscodingStreams

# ---------- helpers shared with other live tests ----------

function _make_dbn_bytes_with_trades(n::Int; dataset = "TEST.MOCK")
    metadata = DBN.Metadata(
        DBN.DBN_VERSION, dataset, DBN.Schema.TRADES,
        Int64(0), nothing, nothing, nothing,
        DBN.SType.INSTRUMENT_ID, false,
        ["AAPL"], String[], String[],
        Tuple{String,String,Int64,Int64}[],
    )
    records = DBN.DBNRecord[]
    for i in 1:n
        hd = DBN.RecordHeader(UInt8(0), DBN.RType.MBP_0_MSG,
                              UInt16(1), UInt32(100 + i),
                              Int64(1_700_000_000_000_000_000 + i * 1_000_000))
        push!(records, DBN.TradeMsg(
            hd, Int64(150_000_000_000 + i * 1_000_000), UInt32(100),
            DBN.Action.TRADE, DBN.Side.ASK, UInt8(0), UInt8(1),
            Int64(1_700_000_000_000_000_000 + i * 1_000_000), Int32(0), UInt32(i),
        ))
    end
    tmp, io = mktemp(); close(io)
    try
        DBN.write_dbn(tmp, metadata, records)
        return read(tmp), n
    finally
        rm(tmp; force = true)
    end
end

# Interleaved trades + status payload for multi-schema-on-one-Live testing.
function _make_dbn_bytes_with_trades_and_status(; n_trades::Int = 4, n_status::Int = 2,
                                                dataset = "TEST.MOCK")
    metadata = DBN.Metadata(
        DBN.DBN_VERSION, dataset, DBN.Schema.TRADES,
        Int64(0), nothing, nothing, nothing,
        DBN.SType.INSTRUMENT_ID, false,
        ["AAPL"], String[], String[],
        Tuple{String,String,Int64,Int64}[],
    )
    records = DBN.DBNRecord[]
    ts_base = Int64(1_700_000_000_000_000_000)
    # Interleave: trade, status, trade, status, trade, trade, ...
    total = n_trades + n_status
    ti = 0; si = 0
    for k in 1:total
        emit_status = si < n_status && (k % 2 == 0)
        if emit_status
            si += 1
            iid = UInt32(7000 + si)
            hd = DBN.RecordHeader(UInt8(0), DBN.RType.STATUS_MSG,
                                  UInt16(1), iid,
                                  ts_base + Int64(k) * Int64(1_000_000))
            push!(records, DBN.StatusMsg(
                hd, UInt64(ts_base + Int64(k) * Int64(1_000_000)),
                UInt16(2), UInt16(0), UInt16(0),  # action=open, reason=0, evt=0
                UInt8('Y'), UInt8('N'), UInt8(0),
            ))
        else
            ti += 1
            iid = UInt32(100 + ti)
            hd = DBN.RecordHeader(UInt8(0), DBN.RType.MBP_0_MSG,
                                  UInt16(1), iid,
                                  ts_base + Int64(k) * Int64(1_000_000))
            push!(records, DBN.TradeMsg(
                hd, Int64(150_000_000_000 + ti * 1_000_000), UInt32(100),
                DBN.Action.TRADE, DBN.Side.ASK, UInt8(0), UInt8(1),
                ts_base + Int64(k) * Int64(1_000_000), Int32(0), UInt32(ti),
            ))
        end
    end
    tmp, io = mktemp(); close(io)
    try
        DBN.write_dbn(tmp, metadata, records)
        return read(tmp), n_trades, n_status
    finally
        rm(tmp; force = true)
    end
end

const _TEST_KEY = "db-1234567890abcdef12345"

# ---------- unit tests ----------

@testset "live streaming — _is_bbo_family" begin
    @test _is_bbo_family(Schema.BBO_1S)
    @test _is_bbo_family(Schema.BBO_1M)
    @test _is_bbo_family(Schema.CBBO_1S)
    @test _is_bbo_family(Schema.CBBO_1M)
    @test _is_bbo_family(Schema.TBBO)
    @test _is_bbo_family(Schema.TCBBO)
    @test !_is_bbo_family(Schema.TRADES)
    @test !_is_bbo_family(Schema.MBP_1)
    @test !_is_bbo_family(Schema.STATUS)
    @test !_is_bbo_family(Schema.OHLCV_1S)
    @test !_is_bbo_family(Schema.IMBALANCE)
    @test !_is_bbo_family(Schema.CMBP_1)
end

@testset "live streaming — _replay_start_ts" begin
    s = SessionStats(schema = Schema.TCBBO)
    @test _replay_start_ts(s, Schema.TCBBO) === nothing
    @test _replay_start_ts(s, Schema.TRADES) === nothing

    s.last_ts_event_by_id[UInt32(1)] = Int64(1_000_000)
    s.last_ts_event_by_id[UInt32(2)] = Int64(3_000_000)
    s.last_ts_recv_by_id[UInt32(1)]  = Int64(2_000_000)
    s.last_ts_recv_by_id[UInt32(2)]  = Int64(5_000_000)
    @test _replay_start_ts(s, Schema.TRADES) == Int64(1_000_000)        # min ts_event
    @test _replay_start_ts(s, Schema.TCBBO)  == Int64(2_000_000)        # min ts_recv
    @test _replay_start_ts(s, Schema.CBBO_1S) == Int64(2_000_000)
end

@testset "live streaming — status alarm dedup" begin
    s = SessionStats(schema = Schema.STATUS)
    iid = UInt32(42)
    function mkstatus(action, is_trading)
        hd = DBN.RecordHeader(UInt8(0), DBN.RType.STATUS_MSG,
                              UInt16(1), iid, Int64(0))
        # StatusMsg fields: hd, ts_recv, action, reason, trading_event,
        # is_trading, is_quoting, is_short_sell_restricted
        return DBN.StatusMsg(hd, UInt64(0), UInt16(action), UInt16(0), UInt16(0),
                             UInt8(is_trading), UInt8('N'), UInt8(0))
    end
    halt_y = mkstatus(8, UInt8('Y'))
    _log_status_alarm!(s, halt_y, Schema.STATUS)
    @test s.alarm_status_count == 1
    _log_status_alarm!(s, halt_y, Schema.STATUS)             # same state — no count
    @test s.alarm_status_count == 1
    open_y = mkstatus(2, UInt8('Y'))                          # leaving alarm
    _log_status_alarm!(s, open_y, Schema.STATUS)
    @test s.alarm_status_count == 2
    _log_status_alarm!(s, open_y, Schema.STATUS)             # same state again
    @test s.alarm_status_count == 2
end

# ---------- IO round-trip via mock TCP ----------

@testset "live streaming — round trip via mock gateway" begin
    bytes, n = _make_dbn_bytes_with_trades(5; dataset = "TEST.MOCK")
    handshake = function (sock)
        write(sock, build_text_frame(lsg_version = "0.9.0"))
        write(sock, build_text_frame(cram = "challenge"))
        flush(sock)
        read_text_frame(sock)                                   # auth
        write(sock, build_text_frame(success = "1", session_id = "sess-1"))
        flush(sock)
        read_text_frame(sock)                                   # subscribe
        read_text_frame(sock)                                   # start_session
        write(sock, bytes)
        flush(sock)
        sleep(0.3)
    end
    mock = spawn_mock_gateway(handshake)
    out_path = mktempdir() do dir
        out = joinpath(dir, "trades.dbn.zst")
        DatabentoAPI.stream_to_file(
            schema = Schema.TRADES, symbols = ["AAPL"],
            dataset = "TEST.MOCK", stype_in = SType.RAW_SYMBOL,
            path = out, compress = true,
            duration_s = 4, reconnect = false,
            key = _TEST_KEY, gateway = "127.0.0.1", port = mock.port,
            wire_compression = Compression.NONE,
            heartbeat_log_interval_s = 1.0,
        )
        try; wait(mock.accept_task) catch end
        @test isfile(out)
        recs = DBN.read_dbn(out)
        @test length(recs) >= n
        @test all(r -> r isa DBN.TradeMsg, recs)
        out
    end
    @test endswith(out_path, ".dbn.zst")
end

# ---------- compress_level parameter coverage ----------

@testset "live streaming — explicit compress_level override" begin
    # The default is L1 (throughput-favoured). This test exercises an explicit
    # higher level to keep the parameter path covered and confirm round-trip.
    bytes, n = _make_dbn_bytes_with_trades(5; dataset = "TEST.MOCK")
    handshake = function (sock)
        write(sock, build_text_frame(lsg_version = "0.9.0"))
        write(sock, build_text_frame(cram = "challenge"))
        flush(sock)
        read_text_frame(sock)
        write(sock, build_text_frame(success = "1", session_id = "sess-1"))
        flush(sock)
        read_text_frame(sock)
        read_text_frame(sock)
        write(sock, bytes)
        flush(sock)
        sleep(0.3)
    end
    mock = spawn_mock_gateway(handshake)
    mktempdir() do dir
        out = joinpath(dir, "trades_l9.dbn.zst")
        DatabentoAPI.stream_to_file(
            schema = Schema.TRADES, symbols = ["AAPL"],
            dataset = "TEST.MOCK", stype_in = SType.RAW_SYMBOL,
            path = out, compress = true, compress_level = 9,
            duration_s = 4, reconnect = false,
            key = _TEST_KEY, gateway = "127.0.0.1", port = mock.port,
            wire_compression = Compression.NONE,
            heartbeat_log_interval_s = 1.0,
        )
        try; wait(mock.accept_task) catch end
        @test isfile(out)
        recs = DBN.read_dbn(out)
        @test length(recs) >= n
        @test all(r -> r isa DBN.TradeMsg, recs)
    end
end

# ---------- ErrorMsg is terminal ----------

@testset "live streaming — ErrorMsg is terminal" begin
    # Build a small DBN stream containing an ErrorMsg after one trade.
    metadata = DBN.Metadata(
        DBN.DBN_VERSION, "TEST.MOCK", DBN.Schema.TRADES,
        Int64(0), nothing, nothing, nothing,
        DBN.SType.INSTRUMENT_ID, false,
        ["AAPL"], String[], String[], Tuple{String,String,Int64,Int64}[],
    )
    hd = DBN.RecordHeader(UInt8(0), DBN.RType.MBP_0_MSG, UInt16(1), UInt32(100),
                          Int64(1_700_000_000_000_000_000))
    trade = DBN.TradeMsg(hd, Int64(150_000_000_000), UInt32(100),
                         DBN.Action.TRADE, DBN.Side.ASK, UInt8(0), UInt8(1),
                         Int64(1_700_000_000_000_000_000), Int32(0), UInt32(1))
    err_text = "subscription rejected"
    err_total = 16 + length(err_text) + 1
    err_units = UInt8(((err_total + 3) ÷ 4))
    err_hd = DBN.RecordHeader(err_units, DBN.RType.ERROR_MSG, UInt16(0), UInt32(0),
                              Int64(1_700_000_000_000_000_001))
    err = DBN.ErrorMsg(err_hd, err_text)

    tmp, io = mktemp(); close(io)
    try
        DBN.write_dbn(tmp, metadata, DBN.DBNRecord[trade, err])
        bytes = read(tmp)

        handshake = function (sock)
            write(sock, build_text_frame(lsg_version = "0.9.0"))
            write(sock, build_text_frame(cram = "challenge"))
            flush(sock)
            read_text_frame(sock)
            write(sock, build_text_frame(success = "1", session_id = "sess-1"))
            flush(sock)
            read_text_frame(sock)
            read_text_frame(sock)
            write(sock, bytes)
            flush(sock)
            sleep(0.4)
        end
        mock = spawn_mock_gateway(handshake)

        mktempdir() do dir
            out = joinpath(dir, "errtest.dbn.zst")
            t0 = time()
            path = DatabentoAPI.stream_to_file(
                schema = Schema.TRADES, symbols = ["AAPL"],
                dataset = "TEST.MOCK", stype_in = SType.RAW_SYMBOL,
                path = out, compress = true,
                duration_s = 5, reconnect = true,
                key = _TEST_KEY, gateway = "127.0.0.1", port = mock.port,
                wire_compression = Compression.NONE,
                heartbeat_log_interval_s = 1.0,
            )
            elapsed = time() - t0
            try; wait(mock.accept_task) catch end
            @test elapsed < 4.5    # bailed out instead of waiting full duration
            @test isfile(path)
            recs = DBN.read_dbn(path)
            @test length(recs) >= 1
            @test recs[1] isa DBN.TradeMsg
        end
    finally
        rm(tmp; force = true)
    end
end

# ---------- multi-schema, single Live, schema-routed files ----------

@testset "live streaming — multi-schema unified Live routes to per-schema files" begin
    bytes, n_trades, n_status = _make_dbn_bytes_with_trades_and_status(
        n_trades = 4, n_status = 2, dataset = "TEST.MOCK")
    handshake = function (sock)
        write(sock, build_text_frame(lsg_version = "0.9.0"))
        write(sock, build_text_frame(cram = "challenge"))
        flush(sock)
        read_text_frame(sock)                                 # auth
        write(sock, build_text_frame(success = "1", session_id = "sess-1"))
        flush(sock)
        # Two subscribe frames (one per schema), then start_session.
        read_text_frame(sock)
        read_text_frame(sock)
        read_text_frame(sock)
        write(sock, bytes)
        flush(sock)
        sleep(0.4)
    end
    mock = spawn_mock_gateway(handshake)
    mktempdir() do dir
        paths = DatabentoAPI.stream_multi_to_files(
            schemas  = [Schema.TRADES, Schema.STATUS],
            symbols  = ["AAPL"],
            dataset  = "TEST.MOCK",
            stype_in = SType.RAW_SYMBOL,
            base_dir = dir,
            compress = true,
            duration_s = 4, reconnect = false,
            key = _TEST_KEY, gateway = "127.0.0.1", port = mock.port,
            wire_compression = Compression.NONE,
            heartbeat_log_interval_s = 1.0,
        )
        try; wait(mock.accept_task) catch end
        @test haskey(paths, Schema.TRADES)
        @test haskey(paths, Schema.STATUS)
        @test isfile(paths[Schema.TRADES])
        @test isfile(paths[Schema.STATUS])

        trade_recs = DBN.read_dbn(paths[Schema.TRADES])
        @test length(trade_recs) == n_trades
        @test all(r -> r isa DBN.TradeMsg, trade_recs)

        status_recs = DBN.read_dbn(paths[Schema.STATUS])
        @test length(status_recs) == n_status
        @test all(r -> r isa DBN.StatusMsg, status_recs)
    end
end
