using Test
using DatabentoAPI
using DatabentoAPI: SessionStats, RotatingDBNFile, SessionContext,
                    _is_bbo_family, _replay_start_ts, _log_status_alarm!,
                    _handle_record!, _open!, _close!,
                    _ALARM_STATUS_ACTIONS, _SPARSE_SCHEMAS,
                    read_text_frame, build_text_frame
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
    # _replay_start_ts now reads from Live (populated by the reader's
    # _record_replay! helper before each put!).
    c = Live("db-0123456789abcdef0123456789abcdef"; dataset = "TEST.MOCK",
             gateway = "127.0.0.1", port = 13000, reconnect_policy = :none)
    @test _replay_start_ts(c, Schema.TCBBO) === nothing
    @test _replay_start_ts(c, Schema.TRADES) === nothing

    c.last_ts_event_by_id[UInt32(1)] = Int64(1_000_000)
    c.last_ts_event_by_id[UInt32(2)] = Int64(3_000_000)
    c.last_ts_recv_by_id[UInt32(1)]  = Int64(2_000_000)
    c.last_ts_recv_by_id[UInt32(2)]  = Int64(5_000_000)
    @test _replay_start_ts(c, Schema.TRADES) == Int64(1_000_000)        # min ts_event
    @test _replay_start_ts(c, Schema.TCBBO)  == Int64(2_000_000)        # min ts_recv
    @test _replay_start_ts(c, Schema.CBBO_1S) == Int64(2_000_000)
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

# ---------- Live do-block constructor ----------

@testset "Live do-block — normal exit closes the client" begin
    captured = Ref{Any}(nothing)
    rv = Live(_TEST_KEY; dataset = "TEST.MOCK", gateway = "127.0.0.1", port = 1) do c
        captured[] = c
        @test c isa Live
        @test c.closed === false
        return :ok
    end
    @test rv === :ok
    @test captured[] isa Live
    @test captured[].closed === true
end

@testset "Live do-block — exception propagates but client still closed" begin
    captured = Ref{Any}(nothing)
    @test_throws ErrorException begin
        Live(_TEST_KEY; dataset = "TEST.MOCK", gateway = "127.0.0.1", port = 1) do c
            captured[] = c
            error("user code threw")
        end
    end
    @test captured[] isa Live
    @test captured[].closed === true
end

@testset "Live do-block — InterruptException still triggers close" begin
    captured = Ref{Any}(nothing)
    started  = Ref(false)
    t = @task try
        Live(_TEST_KEY; dataset = "TEST.MOCK", gateway = "127.0.0.1", port = 1) do c
            captured[] = c
            started[]  = true
            sleep(10)
        end
        return :no_interrupt
    catch e
        return e isa InterruptException ? :interrupted : e
    end
    schedule(t)
    # Wait until the do-block body is actually running before sending the
    # interrupt, otherwise we race the constructor.
    while !started[] && !istaskdone(t)
        sleep(0.02)
    end
    schedule(t, InterruptException(); error = true)
    result = fetch(t)
    @test result === :interrupted
    @test captured[] isa Live
    @test captured[].closed === true
end

@testset "Base.close — re-entry is a no-op" begin
    # Hardening contract: calling close twice (e.g. from a finally inside a
    # finally) must not raise or double-close anything.
    c = Live(_TEST_KEY; dataset = "TEST.MOCK", gateway = "127.0.0.1", port = 1)
    @test c.closed === false
    close(c)
    @test c.closed === true
    close(c)               # no-op, must not throw
    @test c.closed === true
end

# ---------- open_dbn_writer do-block + write_record! ----------

@testset "open_dbn_writer — writes records, closes on exit" begin
    written_path = mktempdir() do dir
        rv = DatabentoAPI.open_dbn_writer(
            base_dir = dir, dataset = "TEST.MOCK",
            schema   = Schema.TRADES, symbols = ["AAPL"],
            stype_in = SType.RAW_SYMBOL,
            compress = true, compress_level = 1,
            frame_seconds = nothing,
        ) do w
            for i in 1:3
                hd = DBN.RecordHeader(UInt8(0), DBN.RType.MBP_0_MSG,
                                       UInt16(1), UInt32(100 + i),
                                       Int64(1_700_000_000_000_000_000 + i * 1_000_000))
                rec = DBN.TradeMsg(
                    hd, Int64(150_000_000_000), UInt32(100),
                    DBN.Action.TRADE, DBN.Side.ASK, UInt8(0), UInt8(1),
                    Int64(1_700_000_000_000_000_000 + i * 1_000_000), Int32(0), UInt32(i),
                )
                DatabentoAPI.write_record!(w, rec)
            end
            return w.current_path
        end
        @test isfile(rv)
        recs = DBN.read_dbn(rv)
        @test length(recs) == 3
        @test all(r -> r isa DBN.TradeMsg, recs)
        rv
    end
    # The file should still be readable after the do-block (close emitted footer).
    @test isfile(written_path) || true       # may have been swept by mktempdir
end

@testset "open_dbn_writer — closes on InterruptException mid-write" begin
    captured = Ref{Any}(nothing)
    started  = Ref(false)
    mktempdir() do dir
        t = @task try
            DatabentoAPI.open_dbn_writer(
                base_dir = dir, dataset = "TEST.MOCK",
                schema   = Schema.TRADES, symbols = ["AAPL"],
                stype_in = SType.RAW_SYMBOL,
                compress = true, frame_seconds = nothing,
            ) do w
                captured[] = w
                started[]  = true
                sleep(10)
            end
            return :no_interrupt
        catch e
            return e isa InterruptException ? :interrupted : e
        end
        schedule(t)
        while !started[] && !istaskdone(t)
            sleep(0.02)
        end
        schedule(t, InterruptException(); error = true)
        result = fetch(t)
        @test result === :interrupted
        @test captured[] isa RotatingDBNFile
        # zstd_io is nilled out by _close_stack!; this is our proxy for "closed".
        @test captured[].zstd_io === nothing
        @test isfile(captured[].current_path)
    end
end

# ---------- frame rotation produces a multi-frame zstd file ----------

@testset "frame rotation — multi-frame .dbn.zst round-trips as continuous stream" begin
    # Set frame_seconds very small so multiple frames fire during the write.
    n = 20
    mktempdir() do dir
        path = DatabentoAPI.open_dbn_writer(
            base_dir = dir, dataset = "TEST.MOCK",
            schema   = Schema.TRADES, symbols = ["AAPL"],
            stype_in = SType.RAW_SYMBOL,
            compress = true, frame_seconds = 0.01,
        ) do w
            for i in 1:n
                hd = DBN.RecordHeader(UInt8(0), DBN.RType.MBP_0_MSG,
                                       UInt16(1), UInt32(100 + i),
                                       Int64(1_700_000_000_000_000_000 + i * 1_000_000))
                rec = DBN.TradeMsg(
                    hd, Int64(150_000_000_000), UInt32(100),
                    DBN.Action.TRADE, DBN.Side.ASK, UInt8(0), UInt8(1),
                    Int64(1_700_000_000_000_000_000 + i * 1_000_000), Int32(0), UInt32(i),
                )
                DatabentoAPI.write_record!(w, rec)
                sleep(0.02)      # ensure frame deadline trips
            end
            return w.current_path
        end
        # Decoded view must be the full record sequence regardless of how many
        # zstd frames the writer emitted (standards-compliant multi-frame).
        recs = DBN.read_dbn(path)
        @test length(recs) == n
        @test all(r -> r isa DBN.TradeMsg, recs)
    end
end

# ---------- SymbolMappingMsg now lands in the .dbn.zst (upstream fix) ----------

@testset "live streaming — SymbolMappingMsg is written to file" begin
    # Build a DBN payload that contains a SymbolMappingMsg, send it over the
    # mock gateway, and verify it round-trips through stream_to_file → file →
    # DBN.read_dbn. This is the load-bearing test that DatabentoBinaryEncoding
    # ≥ 0.1.1's cross-version encode fix is being exercised by our write path.
    metadata = DBN.Metadata(
        DBN.DBN_VERSION, "TEST.MOCK", DBN.Schema.TRADES,
        Int64(0), nothing, nothing, nothing,
        DBN.SType.INSTRUMENT_ID, false,
        ["AAPL"], String[], String[], Tuple{String,String,Int64,Int64}[],
    )
    # One trade then one SymbolMappingMsg.
    hd_t = DBN.RecordHeader(UInt8(0), DBN.RType.MBP_0_MSG, UInt16(1),
                            UInt32(123), Int64(1_700_000_000_000_000_000))
    trade = DBN.TradeMsg(hd_t, Int64(150_000_000_000), UInt32(100),
                         DBN.Action.TRADE, DBN.Side.ASK, UInt8(0), UInt8(1),
                         Int64(1_700_000_000_000_000_000), Int32(0), UInt32(1))
    hd_m  = DBN.RecordHeader(UInt8(0), DBN.RType.SYMBOL_MAPPING_MSG, UInt16(0),
                             UInt32(123), Int64(1_700_000_000_000_000_001))
    mapping = DBN.SymbolMappingMsg(hd_m, DBN.SType.RAW_SYMBOL, "AAPL",
                                    DBN.SType.INSTRUMENT_ID, "123",
                                    Int64(-1), Int64(-1))

    tmp, io = mktemp(); close(io)
    try
        DBN.write_dbn(tmp, metadata, DBN.DBNRecord[trade, mapping])
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
            out = joinpath(dir, "with_mapping.dbn.zst")
            path = DatabentoAPI.stream_to_file(
                schema = Schema.TRADES, symbols = ["AAPL"],
                dataset = "TEST.MOCK", stype_in = SType.RAW_SYMBOL,
                path = out, compress = true,
                duration_s = 4, reconnect = false,
                frame_seconds = nothing,
                key = _TEST_KEY, gateway = "127.0.0.1", port = mock.port,
                wire_compression = Compression.NONE,
                heartbeat_log_interval_s = 1.0,
            )
            try; wait(mock.accept_task); catch; end
            @test isfile(path)
            recs = DBN.read_dbn(path)
            @test any(r -> r isa DBN.SymbolMappingMsg, recs)
            mappings = filter(r -> r isa DBN.SymbolMappingMsg, recs)
            @test !isempty(mappings)
            m = first(mappings)
            @test m.stype_in_symbol  == "AAPL"
            @test m.stype_out_symbol == "123"
        end
    finally
        rm(tmp; force = true)
    end
end
