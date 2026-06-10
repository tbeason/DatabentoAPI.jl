using Test
using DatabentoAPI
using Dates
using HTTP
using CodecZstd
using Logging
using TranscodingStreams
using DatabentoBinaryEncoding
import DatabentoBinaryEncoding as DBN

# Build a small in-memory DBN file (uncompressed), then zstd-compress, and serve it
# from the mock dispatcher to exercise the full get_range pipeline.

function _build_sample_dbn_zstd()
    metadata = DBN.Metadata(
        DBN.DBN_VERSION,
        "XNAS.ITCH",
        DBN.Schema.TRADES,
        Int64(0),
        nothing, nothing, nothing,
        DBN.SType.INSTRUMENT_ID,
        false,
        ["AAPL"], String[], String[],
        Tuple{String,String,Int64,Int64}[],
    )
    records = DBN.DBNRecord[]
    for i in 1:5
        hd = DBN.RecordHeader(
            UInt8(0), DBN.RType.MBP_0_MSG,
            UInt16(1), UInt32(100 + i), Int64(1_700_000_000_000_000_000 + i * 1_000_000),
        )
        push!(records, DBN.TradeMsg(
            hd,
            Int64(150_000_000_000 + i * 1_000_000),  # price
            UInt32(100),                              # size
            DBN.Action.TRADE,
            DBN.Side.ASK,
            UInt8(0),                                 # flags
            UInt8(1),                                 # depth
            Int64(1_700_000_000_000_000_000 + i * 1_000_000),
            Int32(0),
            UInt32(i),
        ))
    end
    tmp_dbn, tmp_io = mktemp()
    close(tmp_io)
    try
        DBN.write_dbn(tmp_dbn, metadata, records)
        raw = read(tmp_dbn)
        compressed = transcode(ZstdCompressor, raw)
        return compressed, length(records)
    finally
        rm(tmp_dbn; force = true)
    end
end

# Like _build_sample_dbn_zstd but for an arbitrary record vector, and with
# *correct* hd.length values (the fixture above writes 0, which typed readers
# ignore but the tolerant decode loop relies on to skip mismatched records by
# header length — real gateway streams always carry correct lengths).
function _build_dbn_zstd(records::Vector{DBN.DBNRecord})
    metadata = DBN.Metadata(
        DBN.DBN_VERSION,
        "XNAS.ITCH",
        DBN.Schema.TRADES,
        Int64(0),
        nothing, nothing, nothing,
        DBN.SType.INSTRUMENT_ID,
        false,
        ["AAPL"], String[], String[],
        Tuple{String,String,Int64,Int64}[],
    )
    tmp_dbn, tmp_io = mktemp()
    close(tmp_io)
    try
        DBN.write_dbn(tmp_dbn, metadata, records)
        raw = read(tmp_dbn)
        return raw, transcode(ZstdCompressor, raw)
    finally
        rm(tmp_dbn; force = true)
    end
end

# TradeMsg is 48 bytes on the wire → hd.length = 12 four-byte units.
function _trade_rec(i)
    hd = DBN.RecordHeader(
        UInt8(12), DBN.RType.MBP_0_MSG,
        UInt16(1), UInt32(100 + i), Int64(1_700_000_000_000_000_000 + i * 1_000_000),
    )
    DBN.TradeMsg(hd,
        Int64(150_000_000_000 + i * 1_000_000), UInt32(100),
        DBN.Action.TRADE, DBN.Side.ASK, UInt8(0), UInt8(1),
        Int64(1_700_000_000_000_000_000 + i * 1_000_000), Int32(0), UInt32(i))
end

# Trades with a gateway ErrorMsg and SystemMsg interleaved — the shape that
# made typed decode throw before #30. ErrorMsg payload is padded to
# hd.length*4-16, so hd.length must cover the message text (+ null).
function _build_mixed_dbn_zstd()
    err_hd = DBN.RecordHeader(UInt8(16), DBN.RType.ERROR_MSG, UInt16(1), UInt32(0), Int64(0))
    sys_hd = DBN.RecordHeader(UInt8(8), DBN.RType.SYSTEM_MSG, UInt16(1), UInt32(0), Int64(0))
    records = DBN.DBNRecord[
        _trade_rec(1),
        _trade_rec(2),
        DBN.ErrorMsg(err_hd, "partial symbol resolution"),
        _trade_rec(3),
        DBN.SystemMsg(sys_hd, "Heartbeat", ""),
        _trade_rec(4),
    ]
    raw, compressed = _build_dbn_zstd(records)
    return raw, compressed, 4   # 4 trades
end

@testset "historical get_range" begin
    bytes, n = _build_sample_dbn_zstd()

    captured = Ref{Any}(nothing)
    function mock(method, url, headers, body; kwargs...)
        captured[] = (; method, url, headers, kwargs)
        HTTP.Response(200; body = bytes)
    end

    c = Historical("test-key"; gateway = "https://hist.test", dispatcher = mock)
    store = get_range(c;
        dataset = "XNAS.ITCH",
        schema  = Schema.TRADES,
        symbols = ["AAPL"],
        start_dt = "2024-01-02T14:30:00",
        end_dt   = "2024-01-02T14:31:00",
        stype_in  = SType.RAW_SYMBOL,
        stype_out = SType.INSTRUMENT_ID)

    @test store isa DBNStore
    @test length(store) == n
    @test store.metadata.dataset == "XNAS.ITCH"
    @test store.metadata.schema == Schema.TRADES
    @test all(r -> r isa DBN.TradeMsg, store)
    # R1: TRADES is type-pure, so the records vector is concretely typed.
    @test store.records isa Vector{DBN.TradeMsg}
    @test eltype(store) === DBN.TradeMsg

    # Wire encoding sanity checks
    qpairs = get(captured[].kwargs, :query, [])
    d = Dict(qpairs)
    @test d["schema"] == "trades"
    @test d["compression"] == "zstd"
    @test d["encoding"] == "dbn"
    @test d["symbols"] == "AAPL"
    @test haskey(d, "end")
    @test !haskey(d, "end_")
    @test occursin("timeseries.get_range", captured[].url)
end

@testset "historical get_range typed=false escape hatch" begin
    bytes, n = _build_sample_dbn_zstd()
    function mock(method, url, headers, body; kwargs...)
        HTTP.Response(200; body = bytes)
    end
    c = Historical("test-key"; gateway = "https://hist.test", dispatcher = mock)
    store = get_range(c;
        dataset = "XNAS.ITCH",
        schema  = Schema.TRADES,
        symbols = ["AAPL"],
        start_dt = "2024-01-02T14:30:00",
        end_dt   = "2024-01-02T14:31:00",
        typed   = false,
    )
    @test store isa DBNStore
    @test length(store) == n
    # With typed=false, records is the Union-typed Vector.
    @test store.records isa Vector{DBN.DBNRecord}
    @test all(r -> r isa DBN.TradeMsg, store)
end

@testset "historical get_range size_hint kwarg" begin
    bytes, n = _build_sample_dbn_zstd()
    function mock(method, url, headers, body; kwargs...)
        HTTP.Response(200; body = bytes)
    end
    c = Historical("test-key"; gateway = "https://hist.test", dispatcher = mock)
    # Exact hint: caller knows the record count from a prior get_record_count.
    store = get_range(c;
        dataset = "XNAS.ITCH",
        schema  = Schema.TRADES,
        symbols = ["AAPL"],
        start_dt = "2024-01-02T14:30:00",
        end_dt   = "2024-01-02T14:31:00",
        size_hint = n,
    )
    @test length(store) == n
    @test store.records isa Vector{DBN.TradeMsg}
    # size_hint is ignored on the typed=false path; verify no crash.
    store2 = get_range(c;
        dataset = "XNAS.ITCH",
        schema  = Schema.TRADES,
        symbols = ["AAPL"],
        start_dt = "2024-01-02T14:30:00",
        end_dt   = "2024-01-02T14:31:00",
        typed = false, size_hint = 999_999,
    )
    @test length(store2) == n
end

@testset "record_type_for_schema" begin
    using DatabentoAPI: record_type_for_schema
    @test record_type_for_schema(Schema.TRADES)     === DBN.TradeMsg
    @test record_type_for_schema(Schema.MBO)        === DBN.MBOMsg
    @test record_type_for_schema(Schema.MBP_1)      === DBN.MBP1Msg
    @test record_type_for_schema(Schema.MBP_10)     === DBN.MBP10Msg
    @test record_type_for_schema(Schema.TBBO)       === DBN.MBP1Msg
    @test record_type_for_schema(Schema.OHLCV_1S)   === DBN.OHLCVMsg
    @test record_type_for_schema(Schema.OHLCV_1M)   === DBN.OHLCVMsg
    @test record_type_for_schema(Schema.OHLCV_1H)   === DBN.OHLCVMsg
    @test record_type_for_schema(Schema.OHLCV_1D)   === DBN.OHLCVMsg
    @test record_type_for_schema(Schema.DEFINITION) === DBN.InstrumentDefMsg
    @test record_type_for_schema(Schema.STATISTICS) === DBN.StatMsg
    @test record_type_for_schema(Schema.STATUS)     === DBN.StatusMsg
    @test record_type_for_schema(Schema.IMBALANCE)  === DBN.ImbalanceMsg
    @test record_type_for_schema(Schema.CMBP_1)     === DBN.CMBP1Msg
    @test record_type_for_schema(Schema.CBBO_1S)    === DBN.CBBO1sMsg
    @test record_type_for_schema(Schema.CBBO_1M)    === DBN.CBBO1mMsg
    @test record_type_for_schema(Schema.TCBBO)      === DBN.TCBBOMsg
    @test record_type_for_schema(Schema.BBO_1S)     === DBN.BBO1sMsg
    @test record_type_for_schema(Schema.BBO_1M)     === DBN.BBO1mMsg
    @test record_type_for_schema(Schema.MIX)        === nothing
end

@testset "foreach_record typed-vs-generic on synthesized stream" begin
    # Exercise both decode paths against in-memory zstd-DBN bytes (no HTTP).
    using DatabentoAPI: open_stream  # not used directly, just docs
    bytes, n = _build_sample_dbn_zstd()

    function decode_typed(bytes)
        seen = Ref(0); first_type = Ref{Any}(nothing)
        io = IOBuffer(bytes)
        decompressed = TranscodingStream(ZstdDecompressor(), io)
        decoder = DBN.DBNDecoder(decompressed)
        DBN.read_header!(decoder)
        DBN._foreach_record_impl(decoder, DBN.TradeMsg) do rec
            seen[] += 1
            first_type[] === nothing && (first_type[] = typeof(rec))
        end
        return seen[], first_type[]
    end

    function decode_generic(bytes)
        seen = Ref(0); first_type = Ref{Any}(nothing)
        io = IOBuffer(bytes)
        decompressed = TranscodingStream(ZstdDecompressor(), io)
        decoder = DBN.DBNDecoder(decompressed)
        DBN.read_header!(decoder)
        while true
            rec = DBN.read_record(decoder)
            rec === nothing && break
            seen[] += 1
            first_type[] === nothing && (first_type[] = typeof(rec))
        end
        return seen[], first_type[]
    end

    n_t, t_t = decode_typed(bytes)
    n_g, t_g = decode_generic(bytes)
    @test n_t == n
    @test n_g == n
    @test t_t === DBN.TradeMsg
    @test t_g === DBN.TradeMsg
end

# A stream_opener that replays a fixed sequence of (status, headers, body) tuples,
# one per reconnect attempt, feeding each body to the consumer as a readable IO.
# This exercises open_stream/foreach_record end-to-end without a live socket.
function _seq_opener(responses)
    i = Ref(0)
    # `consume` is first to match the do-block opener contract (see _default_http_stream).
    return (consume, c, method, url, headers, qpairs) -> begin
        i[] += 1
        status, hdrs, body = responses[i[]]
        return consume(status, hdrs, IOBuffer(body))
    end
end

const _NOSLEEP_TS = _ -> nothing

@testset "foreach_record streams via injected opener" begin
    bytes, n = _build_sample_dbn_zstd()
    opener = _seq_opener([(200, Pair{String,String}[], bytes)])
    c = Historical("test-key"; gateway = "https://hist.test", stream_opener = opener)
    seen = Ref(0)
    md = DatabentoAPI.foreach_record(c;
        dataset = "XNAS.ITCH", schema = Schema.TRADES, symbols = ["AAPL"],
        start_dt = "2024-01-02T14:30:00", end_dt = "2024-01-02T14:31:00") do rec
        seen[] += 1
        @test rec isa DBN.TradeMsg
    end
    @test seen[] == n
    @test md isa DBN.Metadata
    @test md.dataset == "XNAS.ITCH"
end

@testset "foreach_record retries transient status then streams" begin
    bytes, n = _build_sample_dbn_zstd()
    opener = _seq_opener([
        (503, Pair{String,String}[], Vector{UInt8}("temporarily down")),
        (200, Pair{String,String}[], bytes),
    ])
    c = Historical("test-key"; gateway = "https://hist.test",
                   stream_opener = opener, retry_sleep = _NOSLEEP_TS)
    seen = Ref(0)
    DatabentoAPI.foreach_record(c;
        dataset = "XNAS.ITCH", schema = Schema.TRADES, symbols = ["AAPL"],
        start_dt = "2024-01-02T14:30:00", end_dt = "2024-01-02T14:31:00") do rec
        seen[] += 1
    end
    @test seen[] == n
end

@testset "foreach_record maps 4xx → BentoClientError" begin
    body = Vector{UInt8}("""{"detail":{"case":"not_found","message":"nope"}}""")
    opener = _seq_opener([(404, ["request-id" => "rid-1"], body)])
    c = Historical("test-key"; gateway = "https://hist.test", stream_opener = opener)
    @test_throws BentoClientError DatabentoAPI.foreach_record(c;
        dataset = "XNAS.ITCH", schema = Schema.TRADES, symbols = ["AAPL"],
        start_dt = "2024-01-02T14:30:00", end_dt = "2024-01-02T14:31:00") do rec
    end
end

# ---- chunked + concurrent get_range (#33) ----

# Task-safe mock dispatcher for chunked mode: captures every query under a
# lock and lets the test key the response (or failure) off the chunk's
# `start` query param.
function _chunk_mock(respond)   # respond(start_str) -> HTTP.Response (or throw)
    lk = ReentrantLock()
    queries = Vector{Dict{String,String}}()
    dispatcher = (method, url, headers, body; kwargs...) -> begin
        q = Dict(get(kwargs, :query, Pair{String,String}[]))
        lock(lk) do
            push!(queries, q)
        end
        respond(q["start"])
    end
    return dispatcher, queries
end

# Payload whose single trade is tagged (via sequence) so tests can tell which
# chunk each record came from.
function _tagged_payload(tag::Integer)
    _, compressed = _build_dbn_zstd(DBN.DBNRecord[_trade_rec(tag)])
    return compressed
end

@testset "chunked get_range splits, fetches, and concatenates in order (#33)" begin
    # Tag each chunk's record by the day-of-month of the chunk start.
    dispatcher, queries = _chunk_mock(start_str ->
        HTTP.Response(200; body = _tagged_payload(Dates.day(DateTime(start_str)))))
    c = Historical("test-key"; gateway = "https://hist.test", dispatcher = dispatcher)
    store = get_range(c;
        dataset = "XNAS.ITCH", schema = Schema.TRADES, symbols = ["AAPL"],
        start_dt = DateTime(2024, 1, 1), end_dt = DateTime(2024, 1, 4),
        chunk = Day(1))
    @test length(queries) == 3
    starts = sort([q["start"] for q in queries])
    @test starts == ["2024-01-01T00:00:00", "2024-01-02T00:00:00", "2024-01-03T00:00:00"]
    ends = sort([q["end"] for q in queries])
    @test ends == ["2024-01-02T00:00:00", "2024-01-03T00:00:00", "2024-01-04T00:00:00"]
    # Records concatenated in chunk (= time) order regardless of completion order.
    @test store isa DBNStore{DBN.TradeMsg}
    @test [Int(r.sequence) for r in store] == [1, 2, 3]
    @test isempty(store.failed_ranges)

    # Ragged final chunk is clamped to end_dt.
    dispatcher2, queries2 = _chunk_mock(start_str ->
        HTTP.Response(200; body = _tagged_payload(1)))
    c2 = Historical("test-key"; gateway = "https://hist.test", dispatcher = dispatcher2)
    get_range(c2;
        dataset = "XNAS.ITCH", schema = Schema.TRADES, symbols = ["AAPL"],
        start_dt = DateTime(2024, 1, 1), end_dt = DateTime(2024, 1, 3, 12),
        chunk = Day(1))
    @test length(queries2) == 3
    @test maximum(q["end"] for q in queries2) == "2024-01-03T12:00:00"
end

@testset "chunked get_range warns and records failed chunks (#33)" begin
    # Middle chunk (Jan 2) 404s; the others succeed.
    dispatcher, _ = _chunk_mock(start_str ->
        startswith(start_str, "2024-01-02") ?
            HTTP.Response(404; body = """{"detail":{"case":"not_found","message":"nope"}}""") :
            HTTP.Response(200; body = _tagged_payload(Dates.day(DateTime(start_str)))))
    c = Historical("test-key"; gateway = "https://hist.test",
                   dispatcher = dispatcher, max_retries = 0)
    store = @test_logs (:warn, r"chunk failed") (:warn, r"1 of 3 chunks failed") get_range(c;
        dataset = "XNAS.ITCH", schema = Schema.TRADES, symbols = ["AAPL"],
        start_dt = DateTime(2024, 1, 1), end_dt = DateTime(2024, 1, 4),
        chunk = Day(1))
    @test [Int(r.sequence) for r in store] == [1, 3]
    @test store.failed_ranges == [(DateTime(2024, 1, 2), DateTime(2024, 1, 3))]
end

@testset "chunked get_range throws when every chunk fails (#33)" begin
    dispatcher, _ = _chunk_mock(_ ->
        HTTP.Response(404; body = """{"detail":{"case":"not_found","message":"nope"}}"""))
    c = Historical("test-key"; gateway = "https://hist.test",
                   dispatcher = dispatcher, max_retries = 0)
    @test_logs (:warn, r"chunk failed") (:warn, r"chunk failed") match_mode=:any begin
        @test_throws BentoClientError get_range(c;
            dataset = "XNAS.ITCH", schema = Schema.TRADES, symbols = ["AAPL"],
            start_dt = DateTime(2024, 1, 1), end_dt = DateTime(2024, 1, 3),
            chunk = Day(1))
    end
end

@testset "chunked get_range validation (#33)" begin
    c = Historical("test-key"; gateway = "https://hist.test",
                   dispatcher = (args...; kw...) -> error("must not dispatch"))
    common = (dataset = "XNAS.ITCH", schema = Schema.TRADES, symbols = ["AAPL"])
    @test_throws ArgumentError get_range(c; common...,
        start_dt = DateTime(2024, 1, 1), end_dt = DateTime(2024, 1, 3),
        chunk = Day(1), limit = 100)
    @test_throws ArgumentError get_range(c; common...,
        start_dt = DateTime(2024, 1, 1), chunk = Day(1))           # no end_dt
    @test_throws ArgumentError get_range(c; common...,
        start_dt = 1_700_000_000_000_000_000, end_dt = DateTime(2024, 1, 3),
        chunk = Day(1))                                            # unix-ns int
    @test_throws ArgumentError get_range(c; common...,
        start_dt = DateTime(2024, 1, 3), end_dt = DateTime(2024, 1, 1),
        chunk = Day(1))                                            # start ≥ end
    @test_throws ArgumentError get_range(c; common...,
        start_dt = DateTime(2024, 1, 1), end_dt = DateTime(2024, 1, 3),
        chunk = Day(1), concurrency = 0)
    @test_throws ArgumentError get_range(c; common...,
        start_dt = DateTime(2024, 1, 1), end_dt = DateTime(2024, 1, 3),
        chunk = Day(0))                                            # non-advancing
    @test_throws ArgumentError get_range(c; common...,
        start_dt = DateTime(2024, 1, 1), end_dt = DateTime(2024, 1, 3),
        chunk = Day(-1))                                           # negative
end

@testset "chunk=nothing keeps single-request behavior (#33)" begin
    bytes, n = _build_sample_dbn_zstd()
    calls = Ref(0)
    mock = (method, url, headers, body; kwargs...) -> begin
        calls[] += 1
        HTTP.Response(200; body = bytes)
    end
    c = Historical("test-key"; gateway = "https://hist.test", dispatcher = mock)
    store = get_range(c;
        dataset = "XNAS.ITCH", schema = Schema.TRADES, symbols = ["AAPL"],
        start_dt = "2024-01-02T14:30:00", end_dt = "2024-01-02T14:31:00")
    @test calls[] == 1
    @test length(store) == n
    @test isempty(store.failed_ranges)
    # 2-arg constructors (pre-failed_ranges API) still work.
    s2 = DBNStore(store.metadata, store.records)
    @test s2 isa DBNStore{DBN.TradeMsg} && isempty(s2.failed_ranges)
    s3 = DBNStore{DBN.TradeMsg}(store.metadata, store.records)
    @test isempty(s3.failed_ranges)
end

@testset "chunked get_range respects the concurrency gate (#33)" begin
    in_flight = Threads.Atomic{Int}(0)
    max_seen = Threads.Atomic{Int}(0)
    payload = _tagged_payload(1)
    dispatcher = (method, url, headers, body; kwargs...) -> begin
        cur = Threads.atomic_add!(in_flight, 1) + 1
        # Record the high-water mark of simultaneous in-flight requests.
        old = max_seen[]
        while cur > old
            Threads.atomic_cas!(max_seen, old, cur) == old && break
            old = max_seen[]
        end
        sleep(0.05)   # hold the slot long enough for overlap to be observable
        Threads.atomic_sub!(in_flight, 1)
        HTTP.Response(200; body = payload)
    end
    c = Historical("test-key"; gateway = "https://hist.test", dispatcher = dispatcher)
    store = get_range(c;
        dataset = "XNAS.ITCH", schema = Schema.TRADES, symbols = ["AAPL"],
        start_dt = DateTime(2024, 1, 1), end_dt = DateTime(2024, 1, 5),
        chunk = Day(1), concurrency = 2)
    @test length(store) == 4
    @test max_seen[] <= 2
end

@testset "typed get_range tolerates interleaved control records (#30)" begin
    _, compressed, n_trades = _build_mixed_dbn_zstd()
    mock = (method, url, headers, body; kwargs...) -> HTTP.Response(200; body = compressed)
    c = Historical("test-key"; gateway = "https://hist.test", dispatcher = mock)
    store = @test_logs (:warn, r"gateway error record") match_mode=:any get_range(c;
        dataset = "XNAS.ITCH", schema = Schema.TRADES, symbols = ["AAPL"],
        start_dt = "2024-01-02T14:30:00", end_dt = "2024-01-02T14:31:00")
    @test store.records isa Vector{DBN.TradeMsg}
    @test length(store) == n_trades
    @test [r.sequence for r in store] == UInt32[1, 2, 3, 4]
end

@testset "typed foreach_record tolerates interleaved control records (#30)" begin
    _, compressed, n_trades = _build_mixed_dbn_zstd()
    opener = _seq_opener([(200, Pair{String,String}[], compressed)])
    c = Historical("test-key"; gateway = "https://hist.test", stream_opener = opener)
    seen = Ref(0)
    md = @test_logs (:warn, r"gateway error record") match_mode=:any begin
        DatabentoAPI.foreach_record(c;
            dataset = "XNAS.ITCH", schema = Schema.TRADES, symbols = ["AAPL"],
            start_dt = "2024-01-02T14:30:00", end_dt = "2024-01-02T14:31:00") do rec
            @test rec isa DBN.TradeMsg
            seen[] += 1
        end
    end
    @test seen[] == n_trades
    @test md isa DBN.Metadata
end

@testset "typed decode skips unknown rtypes with a summary warn (#30)" begin
    raw, _, n_trades = _build_mixed_dbn_zstd()
    # Splice a fake 8-byte record (length = 2 four-byte units, rtype 0xFE)
    # onto the end of the uncompressed stream — records are just concatenated.
    fake = vcat(UInt8[0x02, 0xFE], zeros(UInt8, 6))
    compressed = transcode(ZstdCompressor, vcat(raw, fake))
    mock = (method, url, headers, body; kwargs...) -> HTTP.Response(200; body = compressed)
    c = Historical("test-key"; gateway = "https://hist.test", dispatcher = mock)
    store = @test_logs (:warn, r"gateway error record") (:warn, r"skipped records") get_range(c;
        dataset = "XNAS.ITCH", schema = Schema.TRADES, symbols = ["AAPL"],
        start_dt = "2024-01-02T14:30:00", end_dt = "2024-01-02T14:31:00")
    @test length(store) == n_trades
end

@testset "wrong record_type override skips with summary warn instead of throwing (#30)" begin
    _, compressed, n_trades = _build_mixed_dbn_zstd()
    opener = _seq_opener([(200, Pair{String,String}[], compressed)])
    c = Historical("test-key"; gateway = "https://hist.test", stream_opener = opener)
    seen = Ref(0)
    @test_logs (:warn, r"gateway error record") (:warn, r"skipped records") begin
        DatabentoAPI.foreach_record(c;
            dataset = "XNAS.ITCH", schema = Schema.TRADES, symbols = ["AAPL"],
            start_dt = "2024-01-02T14:30:00", end_dt = "2024-01-02T14:31:00",
            record_type = DBN.MBOMsg) do rec
            seen[] += 1
        end
    end
    @test seen[] == 0   # every trade skipped: stream rtype ≠ MBOMsg
end

@testset "pure typed stream decodes with no warnings (#30 regression)" begin
    bytes, n = _build_sample_dbn_zstd()
    mock = (method, url, headers, body; kwargs...) -> HTTP.Response(200; body = bytes)
    c = Historical("test-key"; gateway = "https://hist.test", dispatcher = mock)
    store = @test_logs min_level=Logging.Warn get_range(c;
        dataset = "XNAS.ITCH", schema = Schema.TRADES, symbols = ["AAPL"],
        start_dt = "2024-01-02T14:30:00", end_dt = "2024-01-02T14:31:00")
    @test length(store) == n
end

@testset "foreach_record maps read timeout → BentoTimeoutError" begin
    calls = Ref(0)
    opener = (consume, c, method, url, headers, qpairs) -> begin
        calls[] += 1
        throw(HTTP.Exceptions.TimeoutError(100))
    end
    c = Historical("test-key"; gateway = "https://hist.test",
                   stream_opener = opener, retry_sleep = _NOSLEEP_TS, timeout = 100)
    @test_throws BentoTimeoutError DatabentoAPI.foreach_record(c;
        dataset = "XNAS.ITCH", schema = Schema.TRADES, symbols = ["AAPL"],
        start_dt = "2024-01-02T14:30:00", end_dt = "2024-01-02T14:31:00") do rec
    end
    @test calls[] == 1   # read timeouts are not retried
end

@testset "consumer-raised HTTP TimeoutError is not blamed on the stream" begin
    # If the callback's own code throws HTTP.Exceptions.TimeoutError (e.g. it
    # makes its own HTTP request), that must propagate untouched — mapping it
    # to BentoTimeoutError would misreport the Databento stream as the culprit.
    bytes, _ = _build_sample_dbn_zstd()
    opener = _seq_opener([(200, Pair{String,String}[], bytes)])
    c = Historical("test-key"; gateway = "https://hist.test",
                   stream_opener = opener, retry_sleep = _NOSLEEP_TS, timeout = 100)
    @test_throws HTTP.Exceptions.TimeoutError DatabentoAPI.foreach_record(c;
        dataset = "XNAS.ITCH", schema = Schema.TRADES, symbols = ["AAPL"],
        start_dt = "2024-01-02T14:30:00", end_dt = "2024-01-02T14:31:00") do rec
        throw(HTTP.Exceptions.TimeoutError(42))   # consumer's own timeout
    end
end

@testset "foreach_record propagates a consumer exception" begin
    bytes, n = _build_sample_dbn_zstd()
    opener = _seq_opener([(200, Pair{String,String}[], bytes)])
    c = Historical("test-key"; gateway = "https://hist.test", stream_opener = opener)
    # An exception from the consumer must propagate (open_stream's do-block tears
    # the connection down on the way out — verified here via the error path).
    @test_throws ErrorException DatabentoAPI.foreach_record(c;
        dataset = "XNAS.ITCH", schema = Schema.TRADES, symbols = ["AAPL"],
        start_dt = "2024-01-02T14:30:00", end_dt = "2024-01-02T14:31:00") do rec
        error("consumer aborted")
    end
end
