using Test
using DatabentoAPI
using HTTP
using CodecZstd
using TranscodingStreams
using DBN

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
        start   = "2024-01-02T14:30:00",
        end_    = "2024-01-02T14:31:00",
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
        start   = "2024-01-02T14:30:00",
        end_    = "2024-01-02T14:31:00",
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
        start   = "2024-01-02T14:30:00",
        end_    = "2024-01-02T14:31:00",
        size_hint = n,
    )
    @test length(store) == n
    @test store.records isa Vector{DBN.TradeMsg}
    # size_hint is ignored on the typed=false path; verify no crash.
    store2 = get_range(c;
        dataset = "XNAS.ITCH",
        schema  = Schema.TRADES,
        symbols = ["AAPL"],
        start   = "2024-01-02T14:30:00",
        end_    = "2024-01-02T14:31:00",
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
