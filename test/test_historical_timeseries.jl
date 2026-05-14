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
