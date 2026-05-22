module ProfileLiveReader

# Profiles the live reader pipeline (CountingIO → BufferedReader → DBNDecoder
# → Channel{DBN.DBNRecord} → consumer) using an in-process IOBuffer source
# instead of a TCP socket. This isolates the decode + channel hot path from
# the kernel TCP semantics that the bench_live_reader run measures.

include("fixtures.jl"); using .Fixtures

using DatabentoAPI
using DatabentoBinaryEncoding
import DatabentoBinaryEncoding as DBN
using Profile
using Printf
using CodecZstd
using TranscodingStreams

const HERE        = @__DIR__
const RESULTS_DIR = joinpath(HERE, "results")
mkpath(RESULTS_DIR)

# The piece of `_reader_loop` we want to profile, minus the TCP socket.
# Matches the structure in src/live/reader.jl exactly so the flat profile is
# directly comparable.
function drain_through_pipeline(bytes::Vector{UInt8}, expected::Int;
                                wire_zstd::Bool,
                                channel_eltype::Type = DBN.DBNRecord)
    src_io = IOBuffer(bytes)
    raw = wire_zstd ? TranscodingStream(ZstdDecompressor(), src_io) : src_io
    counting = DatabentoAPI.CountingIO(raw)
    buffered = DBN.BufferedReader(counting)
    decoder  = DBN.DBNDecoder(buffered)
    DBN.read_header!(decoder)
    ch = Channel{channel_eltype}(16_384)
    consumer = @async begin
        n = 0
        try
            while n < expected
                _ = take!(ch)
                n += 1
            end
        catch e
            e isa InvalidStateException || rethrow()
        end
        n
    end
    n_in = 0
    try
        while n_in < expected
            rec = DBN.read_record(decoder)
            rec === nothing && break
            put!(ch, rec)
            n_in += 1
        end
    finally
        close(ch)
    end
    return fetch(consumer)
end

# Typed-loop variant: uses _foreach_record_impl + concrete-typed channel.
# This is the proposed fix from R3 — measures what we'd get from a typed
# reader path when the subscription is to a single type-pure schema.
function drain_through_typed_pipeline(bytes::Vector{UInt8}, expected::Int,
                                      ::Type{T}; wire_zstd::Bool) where {T}
    src_io = IOBuffer(bytes)
    raw = wire_zstd ? TranscodingStream(ZstdDecompressor(), src_io) : src_io
    counting = DatabentoAPI.CountingIO(raw)
    buffered = DBN.BufferedReader(counting)
    decoder  = DBN.DBNDecoder(buffered)
    DBN.read_header!(decoder)
    ch = Channel{T}(16_384)
    consumer = @async begin
        n = 0
        try
            while n < expected
                _ = take!(ch)
                n += 1
            end
        catch e
            e isa InvalidStateException || rethrow()
        end
        n
    end
    n_in = 0
    try
        DBN._foreach_record_impl(decoder, T) do rec
            put!(ch, rec)
            n_in += 1
        end
    finally
        close(ch)
    end
    return fetch(consumer)
end

# Channel-only baseline: no decode, no IO. Pure put!/take! cost.
function drain_channel_only(n::Int, ::Type{T}, value::T) where {T}
    ch = Channel{T}(16_384)
    consumer = @async begin
        count = 0
        try
            while count < n
                _ = take!(ch)
                count += 1
            end
        catch e
            e isa InvalidStateException || rethrow()
        end
        count
    end
    for _ in 1:n
        put!(ch, value)
    end
    close(ch)
    return fetch(consumer)
end

function _time_run(label::AbstractString, f; samples::Int = 3)
    f()  # warmup
    best = Inf; allocs = 0
    for _ in 1:samples
        GC.gc()
        g0 = Base.gc_num()
        t0 = time_ns()
        f()
        t = (time_ns() - t0) * 1e-9
        a = Int(Base.gc_num().allocd - g0.allocd)
        if t < best
            best = t; allocs = a
        end
    end
    return best, allocs
end

function _profile(label::AbstractString, f; seconds::Real = 5.0)
    Profile.clear()
    Profile.init(n = 10_000_000, delay = 0.001)
    GC.gc()
    g0 = Base.gc_num()
    t0 = time_ns()
    Profile.@profile begin
        deadline = t0 + UInt64(seconds * 1e9)
        while time_ns() < deadline
            f()
        end
    end
    t1 = time_ns()
    g1 = Base.gc_num()
    gcf = (g1.total_time - g0.total_time) / max(1, t1 - t0)
    outpath = joinpath(RESULTS_DIR, label * ".profile.txt")
    open(outpath, "w") do io
        println(io, "# ", label)
        println(io, "# elapsed: ", (t1 - t0) * 1e-9, " s")
        println(io, "# gc:      ", gcf * 100, " %")
        println(io)
        Profile.print(IOContext(io, :displaysize => (300, 200));
                      format = :flat, sortedby = :count,
                      mincount = 20, maxdepth = 25)
    end
    println("  wrote ", outpath)
end

function run(; tier::Symbol = :small, seconds::Real = 5.0)
    println("\n=== live reader in-process profile (tier=$tier) ===")
    bytes = Fixtures.cached_trades_bytes(tier; zst = false)
    file  = Fixtures.trades_file(tier; zst = false)
    n     = Fixtures.record_count(file)
    println("  records: ", n, "  bytes: ", length(bytes))

    sentinel = DBN.TradeMsg(
        DBN.RecordHeader(UInt8(0), DBN.RType.MBP_0_MSG, UInt16(1), UInt32(1),
                         Int64(1_700_000_000_000_000_000)),
        Int64(150_000_000_000), UInt32(100),
        DBN.Action.TRADE, DBN.Side.ASK, UInt8(0), UInt8(1),
        Int64(1_700_000_000_000_000_000), Int32(0), UInt32(1),
    )

    println("\nTimings (min over 3 samples, in-process — no TCP):")
    for (label, f) in (
        ("channel_only_Any{TradeMsg}",
            () -> drain_channel_only(n, Any, sentinel)),
        ("channel_only_Union{DBNRecord}",
            () -> drain_channel_only(n, DBN.DBNRecord, sentinel)),
        ("channel_only_Concrete{TradeMsg}",
            () -> drain_channel_only(n, DBN.TradeMsg, sentinel)),
        ("pipeline_Union_channel (current)",
            () -> drain_through_pipeline(bytes, n; wire_zstd = false,
                                         channel_eltype = DBN.DBNRecord)),
        ("pipeline_Any_channel",
            () -> drain_through_pipeline(bytes, n; wire_zstd = false,
                                         channel_eltype = Any)),
        ("pipeline_typed (proposed)",
            () -> drain_through_typed_pipeline(bytes, n, DBN.TradeMsg;
                                               wire_zstd = false)),
    )
        t, a = _time_run(label, f; samples = 3)
        Printf.@printf("  %-44s  min=%7.2f ms  alloc=%6.2f MB  %7.2f M rec/s\n",
            label, t * 1e3, a / (1024 * 1024), n / t / 1e6)
    end

    println("\nProfile dumps (5 s each):")
    _profile("live_inproc_union_channel",
             () -> drain_through_pipeline(bytes, n; wire_zstd = false,
                                          channel_eltype = DBN.DBNRecord);
             seconds = seconds)
    _profile("live_inproc_typed",
             () -> drain_through_typed_pipeline(bytes, n, DBN.TradeMsg;
                                                wire_zstd = false);
             seconds = seconds)
end

end # module ProfileLiveReader

if abspath(PROGRAM_FILE) == @__FILE__
    ProfileLiveReader.run()
end
