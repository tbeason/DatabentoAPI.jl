module Fixtures

using DatabentoBinaryEncoding
import DatabentoBinaryEncoding as DBN
using CodecZstd
using TranscodingStreams

const HERE      = @__DIR__
const DATA_DIR  = joinpath(HERE, "data")
const DBN_DATA  = abspath(joinpath(HERE, "..", "..", "DatabentoBinaryEncoding.jl", "benchmark", "data"))

# Sized fixtures. Tier names mirror DBN.jl's existing files when available.
const SIZES = (
    small  = 10_000,
    medium = 100_000,
    large  = 1_000_000,
)

"""
    trades_metadata(dataset="TEST.MOCK")

A minimal Metadata struct with no symbol mappings, suitable for synthesized
TRADES-schema fixtures.
"""
function trades_metadata(dataset::AbstractString = "TEST.MOCK")
    DBN.Metadata(
        DBN.DBN_VERSION, String(dataset), DBN.Schema.TRADES,
        Int64(0), nothing, nothing, nothing,
        DBN.SType.INSTRUMENT_ID, false,
        ["AAPL"], String[], String[],
        Tuple{String,String,Int64,Int64}[],
    )
end

"""
    synth_trades(n; dataset="TEST.MOCK") -> Vector{DBN.TradeMsg}

Generate a deterministic vector of `n` TradeMsg records. Cheap; allocates
once per record.
"""
function synth_trades(n::Integer; dataset::AbstractString = "TEST.MOCK")
    recs = Vector{DBN.TradeMsg}(undef, n)
    base_ts = Int64(1_700_000_000_000_000_000)
    @inbounds for i in 1:n
        hd = DBN.RecordHeader(
            UInt8(0), DBN.RType.MBP_0_MSG,
            UInt16(1), UInt32(100 + (i & 0xFFFF)),
            base_ts + i * 1_000_000,
        )
        recs[i] = DBN.TradeMsg(
            hd,
            Int64(150_000_000_000 + i * 1_000_000),
            UInt32(100),
            DBN.Action.TRADE, DBN.Side.ASK,
            UInt8(0), UInt8(1),
            base_ts + i * 1_000_000,
            Int32(0), UInt32(i),
        )
    end
    return recs
end

# Where DBN.jl ships fixtures, prefer them so cross-package numbers compare
# apples-to-apples; otherwise synthesize+cache locally under benchmark/data/.
function _dbn_jl_path(name::AbstractString)
    p = joinpath(DBN_DATA, name)
    isfile(p) ? p : nothing
end

function _local_cache_path(tier::Symbol; zst::Bool)
    mkpath(DATA_DIR)
    ext = zst ? ".dbn.zst" : ".dbn"
    return joinpath(DATA_DIR, "trades_" * string(tier) * ext)
end

"""
    trades_file(tier::Symbol; zst::Bool=false) -> String

Return a path to a TRADES-schema DBN fixture for the given size tier
(`:small`, `:medium`, `:large`). Prefers the matching DBN.jl benchmark
fixture when present, falls back to a locally cached synthesized file under
`benchmark/data/`. Compresses (zstd) on demand.
"""
function trades_file(tier::Symbol; zst::Bool = false)
    sized = SIZES[tier]
    # Map to DBN.jl naming.
    dbn_name = if tier === :small
        "trades.100k.dbn"     # DBN.jl ships 100k as smallest; bigger than our 10k but fine
    elseif tier === :medium
        "trades.1m.dbn"
    elseif tier === :large
        "trades.10m.dbn"
    else
        nothing
    end
    if dbn_name !== nothing
        candidate = _dbn_jl_path(zst ? dbn_name * ".zst" : dbn_name)
        candidate === nothing || return candidate
    end
    # Fallback: synthesize and cache.
    cache = _local_cache_path(tier; zst = zst)
    if !isfile(cache)
        @info "Synthesizing fixture (one-time)" tier sized zst path=cache
        md   = trades_metadata()
        recs = synth_trades(sized)
        if zst
            tmp = tempname() * ".dbn"
            try
                DBN.write_dbn(tmp, md, recs)
                open(cache, "w") do out
                    enc = TranscodingStream(ZstdCompressor(level = 3), out)
                    open(tmp, "r") do src
                        write(enc, src)
                    end
                    close(enc)
                end
            finally
                rm(tmp; force = true)
            end
        else
            DBN.write_dbn(cache, md, recs)
        end
    end
    return cache
end

"""
    trades_bytes(tier::Symbol; zst::Bool=false) -> Vector{UInt8}

In-memory bytes for the fixture file. Cached once per call.
"""
function trades_bytes(tier::Symbol; zst::Bool = false)
    return read(trades_file(tier; zst = zst))
end

# Lazy module-level cache (one allocation per fixture per Julia session).
const _BYTES_CACHE = Dict{Tuple{Symbol,Bool},Vector{UInt8}}()
function cached_trades_bytes(tier::Symbol; zst::Bool = false)
    get!(_BYTES_CACHE, (tier, zst)) do
        trades_bytes(tier; zst = zst)
    end
end

const _COUNT_CACHE = Dict{String,Int}()
"""
    record_count(file) -> Int

Number of records in the DBN file at `file`. Cached per session.
"""
function record_count(file::AbstractString)
    get!(_COUNT_CACHE, String(file)) do
        length(DBN.read_dbn(String(file)))
    end
end

const _RECORDS_CACHE = Dict{Symbol,Vector{DBN.TradeMsg}}()
"""
    cached_trades_records(tier) -> Vector{DBN.TradeMsg}

Records vector for the tier, cached per session. Useful for write benchmarks
where we want a stable input.
"""
function cached_trades_records(tier::Symbol)
    get!(_RECORDS_CACHE, tier) do
        synth_trades(SIZES[tier])
    end
end

end # module Fixtures
