module BenchHistoricalDecode

include("fixtures.jl"); using .Fixtures
include("bench_common.jl"); using .BenchCommon

using DatabentoAPI
using DatabentoBinaryEncoding
import DatabentoBinaryEncoding as DBN
using HTTP
using CodecZstd
using TranscodingStreams
using Dates

const SUITE = "historical_decode"

# Mock dispatcher returning canned zstd-DBN bytes from a fixture.
function _make_mock(bytes)
    return function (method, url, headers, body; kwargs...)
        return HTTP.Response(200; body = bytes)
    end
end

function _get_range_call(c)
    store = DatabentoAPI.get_range(c;
        dataset = "TEST.MOCK", schema = Schema.TRADES,
        symbols = ["AAPL"],
        start_dt = "2024-01-02T14:30:00",
        end_dt   = "2024-01-02T14:31:00",
        stype_in = SType.RAW_SYMBOL,
        stype_out = SType.INSTRUMENT_ID,
    )
    return length(store)
end

# `foreach_record` calls HTTP.open directly (not through `HTTPClient.dispatcher`),
# so we can't intercept it the same way as `get_range`. To benchmark the post-HTTP
# decode pipeline that `foreach_record` uses, we recreate it directly here from a
# byte buffer (TranscodingStream(ZstdDecompressor) → DBN.DBNDecoder → typed/generic
# read loop). This measures everything past the HTTP boundary.
function _foreach_decode_typed(bytes)
    n = 0
    io = IOBuffer(bytes)
    decompressed = TranscodingStream(ZstdDecompressor(), io)
    decoder = DBN.DBNDecoder(decompressed)
    DBN.read_header!(decoder)
    DBN._foreach_record_impl(decoder, DBN.TradeMsg) do _rec
        n += 1
    end
    return n
end

function _foreach_decode_generic(bytes)
    n = 0
    io = IOBuffer(bytes)
    decompressed = TranscodingStream(ZstdDecompressor(), io)
    decoder = DBN.DBNDecoder(decompressed)
    DBN.read_header!(decoder)
    while true
        rec = DBN.read_record(decoder)
        rec === nothing && break
        n += 1
    end
    return n
end

function run(; tiers = (:small, :medium))
    println("\n=== ", SUITE, " ===")
    for tier in tiers
        file = Fixtures.trades_file(tier; zst = true)
        bytes = Fixtures.cached_trades_bytes(tier; zst = true)
        bytes_in = length(bytes)
        n = Fixtures.record_count(file)
        mock = _make_mock(bytes)
        c = DatabentoAPI.Historical("test-key"; gateway = "https://hist.test",
                                     dispatcher = mock)

        BenchCommon.bench_to_row(
            suite = SUITE, path = "get_range", size = string(tier),
            n_records = n, bytes_in = bytes_in,
            samples = 5, seconds = 30.0,
            f = () -> _get_range_call(c),
        )
        BenchCommon.bench_to_row(
            suite = SUITE, path = "post_http_decode_typed", size = string(tier),
            n_records = n, bytes_in = bytes_in,
            samples = 5, seconds = 30.0,
            f = () -> _foreach_decode_typed(bytes),
        )
        BenchCommon.bench_to_row(
            suite = SUITE, path = "post_http_decode_generic", size = string(tier),
            n_records = n, bytes_in = bytes_in,
            samples = 5, seconds = 30.0,
            f = () -> _foreach_decode_generic(bytes),
        )
    end
end

end # module BenchHistoricalDecode

if abspath(PROGRAM_FILE) == @__FILE__
    BenchHistoricalDecode.run()
end
