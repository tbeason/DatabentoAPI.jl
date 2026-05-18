module BenchDBNRead

include("fixtures.jl"); using .Fixtures
include("bench_common.jl"); using .BenchCommon

using DBN
using CodecZstd
using TranscodingStreams

const SUITE = "dbn_read"

# Each path counts records to prevent dead-code elimination, but the BenchmarkTools
# `@benchmarkable` macro already takes that responsibility.

function _read_dbn(file)
    recs = DBN.read_dbn(file)
    return length(recs)
end

function _read_dbn_typed(file, T)
    recs = DBN.read_dbn_typed(file, T)
    return length(recs)
end

function _foreach_record_typed(file, T)
    n = Ref(0)
    DBN.foreach_record(file, T) do _rec
        n[] += 1
    end
    return n[]
end

function _dbn_stream(file)
    n = 0
    for _rec in DBN.DBNStream(file)
        n += 1
    end
    return n
end

function run(; tiers = (:small, :medium))
    println("\n=== ", SUITE, " ===")
    for tier in tiers
        for zst in (false, true)
            file = Fixtures.trades_file(tier; zst = zst)
            bytes = filesize(file)
            n = Fixtures.record_count(file)
            size_label = string(tier, zst ? "_zst" : "")
            BenchCommon.bench_to_row(
                suite = SUITE, path = "read_dbn", size = size_label,
                n_records = n, bytes_in = bytes,
                f = () -> _read_dbn(file),
            )
            BenchCommon.bench_to_row(
                suite = SUITE, path = "read_dbn_typed", size = size_label,
                n_records = n, bytes_in = bytes,
                f = () -> _read_dbn_typed(file, DBN.TradeMsg),
            )
            BenchCommon.bench_to_row(
                suite = SUITE, path = "foreach_record_typed", size = size_label,
                n_records = n, bytes_in = bytes,
                f = () -> _foreach_record_typed(file, DBN.TradeMsg),
            )
            BenchCommon.bench_to_row(
                suite = SUITE, path = "DBNStream", size = size_label,
                n_records = n, bytes_in = bytes,
                f = () -> _dbn_stream(file),
            )
        end
    end
end

end # module BenchDBNRead

# When run as a script.
if abspath(PROGRAM_FILE) == @__FILE__
    BenchDBNRead.run()
end
