module BenchDBNWrite

include("fixtures.jl"); using .Fixtures
include("bench_common.jl"); using .BenchCommon

using DatabentoBinaryEncoding
import DatabentoBinaryEncoding as DBN
using CodecZstd
using TranscodingStreams

const SUITE = "dbn_write"

function _write_dbn_eager(path, md, recs)
    DBN.write_dbn(path, md, recs)
    return length(recs)
end

function _write_via_stream_writer(path, dataset, recs)
    w = DBN.DBNStreamWriter(path, dataset, DBN.Schema.TRADES;
                            auto_flush = true, flush_interval = 10_000)
    for r in recs
        DBN.write_record!(w, r)
    end
    DBN.close_writer!(w)
    return length(recs)
end

# Mirror DatabentoAPI's stream_to_file IO stack: open file → ZstdCompressor → DBNEncoder.
function _write_via_encoder_zstd(path, md, recs; level::Int = 3)
    raw = open(path, "w")
    top = TranscodingStream(ZstdCompressor(level = level), raw)
    enc = DBN.DBNEncoder(top, md)
    DBN.write_header(enc)
    for r in recs
        DBN.write_record(enc, r)
    end
    flush(top)
    close(top)
    close(raw)
    return length(recs)
end

function _write_via_encoder_uncompressed(path, md, recs)
    raw = open(path, "w")
    enc = DBN.DBNEncoder(raw, md)
    DBN.write_header(enc)
    for r in recs
        DBN.write_record(enc, r)
    end
    close(raw)
    return length(recs)
end

function run(; tiers = (:small, :medium))
    println("\n=== ", SUITE, " ===")
    md = Fixtures.trades_metadata()
    for tier in tiers
        recs   = Fixtures.cached_trades_records(tier)
        n      = length(recs)
        bytes  = n * 40   # rough TradeMsg-on-wire size

        # write_dbn(path, ..., uncompressed)
        let path = tempname() * ".dbn"
            BenchCommon.bench_to_row(
                suite = SUITE, path = "write_dbn", size = string(tier),
                n_records = n, bytes_in = bytes,
                samples = 3, seconds = 30.0,
                f = () -> begin
                    _write_dbn_eager(path, md, recs)
                    rm(path; force = true)
                end,
            )
        end

        # write_dbn (zst inferred from extension)
        let path = tempname() * ".dbn.zst"
            BenchCommon.bench_to_row(
                suite = SUITE, path = "write_dbn_zst", size = string(tier),
                n_records = n, bytes_in = bytes,
                samples = 3, seconds = 30.0,
                f = () -> begin
                    _write_dbn_eager(path, md, recs)
                    rm(path; force = true)
                end,
            )
        end

        # DBNStreamWriter
        let path = tempname() * ".dbn"
            BenchCommon.bench_to_row(
                suite = SUITE, path = "DBNStreamWriter", size = string(tier),
                n_records = n, bytes_in = bytes,
                samples = 3, seconds = 30.0,
                f = () -> begin
                    _write_via_stream_writer(path, "TEST.MOCK", recs)
                    rm(path; force = true)
                end,
            )
        end

        # Encoder direct (uncompressed) — matches an internal write hot path
        let path = tempname() * ".dbn"
            BenchCommon.bench_to_row(
                suite = SUITE, path = "DBNEncoder_raw", size = string(tier),
                n_records = n, bytes_in = bytes,
                samples = 3, seconds = 30.0,
                f = () -> begin
                    _write_via_encoder_uncompressed(path, md, recs)
                    rm(path; force = true)
                end,
            )
        end

        # Encoder + TranscodingStream(ZstdCompressor) — exact stream_to_file path
        for level in (1, 3, 9)
            let path = tempname() * ".dbn.zst"
                BenchCommon.bench_to_row(
                    suite = SUITE, path = "DBNEncoder_zstd_L$(level)", size = string(tier),
                    n_records = n, bytes_in = bytes,
                    samples = 3, seconds = 30.0,
                    f = () -> begin
                        _write_via_encoder_zstd(path, md, recs; level = level)
                        rm(path; force = true)
                    end,
                )
            end
        end
    end
end

end # module BenchDBNWrite

if abspath(PROGRAM_FILE) == @__FILE__
    BenchDBNWrite.run()
end
