module BenchLiveReader

include("fixtures.jl"); using .Fixtures
include("bench_common.jl"); using .BenchCommon

using DatabentoAPI
using DatabentoAPI: read_text_frame, build_text_frame, cram_response
using DBN
using CodecZstd
using TranscodingStreams
using Sockets

const SUITE = "live_reader"
const TEST_KEY = "db-1234567890abcdef12345"
const TEST_CHALLENGE = "abcdef0123456789"

# Spawn a one-shot mock LSG that completes CRAM handshake then writes the
# pre-encoded `bytes` payload in 1 MB chunks (mirrors real gateway pacing).
function _spawn(bytes; wire_zstd::Bool = false)
    payload = if wire_zstd
        transcode(ZstdCompressor, bytes)
    else
        bytes
    end
    server = Sockets.listen(Sockets.IPv4("127.0.0.1"), 0)
    port = Sockets.getsockname(server)[2]
    accept_task = @async begin
        try
            sock = Sockets.accept(server)
            try
                write(sock, build_text_frame(lsg_version = "0.9.0"))
                write(sock, build_text_frame(cram = TEST_CHALLENGE))
                flush(sock)
                read_text_frame(sock)  # auth
                write(sock, build_text_frame(success = "1", session_id = "bench"))
                flush(sock)
                read_text_frame(sock)  # subscribe
                read_text_frame(sock)  # start_session
                chunk = 1024 * 1024
                io = IOBuffer(payload)
                while !eof(io)
                    write(sock, read(io, chunk))
                    flush(sock)
                end
                # Hold long enough for the client to drain.
                sleep(2.0)
            finally
                isopen(sock) && Sockets.close(sock)
            end
        finally
            isopen(server) && Sockets.close(server)
        end
    end
    return (; port = Int(port), accept_task)
end

# Returns the wall-clock duration (s) spent just consuming the channel — the
# spawn/connect/handshake and mock-close are excluded so the throughput number
# reflects the reader/decode loop alone.
function _drain_live_timed(bytes; wire_zstd::Bool, expected::Int)
    mock = _spawn(bytes; wire_zstd = wire_zstd)
    comp = wire_zstd ? Compression.ZSTD : Compression.NONE
    client = DatabentoAPI.Live(TEST_KEY;
        dataset = "TEST.MOCK",
        gateway = "127.0.0.1",
        port = mock.port,
        channel_size = 16_384,
        compression = comp,
    )
    DatabentoAPI.connect!(client)
    DatabentoAPI.subscribe!(client;
        schema = Schema.TRADES, symbols = ["AAPL"],
        stype_in = SType.RAW_SYMBOL)
    DatabentoAPI.start!(client)
    # Block until the first record arrives — this excludes server-side
    # handshake/network setup from the measured window.
    first = take!(client.channel)
    n = 1
    t0 = time_ns()
    deadline = time() + 60.0
    try
        while n < expected && time() < deadline
            try
                _ = take!(client.channel)
                n += 1
            catch e
                e isa InvalidStateException && break
                rethrow()
            end
        end
    finally
        nothing
    end
    elapsed_s = (time_ns() - t0) * 1e-9
    try; close(client); catch; end
    try; wait(mock.accept_task); catch; end
    return n, elapsed_s
end

function _bench_drain(bytes, expected, wire_zstd; samples::Int)
    # Warmup
    _drain_live_timed(bytes; wire_zstd = wire_zstd, expected = expected)
    best = Inf; times = Float64[]
    for _ in 1:samples
        _, t = _drain_live_timed(bytes; wire_zstd = wire_zstd, expected = expected)
        push!(times, t); t < best && (best = t)
    end
    sort!(times)
    med = times[cld(length(times), 2)]
    return best, med
end

function run(; tiers = (:small, :medium))
    println("\n=== ", SUITE, " ===")
    for tier in tiers
        file = Fixtures.trades_file(tier; zst = false)
        bytes = Fixtures.cached_trades_bytes(tier; zst = false)
        n = Fixtures.record_count(file)
        for wire_zstd in (false, true)
            label = wire_zstd ? "live_zstd" : "live_plain"
            best, med = _bench_drain(bytes, n, wire_zstd; samples = 3)
            rps = n / best
            mbps = (length(bytes) / (1024 * 1024)) / best
            row = BenchCommon.Row(
                suite = SUITE, path = label, size = string(tier),
                n_records = n, samples = 3,
                min_s = best, median_s = med,
                alloc_bytes = 0, gc_fraction = 0.0,
                records_per_sec = rps, mb_per_sec = mbps,
            )
            BenchCommon.write_row(row)
            BenchCommon.print_row(row)
        end
    end
end

end # module BenchLiveReader

if abspath(PROGRAM_FILE) == @__FILE__
    BenchLiveReader.run()
end
