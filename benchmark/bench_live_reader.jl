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
                # NOTE: we do NOT sleep here. An earlier version held the
                # connection open for 2 s after the payload, which made the
                # reader's final readbytes!(sock, buf, 64KB) call block for
                # that full 2 s waiting for EOF (it had asked for more bytes
                # than the stream contained). Closing immediately lets the
                # reader see EOF promptly and exit. See PERF_REPORT R3
                # follow-up for the diagnosis.
            finally
                isopen(sock) && Sockets.close(sock)
            end
        finally
            isopen(server) && Sockets.close(server)
        end
    end
    return (; port = Int(port), accept_task)
end

# Returns the wall-clock duration (s) spent just consuming records — the
# spawn/connect/handshake and mock-close are excluded so the throughput number
# reflects the reader/decode loop alone.
function _drain_live_timed(bytes; wire_zstd::Bool, expected::Int, typed::Bool = false)
    mock = _spawn(bytes; wire_zstd = wire_zstd)
    comp = wire_zstd ? Compression.ZSTD : Compression.NONE
    client = DatabentoAPI.Live(TEST_KEY;
        dataset = "TEST.MOCK",
        gateway = "127.0.0.1",
        port = mock.port,
        channel_size = 16_384,
        compression = comp,
        typed = typed,
    )
    DatabentoAPI.connect!(client)
    sub_ret = DatabentoAPI.subscribe!(client;
        schema = Schema.TRADES, symbols = ["AAPL"],
        stype_in = SType.RAW_SYMBOL)
    # In typed mode subscribe! returns Channel{TradeMsg}; in untyped mode it
    # returns an Int — we drain client.channel in that case.
    ch = typed ? sub_ret : client.channel
    DatabentoAPI.start!(client)
    # Block until the first record arrives — this excludes server-side
    # handshake/network setup from the measured window.
    first = take!(ch)
    n = 1
    GC.gc()
    g0 = Base.gc_num()
    t0 = time_ns()
    deadline = time() + 60.0
    try
        while n < expected && time() < deadline
            try
                _ = take!(ch)
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
    g1 = Base.gc_num()
    alloc_b = Int(g1.allocd - g0.allocd)
    gc_frac = elapsed_s > 0 ? (g1.total_time - g0.total_time) * 1e-9 / elapsed_s : 0.0
    try; close(client); catch; end
    try; wait(mock.accept_task); catch; end
    return n, elapsed_s, alloc_b, gc_frac
end

function _bench_drain(bytes, expected, wire_zstd; samples::Int, typed::Bool)
    # Warmup
    _drain_live_timed(bytes; wire_zstd = wire_zstd, expected = expected, typed = typed)
    best = Inf; times = Float64[]
    best_alloc = 0; best_gc = 0.0
    for _ in 1:samples
        _, t, a, g = _drain_live_timed(bytes; wire_zstd = wire_zstd,
                                       expected = expected, typed = typed)
        push!(times, t)
        if t < best
            best = t; best_alloc = a; best_gc = g
        end
    end
    sort!(times)
    med = times[cld(length(times), 2)]
    return best, med, best_alloc, best_gc
end

function run(; tiers = (:small, :medium), typed_too::Bool = true)
    println("\n=== ", SUITE, " ===")
    modes = typed_too ? ((false, ""), (true, "_typed")) : ((false, ""),)
    for tier in tiers
        file = Fixtures.trades_file(tier; zst = false)
        bytes = Fixtures.cached_trades_bytes(tier; zst = false)
        n = Fixtures.record_count(file)
        for (typed, suffix) in modes
            for wire_zstd in (false, true)
                label = string(wire_zstd ? "live_zstd" : "live_plain", suffix)
                best, med, alloc_b, gc_f = _bench_drain(bytes, n, wire_zstd;
                                                       samples = 3, typed = typed)
                rps = n / best
                mbps = (length(bytes) / (1024 * 1024)) / best
                row = BenchCommon.Row(
                    suite = SUITE, path = label, size = string(tier),
                    n_records = n, samples = 3,
                    min_s = best, median_s = med,
                    alloc_bytes = alloc_b, gc_fraction = gc_f,
                    records_per_sec = rps, mb_per_sec = mbps,
                )
                BenchCommon.write_row(row)
                BenchCommon.print_row(row)
            end
        end
    end
end

end # module BenchLiveReader

if abspath(PROGRAM_FILE) == @__FILE__
    BenchLiveReader.run()
end
