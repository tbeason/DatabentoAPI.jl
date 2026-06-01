module BenchLiveReplay

# Replay-stress bench: takes a real .dbn[.zst] archive (typically from
# `batch_download`) and streams it through the live-reader path over a TCP
# loopback mock. The mock writes as fast as the kernel allows; if the reader
# pipeline can't drain that rate, the channel fills up and `put!` blocks —
# which we detect by sampling channel depth.
#
# Use this to characterise live-reader behaviour under firehose loads (OPRA
# CMBP-1 around the open, etc.) without needing a real high-bandwidth network
# or paying for live capture time.

include("fixtures.jl"); using .Fixtures
include("bench_common.jl"); using .BenchCommon

using DatabentoAPI
using DatabentoAPI: read_text_frame, build_text_frame
using DatabentoBinaryEncoding
import DatabentoBinaryEncoding as DBN
using CodecZstd
using TranscodingStreams
using Sockets
using Printf

const SUITE = "live_replay"
const TEST_KEY = "db-1234567890abcdef12345"
const TEST_CHALLENGE = "abcdef0123456789"

# Spawn a mock LSG that streams `bytes` as fast as possible after the
# handshake. Writes in `chunk_bytes`-byte segments; closes immediately after
# the last write so the reader hits EOF without the 2-second hang we
# previously diagnosed.
function spawn_replay(bytes::Vector{UInt8}; chunk_bytes::Int = 4 * 1024 * 1024)
    server = Sockets.listen(Sockets.IPv4("127.0.0.1"), 0)
    port = Sockets.getsockname(server)[2]
    write_started = Ref(0.0)
    write_done = Ref(0.0)
    sent_bytes = Ref(0)
    accept_task = @async begin
        try
            sock = Sockets.accept(server)
            try
                write(sock, build_text_frame(lsg_version = "0.9.0"))
                write(sock, build_text_frame(cram = TEST_CHALLENGE))
                flush(sock)
                read_text_frame(sock)            # auth
                write(sock, build_text_frame(success = "1", session_id = "replay"))
                flush(sock)
                read_text_frame(sock)            # subscribe
                read_text_frame(sock)            # start
                write_started[] = time()
                io = IOBuffer(bytes)
                while !eof(io)
                    n = min(chunk_bytes, length(bytes) - position(io))
                    write(sock, read(io, n))
                    sent_bytes[] += n
                end
                flush(sock)
                write_done[] = time()
            finally
                isopen(sock) && Sockets.close(sock)
            end
        finally
            isopen(server) && Sockets.close(server)
        end
    end
    return (; port = Int(port), accept_task,
              bytes_total = length(bytes),
              sent_bytes, write_started, write_done)
end

# Drive a Live client against the replay mock, tracking consumer rate,
# producer write rate, peak channel depth, and GC pressure.
#
# `reconnect_policy` (`:none` / `:reconnect` / nothing) is splatted into the
# `Live` constructor only when non-`nothing`, so the same harness runs against
# branches whose `Live` predates the kwarg (pass `nothing` there). Under
# `:reconnect` the one-shot mock EOFs and the supervisor keeps retrying a dead
# port, so the channel never closes on its own — the idle watchdog below closes
# the client once `idle_timeout_s` elapses with no new record, and the reported
# rate is computed over the active first→last-record window so the
# reconnect/idle tail does not pollute it.
function replay(; path::AbstractString,
                  wire_zstd::Bool = endswith(lowercase(path), ".zst"),
                  dataset::AbstractString = "OPRA.PILLAR",
                  schema::Schema.T = Schema.CMBP_1,
                  symbols = ["ALL_SYMBOLS"],
                  stype_in::SType.T = SType.RAW_SYMBOL,
                  channel_size::Integer = 65_536,
                  log_every::Int = 1_000_000,
                  typed::Bool = false,
                  reconnect_policy::Union{Nothing,Symbol} = nothing,
                  idle_timeout_s::Real = 3.0)
    bytes = read(path)
    println(SUITE, ": replaying ", basename(path),
            " (", round(length(bytes) / (1024*1024), digits = 1), " MB",
            wire_zstd ? ", wire zstd, expect ~6× expansion" : ", plain",
            typed ? ", TYPED mode" : ", untyped mode",
            reconnect_policy === nothing ? "" : ", reconnect_policy=$(reconnect_policy)", ")")

    mock = spawn_replay(bytes)
    comp = wire_zstd ? Compression.ZSTD : Compression.NONE
    base_kwargs = (; dataset = String(dataset), gateway = "127.0.0.1",
                     port = mock.port, channel_size = Int(channel_size),
                     compression = comp, typed = typed)
    extra_kwargs = reconnect_policy === nothing ? (;) :
                   (; reconnect_policy = reconnect_policy)
    client = DatabentoAPI.Live(TEST_KEY; base_kwargs..., extra_kwargs...)
    DatabentoAPI.connect!(client)
    sub_ret = DatabentoAPI.subscribe!(client;
        schema = schema, symbols = symbols, stype_in = stype_in)
    DatabentoAPI.start!(client)

    # Typed mode: drain the returned Channel{T}. Untyped mode: drain
    # the Union channel on the client.
    ch = typed ? sub_ret : client.channel

    # Idle watchdog (see function docstring). Started before the first take!
    # so a misconfigured run delivering zero records also bails instead of
    # blocking forever.
    t_last = Ref(time_ns())
    idle_ns = UInt64(round(idle_timeout_s * 1e9))
    watchdog_done = Ref(false)
    watchdog = @async begin
        while !watchdog_done[]
            sleep(0.25)
            if (time_ns() - t_last[]) > idle_ns
                try; close(client); catch; end
                break
            end
        end
    end

    first_rec = try
        take!(ch)
    catch e
        watchdog_done[] = true
        try; close(client); catch; end
        rethrow()
    end
    t0 = time_ns()
    t_last[] = t0
    n = 1
    max_depth = 0
    # depth_full_count tracks how often we sampled the channel ≥ 90% full —
    # a proxy for producer-side back-pressure. We sample every `depth_sample_every`
    # records because `Base.n_avail` takes a lock on the channel's condvar; calling
    # it per-record adds ~2 µs per call (lock acquire), tanking consumer rate.
    depth_full_count = 0
    depth_samples = 0
    full_threshold = max(1, Int(channel_size) ÷ 10 * 9)
    depth_sample_every = 1024
    GC.gc()
    g0 = Base.gc_num()
    try
        while true
            try
                _ = take!(ch)
                n += 1
            catch e
                e isa InvalidStateException && break
                rethrow()
            end
            # Refresh the idle-watchdog timestamp and sample channel depth on
            # the same cadence — keeps the per-record hot path identical to the
            # original harness (just take! + counter) so an A/B against an
            # unmodified branch is not skewed by per-record bookkeeping here.
            if (n & (depth_sample_every - 1)) == 0
                t_last[] = time_ns()
                depth = Base.n_avail(ch)
                depth > max_depth && (max_depth = depth)
                depth >= full_threshold && (depth_full_count += 1)
                depth_samples += 1
            end
            if n % log_every == 0
                el = (t_last[] - t0) * 1e-9
                @printf("  %10d records   %.1fs   %.2f M rec/s   max_depth=%d\n",
                        n, el, n / el / 1e6, max_depth)
            end
        end
    finally
        watchdog_done[] = true
        try; close(client); catch; end
        try; wait(mock.accept_task); catch; end
    end
    g1 = Base.gc_num()
    # Active streaming window: first record → last record. Excludes the idle /
    # reconnect-retry tail that elapses before the channel finally closes under
    # reconnect_policy=:reconnect.
    active_s = (t_last[] - t0) * 1e-9
    wall_s = (time_ns() - t0) * 1e-9
    elapsed = active_s > 0 ? active_s : wall_s
    gc_ns = g1.total_time - g0.total_time
    gc_frac = elapsed > 0 ? (gc_ns * 1e-9) / elapsed : 0.0
    alloc_bytes = g1.allocd - g0.allocd
    alloc_mb = alloc_bytes / (1024 * 1024)
    write_dur = mock.write_done[] - mock.write_started[]
    write_mbps = write_dur > 0 ? mock.bytes_total / (1024*1024) / write_dur : 0.0

    println()
    println("=== ", SUITE, " summary ===")
    @printf("  file              %s (%.1f MB on wire)\n",
            basename(path), mock.bytes_total / (1024*1024))
    @printf("  reconnect_policy  %s\n",
            reconnect_policy === nothing ? "(default)" : string(reconnect_policy))
    @printf("  records           %d\n", n)
    @printf("  consumer drain    %.3f s active   %.2f M rec/s (active window)   %.3f s wall\n",
            active_s, n / elapsed / 1e6, wall_s)
    @printf("  producer write    %.3f s   %.1f MB/s on wire (bottlenecked by reader if any backpressure)\n",
            write_dur, write_mbps)
    @printf("  channel           cap=%d  peak=%d (%.1f%%)  near-full %d/%d samples (%.1f%%)\n",
            channel_size, max_depth, 100 * max_depth / channel_size,
            depth_full_count, depth_samples,
            depth_samples > 0 ? 100 * depth_full_count / depth_samples : 0.0)
    @printf("  GC                fraction=%.1f%%  alloc=%.1f MB (%.1f bytes/record)\n",
            gc_frac * 100, alloc_mb, alloc_bytes / n)
    println()
    if depth_full_count > 100 && max_depth >= full_threshold
        @info "channel saturated repeatedly during replay" depth_full_count max_depth
        @info "interpretation: consumer side (take! loop) was slower than the reader+decoder side at times. If this matches a real wire rate you'd see in production, F1b (typed channel) becomes load-bearing."
    elseif max_depth < channel_size ÷ 4
        @info "channel stayed well below capacity" max_depth=max_depth channel_capacity=channel_size
        @info "interpretation: reader+decoder kept up easily — consumer was the gate, which is the bench artefact (take! in a tight loop). To stress the decoder further, increase the wire rate (run multiple replays in parallel) or use a larger / more bursty archive."
    end

    return (; records = n, drain_s = elapsed, active_s = active_s, wall_s = wall_s,
              rate_per_s = n / elapsed,
              max_depth = max_depth, depth_full_count = depth_full_count,
              gc_frac = gc_frac, alloc_bytes = alloc_bytes,
              bytes_per_record = alloc_bytes / n,
              reconnect_policy = reconnect_policy,
              write_dur = write_dur, write_mbps = write_mbps)
end

function run(; path::Union{Nothing,AbstractString} = nothing, kwargs...)
    if path === nothing
        path = get(ENV, "DATABENTO_REPLAY_PATH", nothing)
        path === nothing && error("Pass path=<.dbn.zst path> or set DATABENTO_REPLAY_PATH. " *
                                  "Use benchmark/_fetch_replay_fixture.jl to grab one from Databento.")
    end
    isfile(path) || error("Replay fixture not found: $path")
    return replay(; path = path, kwargs...)
end

end # module BenchLiveReplay

if abspath(PROGRAM_FILE) == @__FILE__
    BenchLiveReplay.run()
end
