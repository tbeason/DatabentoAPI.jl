module BenchTCPThroughput

# Diagnostic bench for the Windows-TCP-loopback ceiling reported in
# benchmark/PERF_REPORT.md (`bench_live_reader` shows 50 k rec/s vs
# in-process 8–10 M rec/s).
#
# Three independent sweeps, all over a localhost TCP loopback:
#   1. Raw read-chunk-size sweep — strip everything except `readbytes!`.
#      Tells us whether libuv has per-call overhead and what chunk size
#      Windows' loopback prefers.
#   2. Server-side write-chunk-size sweep — same idea, varying the other
#      side. If small server writes throttle throughput, the live reader's
#      mock-gateway pacing (1 MB chunks) may not be the bottleneck.
#   3. Through-BufferedReader sweep — measures the actual stack the live
#      reader uses (TCPSocket → CountingIO → BufferedReader), varying the
#      BufferedReader buffer size. Tells us whether bumping the default 64 KB
#      buffer would lift the live ceiling.

using Sockets
using DBN
using DatabentoAPI: CountingIO
using Printf

const SUITE = "tcp_throughput"

# Server: write `total_bytes` to the first accepted client in `chunk_size`-byte
# writes (flushed after each).
function spawn_writer(total_bytes::Int, chunk_size::Int)
    server = Sockets.listen(Sockets.IPv4("127.0.0.1"), 0)
    port = Sockets.getsockname(server)[2]
    accept_task = @async begin
        try
            sock = Sockets.accept(server)
            try
                buf = zeros(UInt8, chunk_size)
                sent = 0
                while sent < total_bytes
                    n = min(chunk_size, total_bytes - sent)
                    Sockets.write(sock, view(buf, 1:n))
                    sent += n
                end
                flush(sock)
                # Hold briefly so client can finish its drain before close.
                sleep(0.1)
            finally
                isopen(sock) && Sockets.close(sock)
            end
        finally
            isopen(server) && Sockets.close(server)
        end
    end
    return (port = Int(port), accept_task = accept_task)
end

# Client: read `total_bytes` using `readbytes!` of `read_chunk` bytes per call.
function drain_socket(sock, total_bytes::Int, read_chunk::Int)
    buf = Vector{UInt8}(undef, read_chunk)
    got = 0
    while got < total_bytes
        want = min(read_chunk, total_bytes - got)
        n = readbytes!(sock, buf, want)
        n == 0 && break
        got += n
    end
    return got
end

# Time `samples` iterations of a single TCP transfer; return min/median wall.
function bench_raw(total_bytes::Int, server_chunk::Int, read_chunk::Int;
                   samples::Int = 3)
    times = Float64[]
    for _ in 1:samples
        server = spawn_writer(total_bytes, server_chunk)
        sock = Sockets.connect("127.0.0.1", server.port)
        t0 = time_ns()
        got = drain_socket(sock, total_bytes, read_chunk)
        elapsed = (time_ns() - t0) * 1e-9
        Sockets.close(sock)
        try; wait(server.accept_task); catch; end
        got == total_bytes || @warn "short read" got total_bytes
        push!(times, elapsed)
    end
    sort!(times)
    return minimum(times), times[cld(length(times), 2)]
end

# Drain through the same stack the live reader uses:
#   TCPSocket → CountingIO → BufferedReader → readbytes!
# Reads in `read_chunk` bytes per call from the BufferedReader (so the
# BufferedReader is doing the actual socket reads).
function drain_buffered(buffered, total_bytes::Int, read_chunk::Int)
    buf = Vector{UInt8}(undef, read_chunk)
    got = 0
    while got < total_bytes
        want = min(read_chunk, total_bytes - got)
        n = readbytes!(buffered, buf, want)
        n == 0 && break
        got += n
    end
    return got
end

function bench_buffered(total_bytes::Int, server_chunk::Int,
                        buffer_size::Int, read_chunk::Int;
                        samples::Int = 3)
    times = Float64[]
    for _ in 1:samples
        server = spawn_writer(total_bytes, server_chunk)
        sock = Sockets.connect("127.0.0.1", server.port)
        counting = CountingIO(sock)
        buffered = DBN.BufferedReader(counting, buffer_size)
        t0 = time_ns()
        got = drain_buffered(buffered, total_bytes, read_chunk)
        elapsed = (time_ns() - t0) * 1e-9
        Sockets.close(sock)
        try; wait(server.accept_task); catch; end
        got == total_bytes || @warn "short read" got total_bytes
        push!(times, elapsed)
    end
    sort!(times)
    return minimum(times), times[cld(length(times), 2)]
end

# Bench 1: vary the client-side read chunk size (server fixed at 1 MB writes).
function bench_raw_read_sweep(; total_mb::Int = 100)
    total_bytes = total_mb * 1024 * 1024
    server_chunk = 1024 * 1024
    println("\n=== ", SUITE, " — raw TCP loopback (server writes 1 MB chunks of ",
            total_mb, " MB) ===")
    println("  vary client `readbytes!` request size:")
    for read_chunk in (4 * 1024, 16 * 1024, 64 * 1024, 256 * 1024,
                       1024 * 1024, 4 * 1024 * 1024)
        min_t, med_t = bench_raw(total_bytes, server_chunk, read_chunk; samples = 3)
        @printf("    read_chunk=%5d KB  min=%6.3f s  med=%6.3f s  %8.1f MB/s\n",
                read_chunk ÷ 1024, min_t, med_t, total_mb / min_t)
    end
end

# Bench 2: vary the server-side write chunk size (client fixed at 1 MB reads).
function bench_raw_write_sweep(; total_mb::Int = 100)
    total_bytes = total_mb * 1024 * 1024
    read_chunk = 1024 * 1024
    println("\n=== ", SUITE, " — raw TCP loopback (client reads 1 MB; vary server writes) ===")
    for server_chunk in (4 * 1024, 16 * 1024, 64 * 1024, 256 * 1024,
                         1024 * 1024, 4 * 1024 * 1024)
        min_t, med_t = bench_raw(total_bytes, server_chunk, read_chunk; samples = 3)
        @printf("    server_chunk=%5d KB  min=%6.3f s  med=%6.3f s  %8.1f MB/s\n",
                server_chunk ÷ 1024, min_t, med_t, total_mb / min_t)
    end
end

# Bench 3: through CountingIO + BufferedReader, varying the buffer size.
function bench_buffered_sweep(; total_mb::Int = 100)
    total_bytes = total_mb * 1024 * 1024
    server_chunk = 1024 * 1024
    read_chunk = 64 * 1024
    println("\n=== ", SUITE, " — through CountingIO + BufferedReader",
            " (server 1 MB writes; reads of 64 KB from BufferedReader) ===")
    println("  vary BufferedReader internal buffer size:")
    for buffer_size in (16 * 1024, 64 * 1024, 256 * 1024,
                        1024 * 1024, 4 * 1024 * 1024)
        min_t, med_t = bench_buffered(total_bytes, server_chunk, buffer_size,
                                      read_chunk; samples = 3)
        @printf("    buffer=%5d KB  min=%6.3f s  med=%6.3f s  %8.1f MB/s\n",
                buffer_size ÷ 1024, min_t, med_t, total_mb / min_t)
    end
end

# A reference number: just the live-reader fixture sized comparison.
# 100 k TradeMsg ≈ 4.8 MB. If raw TCP can do >>2.4 MB/s, the ceiling
# isn't TCP itself.
function bench_live_fixture_size(; total_mb::Float64 = 4.8)
    total_bytes = round(Int, total_mb * 1024 * 1024)
    println("\n=== ", SUITE, " — live-reader fixture size reference (",
            round(total_mb, digits = 2), " MB) ===")
    println("  current `bench_live_reader.jl` baseline: 100 k records / 2.01 s ≈ 2.4 MB/s")
    for (server_chunk, read_chunk) in (
        (1024 * 1024,    64 * 1024),    # matches live reader BufferedReader default
        (1024 * 1024,    1024 * 1024),  # matches live reader's mock server chunks
        (16 * 1024,      64 * 1024),    # small chunks both sides
    )
        min_t, med_t = bench_raw(total_bytes, server_chunk, read_chunk; samples = 5)
        @printf("    server=%4d KB  read=%4d KB  min=%6.3f s  %7.1f MB/s\n",
                server_chunk ÷ 1024, read_chunk ÷ 1024, min_t, total_mb / min_t)
    end
end

function run(; total_mb::Int = 100)
    println("Configuration: ", total_mb, " MB total per transfer, 3 samples min/median")
    bench_raw_read_sweep(total_mb = total_mb)
    bench_raw_write_sweep(total_mb = total_mb)
    bench_buffered_sweep(total_mb = total_mb)
    bench_live_fixture_size()
end

end # module BenchTCPThroughput

if abspath(PROGRAM_FILE) == @__FILE__
    BenchTCPThroughput.run()
end
