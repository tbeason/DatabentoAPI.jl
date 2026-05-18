module BenchStreamToFile

include("fixtures.jl"); using .Fixtures
include("bench_common.jl"); using .BenchCommon

using DatabentoAPI
using DatabentoAPI: read_text_frame, build_text_frame, cram_response
using DBN
using CodecZstd
using TranscodingStreams
using Sockets

const SUITE = "stream_to_file"
const TEST_KEY = "db-1234567890abcdef12345"
const TEST_CHALLENGE = "abcdef0123456789"

# Spawn a mock LSG that streams the payload and then immediately closes its
# socket so `stream_to_file` exits its reader loop. We close after a small
# settle delay so the encoder finishes draining.
function _spawn(bytes; wire_zstd::Bool = false)
    payload = wire_zstd ? transcode(ZstdCompressor, bytes) : bytes
    server = Sockets.listen(Sockets.IPv4("127.0.0.1"), 0)
    port = Sockets.getsockname(server)[2]
    accept_task = @async begin
        try
            sock = Sockets.accept(server)
            try
                write(sock, build_text_frame(lsg_version = "0.9.0"))
                write(sock, build_text_frame(cram = TEST_CHALLENGE))
                flush(sock)
                read_text_frame(sock)
                write(sock, build_text_frame(success = "1", session_id = "bench"))
                flush(sock)
                read_text_frame(sock)  # subscribe
                read_text_frame(sock)  # start
                chunk = 1024 * 1024
                io = IOBuffer(payload)
                while !eof(io)
                    write(sock, read(io, chunk))
                    flush(sock)
                end
                # Close immediately (no sleep). See bench_live_reader.jl note —
                # holding the connection open after payload makes the reader's
                # final readbytes! call block for the full hold duration
                # waiting for EOF.
            finally
                isopen(sock) && Sockets.close(sock)
            end
        finally
            isopen(server) && Sockets.close(server)
        end
    end
    return (; port = Int(port), accept_task)
end

function _stream_once(bytes; wire_zstd::Bool, compress_out::Bool, level::Int = 3)
    mock = _spawn(bytes; wire_zstd = wire_zstd)
    base_dir = mktempdir()
    path = ""
    try
        path = DatabentoAPI.stream_to_file(;
            dataset = "TEST.MOCK",
            schema  = Schema.TRADES,
            symbols = ["AAPL"],
            stype_in = SType.RAW_SYMBOL,
            base_dir = base_dir,
            duration_s = 6.0,                  # bounded so the bench cannot hang
            compress = compress_out,
            compress_level = level,
            key = TEST_KEY,
            gateway = "127.0.0.1",
            port = mock.port,
            wire_compression = wire_zstd ? Compression.ZSTD : Compression.NONE,
            heartbeat_log_interval_s = 60.0,
            reconnect = false,
            channel_size = 16_384,
        )
        try; wait(mock.accept_task); catch; end
        return filesize(path)
    finally
        try; rm(base_dir; force = true, recursive = true); catch; end
    end
end

function run(; tiers = (:small,))
    println("\n=== ", SUITE, " ===")
    for tier in tiers
        file = Fixtures.trades_file(tier; zst = false)
        bytes = Fixtures.cached_trades_bytes(tier; zst = false)
        n = Fixtures.record_count(file)
        for wire_zstd in (false, true)
            for compress_out in (false, true)
                label = string(
                    wire_zstd ? "wirezstd" : "wireplain",
                    "_",
                    compress_out ? "filezstd" : "fileplain",
                )
                BenchCommon.manual_to_row(
                    suite = SUITE,
                    path = label,
                    size = string(tier),
                    n_records = n,
                    bytes_in = length(bytes),
                    samples = 2,
                    f = () -> _stream_once(bytes;
                        wire_zstd = wire_zstd, compress_out = compress_out),
                )
            end
        end
    end
end

end # module BenchStreamToFile

if abspath(PROGRAM_FILE) == @__FILE__
    BenchStreamToFile.run()
end
