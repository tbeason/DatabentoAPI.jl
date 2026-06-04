using Test
using DatabentoAPI
using DatabentoAPI: read_text_frame, build_text_frame, LiveReader
using DatabentoBinaryEncoding
import DatabentoBinaryEncoding as DBN
using Sockets

# ---------------------------------------------------------------------------
# LiveReader — low-latency socket reader unit tests
#
# These drive LiveReader over a Base.BufferStream (an in-memory stream whose
# `readavailable` blocks until data is written, exactly like a socket). The
# key property under test is that a record is decodable the instant its bytes
# are present — LiveReader must NEVER wait to fill a large buffer. Any such
# test that read more than necessary would hang here rather than fail.
# ---------------------------------------------------------------------------

# Native-endian byte encoding of a scalar (matches LiveReader's unsafe_load).
_bytes_of(x) = (b = IOBuffer(); write(b, x); take!(b))

@testset "LiveReader — typed scalar reads" begin
    bs = Base.BufferStream()
    r = LiveReader(bs)
    vals = (Int64(-123456789), UInt32(0xdeadbeef), UInt8(0x42), Int16(-7))
    for v in vals
        write(bs, _bytes_of(v))
    end
    flush(bs)
    @test read(r, Int64)  == vals[1]
    @test read(r, UInt32) == vals[2]
    @test read(r, UInt8)  == vals[3]
    @test read(r, Int16)  == vals[4]
    @test position(r) == sizeof(Int64) + sizeof(UInt32) + sizeof(UInt8) + sizeof(Int16)
    close(bs)
end

@testset "LiveReader — surfaces a record without waiting for more" begin
    # Write exactly one Int64's worth of bytes and read it back. If LiveReader
    # tried to accumulate a chunk before returning, this read would block and
    # the test would hang — completing proves it returns as soon as the bytes
    # are available.
    bs = Base.BufferStream()
    r = LiveReader(bs)
    v = Int64(0x0102030405060708)
    write(bs, _bytes_of(v)); flush(bs)
    @test read(r, Int64) == v
    close(bs)
end

@testset "LiveReader — reassembles a value split across reads" begin
    bs = Base.BufferStream()
    r = LiveReader(bs)
    v = Int64(0x1122334455667788)
    bytes = _bytes_of(v)
    write(bs, bytes[1:3]); flush(bs)            # partial — not enough for an Int64
    writer = @async begin
        sleep(0.05)
        write(bs, bytes[4:end]); flush(bs)      # remainder arrives later
    end
    @test read(r, Int64) == v                   # blocks only until the rest lands
    wait(writer)
    close(bs)
end

@testset "LiveReader — read(n), skip, eof" begin
    bs = Base.BufferStream()
    r = LiveReader(bs)
    write(bs, UInt8[1, 2, 3, 4, 5, 6, 7, 8]); flush(bs)
    @test read(r, 2) == UInt8[1, 2]
    skip(r, 3)
    @test read(r, UInt8) == 0x06
    @test position(r) == 6
    close(bs)                                    # no more bytes after the buffered ones
    @test read(r, 2) == UInt8[7, 8]
    @test eof(r) == true
end

# ---------------------------------------------------------------------------
# Mock gateway helpers (self-contained; mirrors test_live_reader_typed.jl)
# ---------------------------------------------------------------------------

const _TFIX_KEY = "db-1234567890abcdef12345"
const _TFIX_CHALLENGE = "abcdef0123456789"

function _spawn_fix_mock(bytes::Vector{UInt8})
    server = Sockets.listen(Sockets.IPv4("127.0.0.1"), 0)
    port = Sockets.getsockname(server)[2]
    accept_task = @async begin
        try
            sock = Sockets.accept(server)
            try
                write(sock, build_text_frame(lsg_version = "0.9.0"))
                write(sock, build_text_frame(cram = _TFIX_CHALLENGE))
                flush(sock)
                read_text_frame(sock)   # auth
                write(sock, build_text_frame(success = "1", session_id = "fix-test"))
                flush(sock)
                while true
                    frame = read_text_frame(sock)
                    haskey(frame, "start_session") && break
                end
                write(sock, bytes); flush(sock)
            finally
                isopen(sock) && Sockets.close(sock)
            end
        finally
            isopen(server) && Sockets.close(server)
        end
    end
    return (; port = Int(port), accept_task)
end

# Build a DBN payload: `n_sys` SystemMsg (control) followed by `n_trades`
# TradeMsg (data). SystemMsg is used as the control flood because its encode
# path is simple and exercised elsewhere.
function _flood_payload(n_sys::Int, n_trades::Int)
    md = DBN.Metadata(
        DBN.DBN_VERSION, "TEST.MOCK", DBN.Schema.TRADES,
        Int64(0), nothing, nothing, nothing,
        DBN.SType.INSTRUMENT_ID, false,
        ["AAPL"], String[], String[],
        Tuple{String,String,Int64,Int64}[],
    )
    ts0 = Int64(1_700_000_000_000_000_000)
    recs = DBN.DBNRecord[]
    sys_text = "ready"; sys_code = "0"
    sys_body = length(sys_text) + 1 + length(sys_code) + 1
    sys_units = UInt8(((16 + sys_body + 3) ÷ 4))
    for i in 1:n_sys
        push!(recs, DBN.SystemMsg(
            DBN.RecordHeader(sys_units, DBN.RType.SYSTEM_MSG,
                             UInt16(0), UInt32(0), ts0 + i),
            sys_text, sys_code))
    end
    for i in 1:n_trades
        hd = DBN.RecordHeader(UInt8(0), DBN.RType.MBP_0_MSG,
                              UInt16(1), UInt32(100 + i), ts0 + i)
        push!(recs, DBN.TradeMsg(hd, Int64(150_000_000_000 + i),
                                 UInt32(100), DBN.Action.TRADE, DBN.Side.ASK,
                                 UInt8(0), UInt8(1), ts0 + i, Int32(0), UInt32(i)))
    end
    tmp, io = mktemp(); close(io)
    try
        DBN.write_dbn(tmp, md, recs)
        return read(tmp)
    finally
        rm(tmp; force = true)
    end
end

@testset "control_channel_size sizes the control channel independently" begin
    c1 = DatabentoAPI.Live(_TFIX_KEY;
        dataset = "TEST.MOCK", gateway = "127.0.0.1", port = 1,
        channel_size = 64, control_channel_size = 7,
        typed = true, reconnect_policy = :none)
    @test DatabentoAPI.control_channel(c1).sz_max == 7
    c1.closed = true

    # Default: control channel inherits channel_size.
    c2 = DatabentoAPI.Live(_TFIX_KEY;
        dataset = "TEST.MOCK", gateway = "127.0.0.1", port = 1,
        channel_size = 33, typed = true, reconnect_policy = :none)
    @test DatabentoAPI.control_channel(c2).sz_max == 33
    c2.closed = true
end

@testset "undrained control_channel does not starve data" begin
    # 20 control records into a control channel sized 4, never drained, then
    # 5 trades. The blocking-put bug would wedge the reader on control record
    # #5 and deliver zero trades; the non-blocking put must drop the overflow
    # (16) and still deliver all 5 trades.
    n_sys, n_trades, ctrl_size = 20, 5, 4
    bytes = _flood_payload(n_sys, n_trades)
    mock = _spawn_fix_mock(bytes)

    client = DatabentoAPI.Live(_TFIX_KEY;
        dataset = "TEST.MOCK", gateway = "127.0.0.1", port = mock.port,
        channel_size = 64, control_channel_size = ctrl_size,
        typed = true, reconnect_policy = :none)
    DatabentoAPI.connect!(client)
    ch = DatabentoAPI.subscribe!(client;
        schema = Schema.TRADES, symbols = ["AAPL"], stype_in = SType.RAW_SYMBOL)
    DatabentoAPI.start!(client)

    # Drain ONLY the data channel — control channel is deliberately left full.
    trades = DBN.TradeMsg[]
    consumer = @async begin
        try
            while true
                push!(trades, take!(ch))
            end
        catch e
            e isa InvalidStateException || rethrow()
        end
    end

    try; wait(mock.accept_task); catch; end
    try; wait(consumer); catch; end

    @test length(trades) == n_trades                 # data not starved
    @test client.dropped_control == n_sys - ctrl_size  # overflow dropped, counted

    close(client)
end
