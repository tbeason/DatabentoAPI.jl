using Test
using DatabentoAPI
using DatabentoAPI: read_text_frame, build_text_frame
using DatabentoBinaryEncoding
import DatabentoBinaryEncoding as DBN
using Sockets

const TEST_API_KEY_2 = "db-aaaaaaaaaaaaaaa12345"

# Minimal handshake that ignores subscription details and immediately streams a tiny
# binary payload built in-process via DBN.write_dbn.
function _make_handshake_with(bytes)
    return function(sock)
        write(sock, build_text_frame(lsg_version = "0.9.0"))
        write(sock, build_text_frame(cram = "challenge"))
        flush(sock)
        read_text_frame(sock)  # consume auth
        write(sock, build_text_frame(success = "1", session_id = "s1"))
        flush(sock)
        read_text_frame(sock)  # consume sub
        read_text_frame(sock)  # consume start
        write(sock, bytes)
        flush(sock)
        sleep(0.2)
    end
end

function _build_tiny_payload()
    metadata = DBN.Metadata(
        DBN.DBN_VERSION, "GLBX.MDP3", DBN.Schema.TRADES,
        Int64(0), nothing, nothing, nothing,
        DBN.SType.INSTRUMENT_ID, false,
        ["ES.FUT"], String[], String[],
        Tuple{String,String,Int64,Int64}[],
    )
    hd = DBN.RecordHeader(UInt8(0), DBN.RType.MBP_0_MSG,
                          UInt16(1), UInt32(1), Int64(1_700_000_000_000_000_000))
    rec = DBN.TradeMsg(hd, Int64(4500_000_000_000), UInt32(1),
                       DBN.Action.TRADE, DBN.Side.BID,
                       UInt8(0), UInt8(1),
                       Int64(1_700_000_000_000_000_000), Int32(0), UInt32(1))
    tmp, io = mktemp(); close(io)
    try
        DBN.write_dbn(tmp, metadata, DBN.DBNRecord[rec])
        return read(tmp)
    finally
        rm(tmp; force = true)
    end
end

@testset "live subscribe + iteration" begin
    @testset "snapshot+start mutually exclusive" begin
        # No socket needed for arg validation; build a half-initialized client.
        bytes = _build_tiny_payload()
        mock = spawn_mock_gateway(_make_handshake_with(bytes))
        client = Live(TEST_API_KEY_2;
            dataset = "GLBX.MDP3", gateway = "127.0.0.1", port = mock.port,
            reconnect_policy = :none)
        connect!(client)
        @test_throws ArgumentError subscribe!(client;
            schema = Schema.TRADES, symbols = "ES.FUT",
            snapshot = true, start = 1)
        close(client)
        try wait(mock.accept_task) catch end
    end

    @testset "iteration via for-loop" begin
        bytes = _build_tiny_payload()
        mock = spawn_mock_gateway(_make_handshake_with(bytes))
        client = Live(TEST_API_KEY_2;
            dataset = "GLBX.MDP3", gateway = "127.0.0.1", port = mock.port,
            channel_size = 8, reconnect_policy = :none)
        connect!(client)
        subscribe!(client; schema = Schema.TRADES, symbols = "ES.FUT",
                   stype_in = SType.PARENT)
        start!(client)
        got = DBN.DBNRecord[]
        deadline = time() + 5.0
        while time() < deadline
            try
                push!(got, take!(client.channel))
            catch e
                e isa InvalidStateException && break
                rethrow()
            end
            length(got) >= 1 && break
        end
        @test length(got) == 1
        @test got[1] isa DBN.TradeMsg
        close(client)
        try wait(mock.accept_task) catch end
    end

    @testset "subscribe_callback delivers records" begin
        bytes = _build_tiny_payload()
        mock = spawn_mock_gateway(_make_handshake_with(bytes))
        client = Live(TEST_API_KEY_2;
            dataset = "GLBX.MDP3", gateway = "127.0.0.1", port = mock.port,
            channel_size = 8, reconnect_policy = :none)
        connect!(client)
        subscribe!(client; schema = Schema.TRADES, symbols = "ES.FUT")
        start!(client)
        seen = DBN.DBNRecord[]
        cb_task = subscribe_callback(client, rec -> push!(seen, rec))
        deadline = time() + 5.0
        while time() < deadline && isempty(seen)
            sleep(0.05)
        end
        @test length(seen) == 1
        close(client)
        try wait(cb_task) catch end
        try wait(mock.accept_task) catch end
    end
end
