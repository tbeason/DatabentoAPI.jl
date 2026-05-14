using Test
using DatabentoAPI
using DatabentoAPI: read_text_frame, build_text_frame, cram_response
using DBN
using Sockets

# Encode a tiny DBN payload (header + 3 trades) as raw bytes.
function _encoded_dbn_bytes()
    metadata = DBN.Metadata(
        DBN.DBN_VERSION, "GLBX.MDP3", DBN.Schema.TRADES,
        Int64(0), nothing, nothing, nothing,
        DBN.SType.INSTRUMENT_ID, false,
        ["ES.FUT"], String[], String[],
        Tuple{String,String,Int64,Int64}[],
    )
    records = DBN.DBNRecord[]
    for i in 1:3
        hd = DBN.RecordHeader(
            UInt8(0), DBN.RType.MBP_0_MSG,
            UInt16(1), UInt32(200), Int64(1_700_000_000_000_000_000 + i * 1_000_000),
        )
        push!(records, DBN.TradeMsg(
            hd, Int64(4500_000_000_000 + i * 250_000), UInt32(2),
            DBN.Action.TRADE, DBN.Side.BID,
            UInt8(0), UInt8(1),
            Int64(1_700_000_000_000_000_000 + i * 1_000_000), Int32(0), UInt32(i)))
    end
    tmp, io = mktemp(); close(io)
    try
        DBN.write_dbn(tmp, metadata, records)
        return read(tmp), length(records)
    finally
        rm(tmp; force = true)
    end
end

const TEST_API_KEY = "db-1234567890abcdef12345"
const TEST_CHALLENGE = "abcdef0123456789"

@testset "live reader (mock TCP gateway)" begin
    bytes, n = _encoded_dbn_bytes()

    handshake = function(sock)
        write(sock, build_text_frame(lsg_version = "0.9.0"))
        write(sock, build_text_frame(cram = TEST_CHALLENGE))
        flush(sock)
        auth_frame = read_text_frame(sock)
        # Verify CRAM response matches what the client computed.
        @test auth_frame["auth"] == cram_response(TEST_CHALLENGE, TEST_API_KEY)
        @test auth_frame["dataset"] == "GLBX.MDP3"
        write(sock, build_text_frame(success = "1", session_id = "sess-1"))
        flush(sock)
        # Expect one subscription frame
        sub_frame = read_text_frame(sock)
        @test sub_frame["schema"] == "trades"
        @test sub_frame["symbols"] == "ES.FUT"
        # Expect a session start frame
        start_frame = read_text_frame(sock)
        @test haskey(start_frame, "start_session")
        # Now stream the binary DBN
        write(sock, bytes)
        flush(sock)
        # Hold the connection briefly so the client has time to drain
        sleep(0.2)
    end

    mock = spawn_mock_gateway(handshake)

    client = Live(TEST_API_KEY;
        dataset = "GLBX.MDP3",
        gateway = "127.0.0.1",
        port    = mock.port,
        channel_size = 16,
    )
    connect!(client)
    @test client.connected
    @test client.session_id == "sess-1"

    subscribe!(client; schema = Schema.TRADES, symbols = ["ES.FUT"], stype_in = SType.PARENT)
    start!(client)

    received = DBN.DBNRecord[]
    deadline = time() + 5.0
    while length(received) < n && time() < deadline
        try
            push!(received, take!(client.channel))
        catch e
            e isa InvalidStateException && break
            rethrow()
        end
    end

    @test length(received) == n
    @test all(r -> r isa DBN.TradeMsg, received)

    close(client)
    wait(mock.accept_task)
end
