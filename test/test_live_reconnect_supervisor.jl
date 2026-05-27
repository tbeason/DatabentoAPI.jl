using Test
using DatabentoAPI
using DatabentoAPI: read_text_frame, build_text_frame, _gap_start_ns
using DatabentoBinaryEncoding
import DatabentoBinaryEncoding as DBN
using Sockets

const _SUPERVISOR_TEST_KEY = "db-0123456789abcdef0123456789abcdef"

# Build a DBN stream containing `n` trade records, with each record offset
# from `ts_base` (ns) so two sequences can be checked for non-overlap.
function _supervisor_dbn_bytes(n::Int; ts_base::Int64 = Int64(1_700_000_000_000_000_000),
                                       id_base::Int = 100, dataset = "TEST.MOCK")
    metadata = DBN.Metadata(
        DBN.DBN_VERSION, dataset, DBN.Schema.TRADES,
        ts_base, nothing, nothing, nothing,
        DBN.SType.INSTRUMENT_ID, false,
        ["AAPL"], String[], String[],
        Tuple{String,String,Int64,Int64}[],
    )
    records = DBN.DBNRecord[]
    for i in 1:n
        hd = DBN.RecordHeader(UInt8(0), DBN.RType.MBP_0_MSG,
                              UInt16(1), UInt32(id_base + i),
                              ts_base + Int64(i) * Int64(1_000_000))
        push!(records, DBN.TradeMsg(
            hd, Int64(150_000_000_000 + i * 1_000_000), UInt32(100),
            DBN.Action.TRADE, DBN.Side.ASK, UInt8(0), UInt8(1),
            ts_base + Int64(i) * Int64(1_000_000), Int32(0), UInt32(i),
        ))
    end
    tmp, io = mktemp(); close(io)
    try
        DBN.write_dbn(tmp, metadata, records)
        return read(tmp), n
    finally
        rm(tmp; force = true)
    end
end

# Server-side handshake matching what `connect!` expects from the gateway.
function _supervisor_handshake!(sock)
    write(sock, build_text_frame(lsg_version = "0.9.0"))
    write(sock, build_text_frame(cram = "challenge"))
    flush(sock)
    _ = read_text_frame(sock)                                # auth
    write(sock, build_text_frame(success = "1", session_id = "sess"))
    flush(sock)
    _ = read_text_frame(sock)                                # subscribe
    _ = read_text_frame(sock)                                # start_session
end

@testset "live reconnect supervisor — iteration survives a disconnect" begin
    bytes1, n1 = _supervisor_dbn_bytes(3; ts_base = Int64(1_700_000_000_000_000_000),
                                          id_base = 100)
    # Second session uses a later metadata.start_ts so the callback's gap_end
    # is distinguishable from anything seen pre-drop.
    bytes2, n2 = _supervisor_dbn_bytes(4; ts_base = Int64(1_700_000_001_000_000_000),
                                          id_base = 200)

    script1 = function(sock)
        _supervisor_handshake!(sock)
        write(sock, bytes1)
        flush(sock)
        # Give the client generous time to drain (cold JIT can push first-run
        # reader compile latency past several seconds on a fresh process).
        sleep(2.0)
        close(sock)
    end
    script2 = function(sock)
        _supervisor_handshake!(sock)
        write(sock, bytes2)
        flush(sock)
        sleep(2.0)
    end

    mock = spawn_mock_gateway_sequence([script1, script2])
    callback_args = Vector{Tuple{Int64,Int64}}()

    client = Live(_SUPERVISOR_TEST_KEY;
        dataset = "TEST.MOCK",
        gateway = "127.0.0.1", port = mock.port,
        compression = DBN.Compression.NONE,
        reconnect_policy = :reconnect,
        max_reconnect_attempts = 5,
        immediate_reconnect_attempts = 3,
    )
    add_reconnect_callback(client, (g0, g1) -> push!(callback_args, (g0, g1)))

    connect!(client)
    subscribe!(client;
        schema = DBN.Schema.TRADES,
        symbols = ["AAPL"],
        stype_in = DBN.SType.INSTRUMENT_ID,
    )
    start!(client)

    collected = DBN.DBNRecord[]
    deadline = time() + 20.0
    while time() < deadline && length(collected) < n1 + n2
        try
            push!(collected, take!(client.channel))
        catch e
            e isa InvalidStateException && break
            rethrow()
        end
    end
    close(client)
    # Drain the supervisor before asserting on side-effects (callback list).
    # Close transitions state→:closed; supervisor exits its loop after the
    # current iteration. Without this wait, the supervisor's _fire_reconnect_-
    # callbacks may not have run by the time we check, particularly under a
    # warm-JIT full-suite run where everything is fast.
    if client.reconnect_supervisor !== nothing
        try; wait(client.reconnect_supervisor); catch; end
    end
    try; wait(mock.accept_task); catch; end

    @test length(collected) >= n1 + n2
    @test all(r -> r isa DBN.TradeMsg, collected)
    # Pre-drop record ids run in 101..103; post-drop in 201..204.
    pre_drop  = [Int(r.hd.instrument_id) for r in collected if r.hd.instrument_id <= 103]
    post_drop = [Int(r.hd.instrument_id) for r in collected if r.hd.instrument_id >= 201]
    @test length(pre_drop)  == n1
    @test length(post_drop) == n2

    @test length(callback_args) == 1
    g0, g1 = callback_args[1]
    @test g0 > 0                                   # we saw records pre-drop
    @test g1 >= g0                                  # gap_end after gap_start
    @test g1 == Int64(1_700_000_001_000_000_000)    # script2's metadata.start_ts
end

@testset "live reconnect supervisor — gateway ErrorMsg is terminal" begin
    # Build a payload that sends one trade then an ErrorMsg. The reader's
    # control path sets c.terminal_error; the supervisor must NOT reconnect.
    metadata = DBN.Metadata(
        DBN.DBN_VERSION, "TEST.MOCK", DBN.Schema.TRADES,
        Int64(1_700_000_000_000_000_000), nothing, nothing, nothing,
        DBN.SType.INSTRUMENT_ID, false,
        ["AAPL"], String[], String[],
        Tuple{String,String,Int64,Int64}[],
    )
    hd_trade = DBN.RecordHeader(UInt8(0), DBN.RType.MBP_0_MSG,
                                 UInt16(1), UInt32(100),
                                 Int64(1_700_000_000_000_000_000))
    trade = DBN.TradeMsg(
        hd_trade, Int64(150_000_000_000), UInt32(100),
        DBN.Action.TRADE, DBN.Side.ASK, UInt8(0), UInt8(1),
        Int64(1_700_000_000_000_000_000), Int32(0), UInt32(1),
    )
    hd_err = DBN.RecordHeader(UInt8(0), DBN.RType.ERROR_MSG,
                               UInt16(1), UInt32(0),
                               Int64(1_700_000_000_000_000_000))
    err = DBN.ErrorMsg(hd_err, "fatal-test-err")
    tmp, io = mktemp(); close(io)
    bytes = try
        DBN.write_dbn(tmp, metadata, DBN.DBNRecord[trade, err])
        read(tmp)
    finally
        rm(tmp; force = true)
    end

    accept_count = Threads.Atomic{Int}(0)
    script = function(sock)
        Threads.atomic_add!(accept_count, 1)
        _supervisor_handshake!(sock)
        write(sock, bytes)
        flush(sock)
        sleep(2.0)                                     # let client drain
        close(sock)
    end
    # Pre-stage extras; supervisor should refuse to reconnect after the
    # first session's ErrorMsg, so the second accept should never happen.
    mock = spawn_mock_gateway_sequence([script for _ in 1:5])

    callback_count = Ref(0)
    client = Live(_SUPERVISOR_TEST_KEY;
        dataset = "TEST.MOCK",
        gateway = "127.0.0.1", port = mock.port,
        compression = DBN.Compression.NONE,
        typed = true,                                  # use control channel
        reconnect_policy = :reconnect,
        max_reconnect_attempts = 5,
    )
    add_reconnect_callback(client, (g0, g1) -> (callback_count[] += 1))

    connect!(client)
    data_ch = subscribe!(client;
        schema = DBN.Schema.TRADES,
        symbols = ["AAPL"],
        stype_in = DBN.SType.INSTRUMENT_ID,
    )
    start!(client)

    # Drain the data channel until it closes (terminal state closes channels).
    seen = DBN.TradeMsg[]
    deadline = time() + 10.0
    while time() < deadline
        try
            push!(seen, take!(data_ch))
        catch e
            e isa InvalidStateException && break
            rethrow()
        end
    end
    close(client)
    if client.reconnect_supervisor !== nothing
        try; wait(client.reconnect_supervisor); catch; end
    end
    try; close(mock.server); catch; end
    try; wait(mock.accept_task); catch; end

    @test accept_count[] == 1                    # never reconnected
    @test client.terminal_error !== nothing      # ErrorMsg captured
    @test callback_count[] == 0                  # no reconnect callback fired
    @test length(seen) >= 1                      # got the pre-error trade
end

@testset "live_session bundles connect/subscribe/start + close" begin
    bytes, n = _supervisor_dbn_bytes(3; ts_base = Int64(1_700_000_000_000_000_000),
                                        id_base = 100)
    script = function(sock)
        _supervisor_handshake!(sock)
        write(sock, bytes); flush(sock)
        sleep(2.0)
        close(sock)
    end
    mock = spawn_mock_gateway_sequence([script])

    collected = DBN.DBNRecord[]
    live_session(;
        dataset = "TEST.MOCK",
        subscriptions = [(; schema = DBN.Schema.TRADES,
                            symbols = ["AAPL"],
                            stype_in = DBN.SType.INSTRUMENT_ID)],
        reconnect_policy = :none,          # single-shot for this smoke test
        key = _SUPERVISOR_TEST_KEY,
        gateway = "127.0.0.1", port = mock.port,
        compression = DBN.Compression.NONE,
    ) do client
        deadline = time() + 10.0
        while time() < deadline && length(collected) < n
            try
                push!(collected, take!(client.channel))
            catch e
                e isa InvalidStateException && break
                rethrow()
            end
        end
    end
    try; wait(mock.accept_task); catch; end

    @test length(collected) == n
    @test all(r -> r isa DBN.TradeMsg, collected)
end
