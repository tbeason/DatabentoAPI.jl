using Test
using DatabentoAPI
using DatabentoBinaryEncoding
import DatabentoBinaryEncoding as DBN

@testset "typed mode — error coverage" begin

    @testset "subscribe! rejects schemas without concrete record type" begin
        client = DatabentoAPI.Live("db-1234567890abcdef12345";
            dataset = "TEST.MOCK",
            gateway = "127.0.0.1", port = 1,
            typed = true)
        client.connected = true   # bypass actual connect for the validation check
        @test_throws ArgumentError DatabentoAPI.subscribe!(client;
            schema = Schema.MIX, symbols = ["AAPL"], stype_in = SType.RAW_SYMBOL)
        client.closed = true
    end

    @testset "iterate(::Live) errors in typed mode" begin
        client = DatabentoAPI.Live("db-1234567890abcdef12345";
            dataset = "TEST.MOCK",
            gateway = "127.0.0.1", port = 1,
            typed = true)
        client.connected = true
        client.started = true
        @test_throws ArgumentError iterate(client)
        client.closed = true
    end

    @testset "control_channel(client) requires typed mode" begin
        untyped = DatabentoAPI.Live("db-1234567890abcdef12345";
            dataset = "TEST.MOCK", typed = false)
        @test_throws ArgumentError DatabentoAPI.control_channel(untyped)
        untyped.closed = true
    end

    @testset "channel(client, schema) requires typed mode" begin
        untyped = DatabentoAPI.Live("db-1234567890abcdef12345";
            dataset = "TEST.MOCK", typed = false)
        @test_throws ArgumentError DatabentoAPI.channel(untyped, Schema.TRADES)
        untyped.closed = true
    end

    @testset "channel(client, schema) errors for unsubscribed schema" begin
        client = DatabentoAPI.Live("db-1234567890abcdef12345";
            dataset = "TEST.MOCK", typed = true)
        @test_throws KeyError DatabentoAPI.channel(client, Schema.TRADES)
        client.closed = true
    end

    @testset "subscribe_callback errors in typed mode" begin
        client = DatabentoAPI.Live("db-1234567890abcdef12345";
            dataset = "TEST.MOCK", typed = true)
        client.connected = true
        client.started = true
        @test_throws ArgumentError DatabentoAPI.subscribe_callback(client, _ -> nothing)
        client.closed = true
    end

    @testset "subscribe! after start! still errors (typed mode preserves rule)" begin
        client = DatabentoAPI.Live("db-1234567890abcdef12345";
            dataset = "TEST.MOCK", typed = true)
        client.connected = true
        client.started = true
        @test_throws ArgumentError DatabentoAPI.subscribe!(client;
            schema = Schema.TRADES, symbols = ["AAPL"], stype_in = SType.RAW_SYMBOL)
        client.closed = true
    end

    @testset "untyped mode: subscribe! still returns Int sub_id" begin
        # Smoke check the untyped path is fully preserved.
        client = DatabentoAPI.Live("db-1234567890abcdef12345";
            dataset = "TEST.MOCK", typed = false)
        client.connected = true
        # Can't actually call subscribe! without a real socket, but we can
        # verify the existing channel field is the Union-typed one.
        @test client.channel isa Channel{DBN.DBNRecord}
        @test client.control_channel === nothing
        @test isempty(client.typed_data_channels)
        client.closed = true
    end
end
