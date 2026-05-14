using Test
using DatabentoAPI

@testset "enums" begin
    @testset "DatabentoAPI-specific enums exist" begin
        @test JobState.QUEUED isa JobState.T
        @test JobState.DONE isa JobState.T
        @test SplitDuration.DAY isa SplitDuration.T
        @test Packaging.ZIP isa Packaging.T
        @test Delivery.DOWNLOAD isa Delivery.T
        @test SymbologyResolution.OK isa SymbologyResolution.T
        @test RollRule.VOLUME isa RollRule.T
        @test FeedMode.LIVE isa FeedMode.T
        @test ReconnectPolicy.NONE isa ReconnectPolicy.T
        @test HistoricalGateway.BO1 isa HistoricalGateway.T
    end

    @testset "DBN.jl enums re-exported" begin
        @test Schema.TRADES isa Schema.T
        @test SType.RAW_SYMBOL isa SType.T
        @test Compression.ZSTD isa Compression.T
        @test Encoding.DBN isa Encoding.T
    end
end
