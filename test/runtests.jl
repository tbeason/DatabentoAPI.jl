using Test
using DatabentoAPI

include(joinpath(@__DIR__, "mocks", "tcp_mock.jl"))

offline_failed = false
try
    @testset "DatabentoAPI" begin
        include("test_auth.jl")
        include("test_errors.jl")
        include("test_enums.jl")
        include("test_http.jl")
        include("test_live_cram.jl")
        include("test_live_protocol.jl")
        include("test_live_gateway.jl")
        include("test_conversion.jl")
        include("test_historical_metadata.jl")
        include("test_historical_timeseries.jl")
        include("test_historical_batch.jl")
        include("test_historical_symbology.jl")
        include("test_live_reader.jl")
        include("test_live_reader_typed.jl")
        include("test_live_subscribe.jl")
        include("test_live_streaming.jl")
        include("test_typed_mode_errors.jl")
    end
catch e
    global offline_failed = true
    @error "Offline tests failed; will still attempt live tests if enabled" exception = (e, catch_backtrace())
end

if get(ENV, "DATABENTO_LIVE_TESTS", "") == "1"
    @info "Running gated live-network tests"
    @testset "DatabentoAPI live" begin
        include("live/test_historical_smoke.jl")
        include("live/test_live_smoke.jl")
        include("live/test_live_streaming_smoke.jl")
    end
end

offline_failed && error("Offline test suite had failures")
