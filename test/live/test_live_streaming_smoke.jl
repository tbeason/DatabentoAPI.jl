using Test
using DatabentoAPI
using DatabentoBinaryEncoding
import DatabentoBinaryEncoding as DBN

@testset "Live streaming smoke (OPRA.PILLAR)" begin
    base = mktempdir()
    @info "Live streaming smoke" base_dir = base
    paths = stream_multi_to_files(
        dataset  = get(ENV, "DATABENTO_LIVE_DATASET", "OPRA.PILLAR"),
        schemas  = [Schema.TCBBO, Schema.CBBO_1S, Schema.STATUS],
        symbols  = split(get(ENV, "DATABENTO_LIVE_SYMBOLS", "SPXW.OPT"), ","),
        stype_in = SType.PARENT,
        base_dir = base,
        duration_s = parse(Int, get(ENV, "DATABENTO_LIVE_DURATION", "30")),
        compress = true,
        reconnect = false,
        heartbeat_log_interval_s = 10.0,
    )
    @test length(paths) == 3
    expected_types = Dict(
        Schema.TCBBO   => DBN.TCBBOMsg,
        Schema.CBBO_1S => DBN.CBBO1sMsg,
        Schema.STATUS  => DBN.StatusMsg,
    )
    for (sch, path) in paths
        @info "smoke result" schema = sch path = path
        @test isfile(path)
        _, recs = DatabentoAPI.read_capture(path)
        @info "decoded" schema = sch n = length(recs)
        @test length(recs) >= 0
        if !isempty(recs)
            T = expected_types[sch]
            @test any(r -> r isa T, recs)
        end
    end
end
