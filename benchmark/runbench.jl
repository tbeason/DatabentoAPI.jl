# Top-level runner.
#
# Activate the benchmark project and invoke:
#
#     julia --project=benchmark benchmark/runbench.jl
#
# Pass `--profile` to also run the profile sampling pass (adds ~30s).
# Pass `--tiers=small,medium` to override the default size tiers.

using Pkg
Pkg.activate(@__DIR__)
Pkg.instantiate()

const PROFILE = "--profile" in ARGS
const TIERS = let
    arg = findfirst(a -> startswith(a, "--tiers="), ARGS)
    if arg === nothing
        (:small, :medium)
    else
        Tuple(Symbol(strip(s)) for s in split(split(ARGS[arg], "=")[2], ","))
    end
end

println("Running DatabentoAPI.jl + DBN.jl pressure-test")
println("  tiers : ", TIERS)
println("  profile: ", PROFILE)
println("  results: ", joinpath(@__DIR__, "results"))

include(joinpath(@__DIR__, "bench_dbn_read.jl"))
include(joinpath(@__DIR__, "bench_dbn_write.jl"))
include(joinpath(@__DIR__, "bench_historical_decode.jl"))
include(joinpath(@__DIR__, "bench_live_reader.jl"))
include(joinpath(@__DIR__, "bench_stream_to_file.jl"))

BenchDBNRead.run(tiers = TIERS)
BenchDBNWrite.run(tiers = TIERS)
BenchHistoricalDecode.run(tiers = TIERS)
BenchLiveReader.run(tiers = TIERS)
BenchStreamToFile.run(tiers = (:small,))   # stream_to_file is slow; small tier only

if PROFILE
    include(joinpath(@__DIR__, "profile_hotspots.jl"))
    ProfileHotspots.run(tier = :medium, seconds = 5.0)
end

println("\nDone. Results CSVs and profile txt files are under benchmark/results/")
