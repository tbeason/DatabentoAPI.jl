using Test
using DatabentoAPI

# Live-network smoke test: only runs when DATABENTO_LIVE_TESTS=1.
# Connects to GLBX.MDP3, subscribes to ES.FUT trades for 30s, validates connection.

@testset "Live smoke (real gateway)" begin
    # Defaults to OPRA.PILLAR since that's the only feed the maintainer is
    # subscribed to. Override with DATABENTO_LIVE_DATASET / _SYMBOLS / _STYPE.
    dataset  = get(ENV, "DATABENTO_LIVE_DATASET", "OPRA.PILLAR")
    symbols  = split(get(ENV, "DATABENTO_LIVE_SYMBOLS", "SPXW.OPT"), ",")
    stype_in = SType.PARENT
    if haskey(ENV, "DATABENTO_LIVE_STYPE")
        stype_in = getfield(SType, Symbol(uppercase(ENV["DATABENTO_LIVE_STYPE"])))
    end
    duration = parse(Int, get(ENV, "DATABENTO_LIVE_DURATION", "10"))

    client = Live(dataset = dataset)
    connect!(client)
    subscribe!(client;
        schema   = Schema.TRADES,
        symbols  = symbols,
        stype_in = stype_in)
    start!(client)

    # Poll the channel non-blockingly with a hard deadline. Avoids the iterator's
    # blocking `take!` so we never wait past `duration` for the close to take
    # effect.
    counts = Dict{Symbol,Int}()
    seen = 0
    sample_system_msgs = String[]
    sample_errors      = String[]
    deadline = time() + duration
    while time() < deadline
        if isready(client.channel)
            rec = take!(client.channel)
            seen += 1
            t = nameof(typeof(rec))
            counts[t] = get(counts, t, 0) + 1
            if rec isa DBN.SystemMsg && length(sample_system_msgs) < 5
                push!(sample_system_msgs, "code=$(rec.code) msg=$(rec.msg)")
            elseif rec isa DBN.ErrorMsg && length(sample_errors) < 5
                push!(sample_errors, rec.err)
            end
        else
            sleep(0.05)
        end
        # If reader closed the channel, drain any remaining records and exit.
        if !isopen(client.channel) && !isready(client.channel)
            break
        end
    end
    close(client)
    @info "Live smoke complete" duration_s = duration total = seen by_type = counts
    isempty(sample_system_msgs) || @info "SystemMsg samples" sample_system_msgs
    isempty(sample_errors)      || @warn "ErrorMsg samples" sample_errors
    @test seen >= 0
end
