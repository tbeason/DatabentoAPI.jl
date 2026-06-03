using Test
using DatabentoAPI
using Dates

# Live-network smoke test: only runs when DATABENTO_LIVE_TESTS=1 and a real API key
# is configured. Costs real money — keep the query tiny.

@testset "Historical smoke (live network)" begin
    # Defaults target OPRA.PILLAR (the maintainer's current subscription).
    # Override with DATABENTO_HIST_DATASET / _SYMBOLS / _START / _END if needed.
    # OPRA.PILLAR is the maintainer's only entitlement; symbols must use .OPT
    # suffixes with stype_in=parent (e.g. SPX.OPT, SPXW.OPT).
    dataset  = get(ENV, "DATABENTO_HIST_DATASET", "OPRA.PILLAR")
    symbols  = split(get(ENV, "DATABENTO_HIST_SYMBOLS", "SPX.OPT"), ",")
    start_s  = get(ENV, "DATABENTO_HIST_START", "2024-01-02T14:30:00")
    end_s    = get(ENV, "DATABENTO_HIST_END",   "2024-01-02T14:31:00")
    stype_in = SType.PARENT
    if haskey(ENV, "DATABENTO_HIST_STYPE")
        stype_in = getfield(SType, Symbol(uppercase(ENV["DATABENTO_HIST_STYPE"])))
    end

    client = Historical()

    @testset "free metadata endpoints" begin
        pubs = list_publishers(client)
        @test !isempty(pubs)
        @info "publishers" n = length(pubs)

        datasets = list_datasets(client)
        @test !isempty(datasets)
        @info "available datasets" sample = first(datasets, min(5, length(datasets)))

        schemas = list_schemas(client; dataset = dataset)
        @test !isempty(schemas)
        @info "schemas for $dataset" schemas = schemas
    end

    @testset "cost estimate (free)" begin
        # `get_cost` and `get_billable_size` are free — they estimate without
        # actually transferring data.
        cost = get_cost(client;
            dataset  = dataset,
            symbols  = symbols,
            schema   = Schema.TRADES,
            start_dt = start_s,
            end_dt   = end_s,
            stype_in = stype_in)
        @info "cost estimate" cost
        @test cost !== nothing

        sz = get_billable_size(client;
            dataset  = dataset,
            symbols  = symbols,
            schema   = Schema.TRADES,
            start_dt = start_s,
            end_dt   = end_s,
            stype_in = stype_in)
        @info "billable size" sz
        @test sz !== nothing
    end

    if get(ENV, "DATABENTO_HIST_FETCH", "") == "1"
        @testset "tiny get_range (BILLED)" begin
            store = get_range(client;
                dataset  = dataset,
                schema   = Schema.TRADES,
                symbols  = symbols,
                start_dt = start_s,
                end_dt   = end_s,
                stype_in = stype_in)
            @test store isa DBNStore
            @test store.metadata.dataset == dataset
            @info "got data" n = length(store)
        end
    else
        @info "skipping get_range — set DATABENTO_HIST_FETCH=1 to enable (BILLED)"
    end
end
