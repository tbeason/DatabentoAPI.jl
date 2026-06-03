# Quick Start

```@meta
CurrentModule = DatabentoAPI
```

Two minimal examples, one per API surface. They assume you have an API key
set up — see [Authentication](guide/authentication.md) if not.

## Historical: pull a slice of trades

```julia
using DatabentoAPI, Dates

client = Historical()
store  = get_range(client;
    dataset  = "XNAS.ITCH",
    schema   = Schema.TRADES,
    symbols  = ["AAPL"],
    start_dt = DateTime(2024, 1, 2, 14, 30),
    end_dt   = DateTime(2024, 1, 2, 14, 31),
    stype_in = SType.RAW_SYMBOL)

df = to_dataframe(store)
@show size(df) first(df, 3)
```

For datasets too large to materialize, use [`foreach_record`](@ref) with a
typed callback (zero-allocation hot path) — see [Historical Data](@ref).

## Live: subscribe inside a do-block

```julia
using DatabentoAPI

Live(dataset = "GLBX.MDP3") do live
    connect!(live)
    subscribe!(live;
        schema   = Schema.TRADES,
        symbols  = ["ES.FUT"],
        stype_in = SType.PARENT)
    start!(live)
    for rec in live
        rec isa DBN.TradeMsg && @info "trade" rec.price rec.size
    end
end
# Ctrl-C exits the loop, the do-block's `finally` calls `close(live)`,
# and the function returns normally.
```

The Live client auto-reconnects on TCP drops by default (immediate-retry
phase then exponential backoff). For long-running captures that *also*
write to disk, use [`stream_to_file`](@ref) — see [Capture to File](@ref).
For one-call setup that bundles `connect! → subscribe! → start!`, use
[`live_session`](@ref) — see [Live Streaming](@ref).

## What next?

- Authenticate explicitly: [Authentication](guide/authentication.md).
- Bulk historical workflows + batch jobs: [Historical Data](@ref).
- Typed channels, callbacks, and slow-reader handling: [Live Streaming](@ref).
- Persisting records with frame rotation: [Capture to File](@ref).
