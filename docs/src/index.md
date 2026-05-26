# DatabentoAPI.jl Documentation

Julia client for the [Databento](https://databento.com) market-data APIs:

- **Historical** — HTTP API for backfills, metadata, batch jobs, and symbology.
- **Live** — TCP API for low-latency real-time feeds with reconnect, multiple
  schemas, and crash-safe capture to disk.

Built on top of [DatabentoBinaryEncoding.jl](https://github.com/tbeason/DatabentoBinaryEncoding.jl),
which handles all binary record decoding. DatabentoAPI.jl is a thin transport
+ session layer; the heavy lifting on the wire format lives in the sister
package.

## What this package wraps

- Authenticated HTTP requests to the Historical gateway.
- The Live TCP protocol: CRAM handshake, subscription frames, multi-schema
  routing, full-jitter exponential backoff on reconnect.
- Capture to `.dbn[.zst]` files with optional time-based file rotation and
  in-file zstd frame rotation for crash safety.
- Conversion helpers wrapping DBN.jl's CSV / JSON / Parquet / DataFrame
  converters.

## Quick taste

```julia
using DatabentoAPI, Dates

# Historical: pull a minute of trades and convert to a DataFrame.
client = Historical()
store  = get_range(client;
    dataset  = "XNAS.ITCH",
    schema   = Schema.TRADES,
    symbols  = ["AAPL"],
    start    = DateTime(2024, 1, 2, 14, 30),
    end_     = DateTime(2024, 1, 2, 14, 31),
    stype_in = SType.RAW_SYMBOL)
df = to_dataframe(store)

# Live: subscribe and iterate inside a do-block; clean shutdown on Ctrl-C.
Live(dataset = "GLBX.MDP3") do live
    connect!(live)
    subscribe!(live; schema = Schema.TRADES, symbols = ["ES.FUT"], stype_in = SType.PARENT)
    start!(live)
    for rec in live
        @info "tick" rec
    end
end
```

## Where to go next

- New here? [Installation](@ref) → [Quick Start](@ref Quick-Start).
- Setting up credentials? See [Authentication](guide/authentication.md).
- Historical workflows: [Historical Data](@ref).
- Live feeds: [Live Streaming](@ref) and [Capture to File](@ref).
- Reference: [API Reference](api/historical.md).

## Status

The package is in active development. Core functionality is tested
(1500+ offline tests against mocked TCP gateways and HTTP servers) and used
in production captures, but the public API may still evolve. Pin to the
patch version when stability matters.
