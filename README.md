# DatabentoAPI.jl

[![CI](https://github.com/tbeason/DatabentoAPI.jl/actions/workflows/CI.yml/badge.svg)](https://github.com/tbeason/DatabentoAPI.jl/actions/workflows/CI.yml)
[![Documentation](https://github.com/tbeason/DatabentoAPI.jl/actions/workflows/documentation.yml/badge.svg)](https://tbeason.github.io/DatabentoAPI.jl)

Julia client for the [Databento](https://databento.com) market-data APIs:

- **Historical** — HTTP API for backfills, metadata, batch jobs, symbology.
- **Live** — TCP API for low-latency real-time feeds with reconnect,
  multi-schema routing, and crash-safe capture to disk.

All binary DBN decoding is delegated to
[DatabentoBinaryEncoding.jl](https://github.com/tbeason/DatabentoBinaryEncoding.jl).

## Documentation

**Full documentation: https://tbeason.github.io/DatabentoAPI.jl**

User guides for [authentication](https://tbeason.github.io/DatabentoAPI.jl/stable/guide/authentication/),
[historical data](https://tbeason.github.io/DatabentoAPI.jl/stable/guide/historical/),
[live streaming](https://tbeason.github.io/DatabentoAPI.jl/stable/guide/live/),
[capture to file](https://tbeason.github.io/DatabentoAPI.jl/stable/guide/capture/),
[format conversion](https://tbeason.github.io/DatabentoAPI.jl/stable/guide/conversion/),
plus an API reference, performance notes, and troubleshooting guide.

## Installation

```julia
using Pkg
Pkg.add("DatabentoAPI")
```

Requires Julia ≥ 1.12.

## Quick start

```julia
using DatabentoAPI, Dates

# Historical
client = Historical()                       # API key from ~/.databento/config.toml or DATABENTO_API_KEY
store  = get_range(client;
    dataset  = "XNAS.ITCH",
    schema   = Schema.TRADES,
    symbols  = ["AAPL"],
    start    = DateTime(2024, 1, 2, 14, 30),
    end_     = DateTime(2024, 1, 2, 14, 31),
    stype_in = SType.RAW_SYMBOL)
df = to_dataframe(store)

# Live (do-block guarantees clean close on Ctrl-C)
Live(dataset = "GLBX.MDP3") do live
    connect!(live)
    subscribe!(live; schema = Schema.TRADES, symbols = ["ES.FUT"], stype_in = SType.PARENT)
    start!(live)
    for rec in live
        println(rec)
    end
end
```

See the [Quick Start guide](https://tbeason.github.io/DatabentoAPI.jl/stable/quickstart/)
for details.

## Status

Current version: **0.1.2**. Offline test suite: **1500+ tests**, ~30 s.
Live-network smoke tests are gated behind `DATABENTO_LIVE_TESTS=1`. See
[CHANGELOG.md](CHANGELOG.md) for the full release history.

## License

MIT.
