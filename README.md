# DatabentoAPI.jl

Julia client for the [Databento](https://databento.com) market data APIs.

Wraps both endpoints:

- **Historical** — HTTP requests to `https://hist.databento.com`, returning zstd‑compressed DBN payloads.
- **Live** — raw TCP to per‑dataset gateways (`<dataset>.lsg.databento.com:13000`) with CRAM authentication.

All DBN binary record decoding is delegated to the sibling package [DBN.jl](https://github.com/tylerbeason/DBN.jl).

## Status

`v0.1.0` — unregistered, in active development. Both Historical and Live clients are wired up and exercised by an offline test suite (132 tests, ~12 s). Live‑network smoke tests are gated behind `DATABENTO_LIVE_TESTS=1` and default to the OPRA.PILLAR feed.

## Installation

DBN.jl is unregistered, so install it as a dev dep alongside this package:

```julia
using Pkg
Pkg.develop(url = "https://github.com/tylerbeason/DBN.jl")
Pkg.develop(url = "https://github.com/tylerbeason/DatabentoAPI.jl")
```

Or from a local clone of both repos:

```julia
Pkg.develop(path = "/path/to/DBN.jl")
Pkg.develop(path = "/path/to/DatabentoAPI.jl")
```

Once both packages are in the General registry, this becomes:

```julia
Pkg.add("DatabentoAPI")
```

## Authentication

Resolved in this order:

1. The `key` argument to `Historical(...)` / `Live(...)`.
2. `~/.databento/config.toml`:
   ```toml
   [auth]
   api_key = "db-..."
   ```
3. `ENV["DATABENTO_API_KEY"]`.

If none are set, [`load_api_key`](src/auth.jl) throws `BentoAuthError`.

## Historical example

```julia
using DatabentoAPI, Dates

client = Historical()                    # picks up the key from config or env
store  = get_range(client;
    dataset  = "OPRA.PILLAR",
    schema   = Schema.TRADES,
    symbols  = ["AAPL"],
    start    = DateTime(2024, 1, 2, 14, 30),
    end_     = DateTime(2024, 1, 2, 14, 31),
    stype_in = SType.RAW_SYMBOL)

println("got ", length(store), " records")
df = to_dataframe(store)                  # delegates to DBN.records_to_dataframe
```

Other endpoints exposed at the package level:

```
list_publishers, list_datasets, list_schemas, list_fields,
list_unit_prices, get_dataset_range, get_dataset_condition,
get_record_count, get_billable_size, get_cost,
submit_job, list_jobs, list_files, batch_download,
resolve
```

`batch_download` is named to avoid clashing with `Base.download`.

## Live example

```julia
using DatabentoAPI

client = Live(dataset = "OPRA.PILLAR")
connect!(client)
subscribe!(client;
    schema   = Schema.TRADES,
    symbols  = ["AAPL"],
    stype_in = SType.RAW_SYMBOL)
start!(client)

for rec in client                         # iterator pulls from internal Channel
    println(rec)
    rec isa DBN.TradeMsg && rec.price > some_threshold && break
end
close(client)
```

Callback variant:

```julia
subscribe_callback(client, rec -> @info "tick" rec)
sleep(30)
close(client)
```

## Development

```julia
using Pkg
Pkg.activate("DatabentoAPI.jl")
Pkg.develop(path = "../DBN.jl")
Pkg.instantiate()
Pkg.test()                                 # 132 offline tests, ~12s
```

To run real‑network smoke tests (uses your API key, costs money):

```bash
export DATABENTO_LIVE_TESTS=1
julia --project=. -e 'using Pkg; Pkg.test()'
```

The Live smoke test defaults to `OPRA.PILLAR`. Override with `DATABENTO_LIVE_DATASET`, `DATABENTO_LIVE_SYMBOLS`, `DATABENTO_LIVE_STYPE`. The Historical smoke test honours `DATABENTO_HIST_DATASET`, `DATABENTO_HIST_SYMBOLS`, `DATABENTO_HIST_START`, `DATABENTO_HIST_END`.

## License

MIT.
