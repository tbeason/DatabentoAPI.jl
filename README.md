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

For very large queries where buffering the entire payload + decoded record vector
in memory is wasteful, use the streaming variant `foreach_record`. It overlaps the
HTTP download with decompress + decode and never materialises the full record list:

```julia
n = 0
md = foreach_record(client;
    dataset  = "OPRA.PILLAR", schema = Schema.TRADES, symbols = ["SPY.OPT"],
    start    = DateTime(2026, 4, 15, 14, 30),
    end_     = DateTime(2026, 4, 15, 15, 30),
    stype_in = SType.PARENT) do rec
    n += 1
    # process rec on the fly — it's a DBN.TradeMsg / MBP1Msg / etc.
end
println("processed ", n, " records, dataset=", md.dataset)
```

Other endpoints exposed at the package level:

```
list_publishers, list_datasets, list_schemas, list_fields,
list_unit_prices, get_dataset_range, get_dataset_condition,
get_record_count, get_billable_size, get_cost,
submit_job, list_jobs, list_files, batch_download,
resolve, foreach_record
```

`batch_download` is named to avoid clashing with `Base.download`.

## Live example

The do-block form mirrors `Base.open` and guarantees `close(client)` runs on
`Ctrl-C`, exceptions, or normal exit — no `try/finally` boilerplate needed.

```julia
using DatabentoAPI

Live(dataset = "OPRA.PILLAR") do client
    connect!(client)
    subscribe!(client;
        schema   = Schema.TRADES,
        symbols  = ["AAPL"],
        stype_in = SType.RAW_SYMBOL)
    start!(client)
    for rec in client                     # iterator pulls from internal Channel
        println(rec)
        rec isa DBN.TradeMsg && rec.price > some_threshold && break
    end
end   # Ctrl-C here triggers a clean close(client)
```

Manual lifecycle still works if you prefer:

```julia
client = Live(dataset = "OPRA.PILLAR")
connect!(client); subscribe!(client; …); start!(client)
for rec in client; …; end
close(client)
```

Callback variant:

```julia
Live(dataset = "OPRA.PILLAR") do client
    connect!(client)
    subscribe!(client; schema = Schema.TRADES, symbols = ["AAPL"], stype_in = SType.RAW_SYMBOL)
    subscribe_callback(client, rec -> @info "tick" rec)
    start!(client)
    sleep(30)
end
```

### Writing records to disk yourself

If you want to subscribe and write records to a DBN file from your own loop —
including in-file zstd frame rotation for crash safety — pair `Live` with
`open_dbn_writer`:

```julia
Live(dataset = "GLBX.MDP3") do client
    connect!(client)
    subscribe!(client; schema = Schema.TRADES, symbols = ["ES.FUT"], stype_in = SType.PARENT)
    start!(client)
    open_dbn_writer(; base_dir = "./capture", dataset = "GLBX.MDP3",
                      schema = Schema.TRADES, symbols = ["ES.FUT"],
                      stype_in = SType.PARENT,
                      frame_seconds = 60.0) do writer
        for rec in client
            write_record!(writer, rec)
        end
    end
end
```

For the common case of "subscribe and dump every record to disk", use
[`stream_to_file`](@ref) directly — it wraps the same pieces.

### Typed-channel variant (`typed = true`)

For high-throughput consumers, opt into per-schema typed channels:
`subscribe!` returns a `Channel{T}` for the concrete record type instead
of an `Int` sub_id. Per-record allocation drops ~85% on real OPRA
streams (see `benchmark/PERF_REPORT.md`).

```julia
client = Live(dataset = "OPRA.PILLAR", typed = true)
connect!(client)
ch_trades = subscribe!(client; schema = Schema.TRADES, symbols = ["SPY.OPT"],
                       stype_in = SType.PARENT)               # Channel{DBN.TradeMsg}
ch_cbbo   = subscribe!(client; schema = Schema.CBBO_1S, symbols = ["SPY.OPT"],
                       stype_in = SType.PARENT)               # Channel{DBN.CBBO1sMsg}
start!(client)

@async for rec in ch_trades                                    # type-stable: rec :: TradeMsg
    handle_trade(rec)
end
@async for rec in ch_cbbo                                      # type-stable: rec :: CBBO1sMsg
    handle_quote(rec)
end

# ErrorMsg / SystemMsg / SymbolMappingMsg arrive on a dedicated channel:
@async for rec in control_channel(client)
    rec isa DBN.ErrorMsg && @error "gateway error" rec.err
end
```

Caveats:
- Subscribing to a schema with no concrete record type (e.g. `Schema.MIX`)
  errors at `subscribe!`.
- `for rec in client` and `subscribe_callback(client, fn)` are
  untyped-mode-only; under typed mode iterate each channel directly.

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

## Benchmarks

See `benchmark/` for the offline performance suite (`julia --project=benchmark benchmark/runbench.jl`) and `benchmark/PERF_REPORT.md` for the current baseline numbers and recommendations.

For high-throughput iteration over historical data, prefer
`DatabentoAPI.foreach_record(client; ...)` or `DBN.foreach_record(file, T)`
over `DBNStore` indexing or `DBNStream`: the typed-callback path has
near-zero per-record allocation (~0.13 bytes/record), while the Union-typed
iterator paths allocate ~120 bytes/record.

## License

MIT.
