# Historical Data

```@meta
CurrentModule = DatabentoAPI
```

The Historical API is an HTTPS endpoint at `hist.databento.com`. Use it for
backfills, metadata lookups, batch jobs, and symbology resolution.

## The client

```julia
using DatabentoAPI

client = Historical()                       # key resolved per Authentication
# or
client = Historical("db-override-key")
# or with explicit gateway:
client = Historical(; gateway = HistoricalGateway.BO1)
```

The client is cheap to construct (no network on construction) and safe to
share across threads — each call opens a fresh HTTP request via `HTTP.jl`.

## Fetching records: `get_range`

For queries whose result fits comfortably in RAM (under a few GB):

```julia
using DatabentoAPI, Dates

store = get_range(client;
    dataset  = "XNAS.ITCH",
    schema   = Schema.TRADES,
    symbols  = ["AAPL", "MSFT"],
    start_dt = DateTime(2024, 1, 2, 14, 30),
    end_dt   = DateTime(2024, 1, 2, 15, 0),
    stype_in = SType.RAW_SYMBOL)

length(store.records)     # Vector{DBN.DBNRecord}
store.metadata.dataset    # "XNAS.ITCH"
```

[`get_range`](@ref) returns a [`DBNStore`](@ref) — a struct bundling the DBN
metadata header with a vector of decoded records. Convert it with
[`to_dataframe`](@ref), [`to_csv`](@ref), [`to_json`](@ref),
[`to_parquet`](@ref), or write it back to disk with [`to_file`](@ref).

## Streaming records: `foreach_record`

For larger queries (millions of records), don't materialize the whole vector.
Stream record-by-record with a typed callback — DBN.jl's typed dispatch keeps
the hot path allocation-free:

```julia
n_trades = Ref(0)
foreach_record(client, DBN.TradeMsg;
    dataset = "XNAS.ITCH", schema = Schema.TRADES,
    symbols = ["AAPL"],
    start_dt = DateTime(2024, 1, 2, 14, 30),
    end_dt   = DateTime(2024, 1, 2, 20, 0)) do trade
    n_trades[] += 1
end
@show n_trades[]
```

The first argument after `client` is the concrete record type for the
schema you've requested (`DBN.TradeMsg` for `Schema.TRADES`,
`DBN.MBP1Msg` for `Schema.MBP_1`, etc.). DBN.jl's `record_type_for_dbn_schema`
maps schemas → types.

## Metadata: cheap, free, useful

Before issuing a large `get_range`, preview the query:

```julia
get_record_count(client; dataset = "XNAS.ITCH", schema = Schema.TRADES,
                 symbols = ["AAPL"], start_dt = "2024-01-02", end_dt = "2024-01-03")
# → integer record count

get_billable_size(client; dataset = "XNAS.ITCH", schema = Schema.TRADES,
                  symbols = ["AAPL"], start_dt = "2024-01-02", end_dt = "2024-01-03")
# → integer billable bytes

get_cost(client; dataset = "XNAS.ITCH", schema = Schema.TRADES,
         symbols = ["AAPL"], start_dt = "2024-01-02", end_dt = "2024-01-03",
         mode = FeedMode.HISTORICAL)
# → USD cost
```

Other metadata endpoints — all free — enumerate the catalog:

| Function                          | Use                                     |
|-----------------------------------|-----------------------------------------|
| [`list_publishers`](@ref)         | All venues / data providers             |
| [`list_datasets`](@ref)           | All datasets, optionally date-windowed  |
| [`list_schemas`](@ref)            | Schemas offered by a dataset            |
| [`list_fields`](@ref)             | Field list for a (schema, encoding)     |
| [`list_unit_prices`](@ref)        | Price-per-GB by feed mode and schema    |
| [`get_dataset_range`](@ref)       | First/last available timestamp          |
| [`get_dataset_condition`](@ref)   | Per-date data quality status            |

## Batch jobs: results too big to stream

Submit an asynchronous job, poll for completion, then download:

```julia
job = submit_job(client;
    dataset = "XNAS.ITCH", schema = Schema.MBO,
    symbols = ["AAPL"],
    start_dt = "2024-01-02", end_dt = "2024-01-09")

job_id = job["id"]

# Poll until done
while true
    jobs = list_jobs(client; states = [JobState.PROCESSING, JobState.DONE])
    j = first(filter(x -> x["id"] == job_id, jobs))
    j["state"] == "done" && break
    sleep(30)
end

# Fetch files
paths = batch_download(client; job_id = job_id, output_dir = "./batch_out")
```

See [`submit_job`](@ref) for the full kwarg list — `split_duration`,
`packaging`, `delivery`, etc.

## Symbology

Resolve symbols from one stype to another over a date range:

```julia
resolve(client;
    dataset    = "GLBX.MDP3",
    symbols    = ["ES.FUT"],
    stype_in   = SType.PARENT,
    stype_out  = SType.INSTRUMENT_ID,
    start_date = Date(2024, 1, 2),
    end_date   = Date(2024, 1, 5))
```

Returns a JSON object whose `result` field maps each input symbol to a list
of `(start_date, end_date, mapped_symbol)` intervals. Useful for joining
historical records to a stable symbol over time.
