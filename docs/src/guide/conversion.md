# Format Conversion

```@meta
CurrentModule = DatabentoAPI
```

DatabentoAPI.jl re-exports DBN.jl's format converters so you can move records
between DBN, CSV, JSON, Parquet, and DataFrames without an extra `using`.

## The `DBNStore` container

[`get_range`](@ref) returns a [`DBNStore`](@ref): a struct bundling the DBN
metadata header with a vector of decoded records.

```julia
store.metadata    # DBN.Metadata — dataset, schema, symbols, ts_in/out_first
store.records     # Vector{DBN.DBNRecord}
length(store)
isempty(store)
```

A `DBNStore` is the input to every conversion function below.

## To DataFrame

```julia
using DatabentoAPI, DataFrames

df = to_dataframe(store)
```

[`to_dataframe`](@ref) delegates to DBN.jl's `records_to_dataframe`. Best for
exploratory analysis up to a few million rows.

## To CSV / JSON / Parquet

```julia
to_csv(store,     "trades.csv")
to_json(store,    "trades.jsonl")     # newline-delimited
to_parquet(store, "trades.parquet")
```

Each function round-trips the records through a temporary DBN file (DBN.jl's
converters work on file paths) and writes the requested format. For very
large stores, write to a `.dbn[.zst]` first via [`to_file`](@ref) and call
the DBN.jl converters directly on that file — you'll avoid the intermediate
materialization.

## Round-tripping records back to disk

```julia
to_file(store, "snapshot.dbn.zst")    # compression on by extension
to_file(store, "snapshot.dbn")        # uncompressed
```

The output is byte-equivalent to a `stream_to_file` capture of the same
records, so [`read_capture`](@ref) (or `DBN.read_dbn_with_metadata`) loads
it back losslessly.

## Performance note

For million-record-plus historical workflows, [`foreach_record`](@ref) with
a typed callback is the high-throughput path — it streams records record-by-
record with near-zero per-record allocation. [`get_range`](@ref) materializes
the entire `DBNStore` in RAM, which is fine for under ~1 GB of data but
expensive beyond that.

See [Performance](@ref) for benchmark numbers and a more detailed comparison.
