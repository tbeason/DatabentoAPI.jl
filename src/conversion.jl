# Thin wrappers around DBN.jl's conversion helpers. DatabentoAPI.jl deliberately
# does not depend on DataFrames/CSV/Parquet directly — DBN.jl already pulls them in.

"""
    to_dataframe(store)

Convert a `DBNStore` to a `DataFrame` via DBN.jl's `records_to_dataframe`.
"""
to_dataframe(s::DBNStore) = DBN.records_to_dataframe(s.records)

"""
    to_file(store, path)

Write the store's records back out as a DBN file using DBN.jl's `write_dbn`.
The output is compressed when `path` ends in `.zst`.
"""
to_file(s::DBNStore, path::AbstractString) = DBN.write_dbn(String(path), s.metadata, s.records)

# CSV / JSON / Parquet conversions go through a temporary DBN file because DBN.jl's
# format converters operate on file paths.
function _via_temp_dbn(s::DBNStore, out_path::AbstractString, fn)
    tmp_path, tmp_io = mktemp()
    close(tmp_io)
    try
        DBN.write_dbn(tmp_path, s.metadata, s.records)
        return fn(tmp_path, String(out_path))
    finally
        isfile(tmp_path) && rm(tmp_path; force = true)
    end
end

"""
    to_csv(store, path)

Write the records in `store` to a CSV file at `path`. Round-trips the records
through a temporary DBN file because DBN.jl's CSV converter operates on file
paths; for large stores prefer running `DBN.dbn_to_csv` directly on a captured
`.dbn[.zst]` file from [`stream_to_file`](@ref).

```julia
store = get_range(client; dataset = "XNAS.ITCH", schema = Schema.TRADES,
                  symbols = ["AAPL"], start = DateTime(2024,1,2,14,30),
                  end_   = DateTime(2024,1,2,14,31), stype_in = SType.RAW_SYMBOL)
to_csv(store, "trades.csv")
```
"""
to_csv(s::DBNStore, path::AbstractString)     = _via_temp_dbn(s, path, DBN.dbn_to_csv)

"""
    to_json(store, path)

Write the records in `store` to a newline-delimited JSON file at `path`. One
record per line; same temp-DBN bounce as [`to_csv`](@ref).
"""
to_json(s::DBNStore, path::AbstractString)    = _via_temp_dbn(s, path, DBN.dbn_to_json)

"""
    to_parquet(store, path)

Write the records in `store` to a Parquet file at `path`. Same temp-DBN bounce
as [`to_csv`](@ref); the column schema comes from DBN.jl's `dbn_to_parquet`.
"""
to_parquet(s::DBNStore, path::AbstractString) = _via_temp_dbn(s, path, DBN.dbn_to_parquet)
