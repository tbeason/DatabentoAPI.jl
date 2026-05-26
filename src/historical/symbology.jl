"""
    resolve(client; dataset, symbols, stype_in, stype_out,
            start_date, end_date=nothing)

Resolve `symbols` from `stype_in` (e.g. `SType.RAW_SYMBOL`) to
`stype_out` (e.g. `SType.INSTRUMENT_ID`) within the
`[start_date, end_date]` date range. Returns a JSON object whose `result`
field maps each input symbol to a list of `(d0, d1, s)` intervals where
the mapping is valid (instruments expire, roll, get renamed) — see
[`SymbologyResolution`](@ref) for the per-symbol status convention.

Example:

```julia
resolve(client; dataset = "GLBX.MDP3", symbols = ["ES.FUT"],
        stype_in = SType.PARENT, stype_out = SType.INSTRUMENT_ID,
        start_date = Date(2024,1,2), end_date = Date(2024,1,5))
```

Wraps `POST /v0/symbology.resolve`.
"""
function resolve(c::Historical;
                 dataset::AbstractString,
                 symbols,
                 stype_in::SType.T,
                 stype_out::SType.T,
                 start_date::Union{Date,AbstractString},
                 end_date::Union{Nothing,Date,AbstractString} = nothing)
    body = _clean_params((
        dataset    = String(dataset),
        symbols    = symbols_str(symbols),
        stype_in   = stype_str(stype_in),
        stype_out  = stype_str(stype_out),
        start_date = ts_str(start_date),
        end_date   = ts_str(end_date),
    ))
    return post_json(c.http, hist_path("symbology.resolve"); body = body)
end
