"""
    get_range(client; dataset, schema, symbols, start, end_=nothing,
              stype_in=SType.RAW_SYMBOL, stype_out=SType.INSTRUMENT_ID, limit=nothing)

Fetch a time range of records from the Databento Historical API. Returns a [`DBNStore`](@ref).

The endpoint streams zstd-compressed DBN bytes; this function decompresses and decodes
them in memory using DBN.jl. For very large ranges consider `submit_job` instead.
"""
function get_range(c::Historical;
                   dataset::AbstractString,
                   schema::Schema.T,
                   symbols,
                   start,
                   end_ = nothing,
                   stype_in::SType.T  = SType.RAW_SYMBOL,
                   stype_out::SType.T = SType.INSTRUMENT_ID,
                   limit::Union{Nothing,Integer} = nothing)::DBNStore
    query = (
        dataset     = String(dataset),
        symbols     = symbols_str(symbols),
        schema      = schema_str(schema),
        stype_in    = stype_str(stype_in),
        stype_out   = stype_str(stype_out),
        start       = ts_str(start),
        end_        = ts_str(end_),
        limit       = limit,
        encoding    = "dbn",
        compression = "zstd",
    )
    # `end_` → `end` on the wire (Julia keyword conflict).
    qpairs = _clean_params(query)
    for (i, (k, v)) in enumerate(qpairs)
        k == "end_" && (qpairs[i] = "end" => v)
    end

    bytes = get_bytes(c.http, hist_path("timeseries.get_range");
                      query = qpairs,
                      accept = "application/octet-stream")
    return decode_dbn_bytes(bytes; zstd = true)
end
