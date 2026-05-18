"""
    get_range(client; dataset, schema, symbols, start, end_=nothing,
              stype_in=SType.RAW_SYMBOL, stype_out=SType.INSTRUMENT_ID,
              limit=nothing, typed=true)

Fetch a time range of records from the Databento Historical API. Returns a [`DBNStore`](@ref).

The endpoint streams zstd-compressed DBN bytes; this function decompresses and decodes
them in memory using DBN.jl. For very large ranges consider `submit_job` instead.

When `typed=true` (default) and the schema is type-pure (almost all schemas are,
the exception is `Schema.MIX`), the records vector is `Vector{T}` for the
concrete record type — roughly 10× faster decode and 60% less allocation than
the generic path. Pass `typed=false` to force the legacy `Vector{DBN.DBNRecord}`
return for callers that rely on the Union element type.
"""
function get_range(c::Historical;
                   dataset::AbstractString,
                   schema::Schema.T,
                   symbols,
                   start,
                   end_ = nothing,
                   stype_in::SType.T  = SType.RAW_SYMBOL,
                   stype_out::SType.T = SType.INSTRUMENT_ID,
                   limit::Union{Nothing,Integer} = nothing,
                   typed::Bool = true)
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
    T = typed ? record_type_for_schema(schema) : nothing
    if T === nothing
        return decode_dbn_bytes(bytes; zstd = true)
    else
        return decode_dbn_bytes(bytes, T; zstd = true)
    end
end

"""
    record_type_for_schema(schema)

Return the concrete `DBN` record struct that a given `Schema` produces. Returns
`nothing` for schemas that mix record types (e.g. `Schema.MIX`). Used by
[`foreach_record`](@ref) to enable the zero-allocation type-specific decode path.
"""
function record_type_for_schema(schema::Schema.T)
    schema == Schema.TRADES     && return DBN.TradeMsg
    schema == Schema.MBO        && return DBN.MBOMsg
    schema == Schema.MBP_1      && return DBN.MBP1Msg
    schema == Schema.MBP_10     && return DBN.MBP10Msg
    schema == Schema.TBBO       && return DBN.MBP1Msg
    schema == Schema.OHLCV_1S   && return DBN.OHLCVMsg
    schema == Schema.OHLCV_1M   && return DBN.OHLCVMsg
    schema == Schema.OHLCV_1H   && return DBN.OHLCVMsg
    schema == Schema.OHLCV_1D   && return DBN.OHLCVMsg
    schema == Schema.DEFINITION && return DBN.InstrumentDefMsg
    schema == Schema.STATISTICS && return DBN.StatMsg
    schema == Schema.STATUS     && return DBN.StatusMsg
    schema == Schema.IMBALANCE  && return DBN.ImbalanceMsg
    schema == Schema.CMBP_1     && return DBN.CMBP1Msg
    schema == Schema.CBBO_1S    && return DBN.CBBO1sMsg
    schema == Schema.CBBO_1M    && return DBN.CBBO1mMsg
    schema == Schema.TCBBO      && return DBN.TCBBOMsg
    schema == Schema.BBO_1S     && return DBN.BBO1sMsg
    schema == Schema.BBO_1M     && return DBN.BBO1mMsg
    return nothing
end

"""
    foreach_record(f, client::Historical; dataset, schema, symbols, start, end_=nothing,
                   stype_in=SType.RAW_SYMBOL, stype_out=SType.INSTRUMENT_ID,
                   limit=nothing, record_type=nothing)

Streaming variant of [`get_range`](@ref). Calls `f(record)` for each `DBN` record as
it arrives off the wire — overlaps HTTP download with decompress + decode and never
materialises the compressed payload or the records vector in memory. Use this for
very large queries where `get_range`'s in-memory buffering would be wasteful.

By default, the concrete record type is inferred from `schema` (e.g.
`Schema.TRADES → DBN.TradeMsg`) and the type-specific zero-allocation decode path
is used (~2× faster than generic dispatch). Pass `record_type = DBN.DBNRecord` to
force the generic Union-typed path, or `record_type = SomeT` to override.

Returns the `DBN.Metadata` from the response header.
"""
function foreach_record(f, c::Historical;
                        dataset::AbstractString,
                        schema::Schema.T,
                        symbols,
                        start,
                        end_ = nothing,
                        stype_in::SType.T  = SType.RAW_SYMBOL,
                        stype_out::SType.T = SType.INSTRUMENT_ID,
                        limit::Union{Nothing,Integer} = nothing,
                        record_type::Union{Nothing,Type} = nothing)::DBN.Metadata
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
    qpairs = _clean_params(query)
    for (i, (k, v)) in enumerate(qpairs)
        k == "end_" && (qpairs[i] = "end" => v)
    end
    T = record_type === nothing ? record_type_for_schema(schema) : record_type
    return open_stream(c.http, hist_path("timeseries.get_range");
                       query = qpairs,
                       accept = "application/octet-stream") do body
        decompressed = TranscodingStream(ZstdDecompressor(), body)
        decoder = DBN.DBNDecoder(decompressed)
        DBN.read_header!(decoder)
        if T === nothing || T === DBN.DBNRecord
            while true
                rec = DBN.read_record(decoder)
                rec === nothing && break
                f(rec)
            end
        else
            DBN._foreach_record_impl(f, decoder, T)
        end
        decoder.metadata
    end
end
