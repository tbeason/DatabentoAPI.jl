# Metadata endpoints: dataset listings, schema info, cost/billing helpers.

"""
    list_publishers(client::Historical)

List every Databento publisher (venue / data provider). Returns a JSON array
of objects with at least `publisher_id`, `dataset`, and `description`.
Wraps `GET /v0/metadata.list_publishers`.
"""
list_publishers(c::Historical) =
    get_json(c.http, hist_path("metadata.list_publishers"))

"""
    list_datasets(client; start_date=nothing, end_date=nothing)

List Databento datasets, optionally filtered to those that have data in the
`[start_date, end_date]` window. Returns a JSON array of dataset names
(`"XNAS.ITCH"`, `"OPRA.PILLAR"`, …).
Wraps `GET /v0/metadata.list_datasets`.
"""
function list_datasets(c::Historical;
                       start_date::Union{Nothing,Date,AbstractString} = nothing,
                       end_date::Union{Nothing,Date,AbstractString}   = nothing)
    return get_json(c.http, hist_path("metadata.list_datasets");
                    query = (start_date = ts_str(start_date), end_date = ts_str(end_date)))
end

"""
    list_schemas(client; dataset)

List the schemas offered by `dataset` (e.g. `["trades", "tbbo", "mbp-1", ...]`).
Wraps `GET /v0/metadata.list_schemas`.
"""
list_schemas(c::Historical; dataset::AbstractString) =
    get_json(c.http, hist_path("metadata.list_schemas"); query = (; dataset = String(dataset)))

"""
    list_fields(client; schema=nothing, encoding=nothing)

List the fields available for a given (`schema`, `encoding`) combination.
`schema` accepts a `Schema` value or its lowercase string; `encoding`
accepts an `Encoding` value or its lowercase string. Either kwarg can
be `nothing` to broaden the query. Wraps `GET /v0/metadata.list_fields`.
"""
function list_fields(c::Historical;
                     schema::Union{Nothing,Schema.T,AbstractString} = nothing,
                     encoding::Union{Nothing,Encoding.T,AbstractString} = nothing)
    schema_v   = schema   isa Schema.T   ? schema_str(schema)     : (schema   === nothing ? nothing : String(schema))
    encoding_v = encoding isa Encoding.T ? encoding_str(encoding) : (encoding === nothing ? nothing : String(encoding))
    return get_json(c.http, hist_path("metadata.list_fields");
                    query = (schema = schema_v, encoding = encoding_v))
end

"""
    list_unit_prices(client; dataset)

Per-feed-mode and per-schema price list for `dataset` (USD per GB). Useful as
an input to manual cost calculations; for a quote on a specific query, prefer
[`get_cost`](@ref). Wraps `GET /v0/metadata.list_unit_prices`.
"""
list_unit_prices(c::Historical; dataset::AbstractString) =
    get_json(c.http, hist_path("metadata.list_unit_prices"); query = (; dataset = String(dataset)))

"""
    get_dataset_range(client; dataset)

Earliest and latest available timestamps for `dataset`. Use the result to
bound `start_dt`/`end_dt` on subsequent [`get_range`](@ref) or [`submit_job`](@ref)
calls. Wraps `GET /v0/metadata.get_dataset_range`.
"""
get_dataset_range(c::Historical; dataset::AbstractString) =
    get_json(c.http, hist_path("metadata.get_dataset_range"); query = (; dataset = String(dataset)))

"""
    get_dataset_condition(client; dataset, start_date=nothing, end_date=nothing)

Per-date data quality / availability status for `dataset` in the
`[start_date, end_date]` window. Helpful for skipping known-bad days
before running a long backfill. Wraps `GET /v0/metadata.get_dataset_condition`.
"""
function get_dataset_condition(c::Historical;
                               dataset::AbstractString,
                               start_date::Union{Nothing,Date,AbstractString} = nothing,
                               end_date::Union{Nothing,Date,AbstractString}   = nothing)
    return get_json(c.http, hist_path("metadata.get_dataset_condition");
                    query = (dataset = String(dataset),
                             start_date = ts_str(start_date),
                             end_date = ts_str(end_date)))
end

"""
    get_record_count(client; dataset, symbols, schema, start_dt, end_dt=nothing,
                     stype_in=SType.RAW_SYMBOL, limit=nothing)

Count of records the same query would return from [`get_range`](@ref) without
actually downloading them. Free; use this before issuing an expensive
[`get_range`](@ref) on an unknown date range.
Wraps `GET /v0/metadata.get_record_count`.
"""
function get_record_count(c::Historical;
                          dataset::AbstractString,
                          symbols,
                          schema::Schema.T,
                          start_dt,
                          end_dt = nothing,
                          stype_in::SType.T = SType.RAW_SYMBOL,
                          limit::Union{Nothing,Integer} = nothing)
    qpairs = _clean_params((
        dataset  = String(dataset),
        symbols  = symbols_str(symbols),
        schema   = schema_str(schema),
        stype_in = stype_str(stype_in),
        start    = ts_str(start_dt),
        end_     = ts_str(end_dt),
        limit    = limit,
    ))
    for (i, (k, v)) in enumerate(qpairs)
        k == "end_" && (qpairs[i] = "end" => v)
    end
    return get_json(c.http, hist_path("metadata.get_record_count"); query = qpairs)
end

"""
    get_billable_size(client; dataset, symbols, schema, start_dt, end_dt=nothing,
                      stype_in=SType.RAW_SYMBOL, limit=nothing)

Billable size in bytes (uncompressed CSV-equivalent) the same query would
return from [`get_range`](@ref). Free; combine with [`list_unit_prices`](@ref)
or use [`get_cost`](@ref) directly for a dollar estimate.
Wraps `GET /v0/metadata.get_billable_size`.
"""
function get_billable_size(c::Historical;
                           dataset::AbstractString,
                           symbols,
                           schema::Schema.T,
                           start_dt,
                           end_dt = nothing,
                           stype_in::SType.T = SType.RAW_SYMBOL,
                           limit::Union{Nothing,Integer} = nothing)
    qpairs = _clean_params((
        dataset  = String(dataset),
        symbols  = symbols_str(symbols),
        schema   = schema_str(schema),
        stype_in = stype_str(stype_in),
        start    = ts_str(start_dt),
        end_     = ts_str(end_dt),
        limit    = limit,
    ))
    for (i, (k, v)) in enumerate(qpairs)
        k == "end_" && (qpairs[i] = "end" => v)
    end
    return get_json(c.http, hist_path("metadata.get_billable_size"); query = qpairs)
end

"""
    get_cost(client; dataset, symbols, schema, start_dt, end_dt=nothing,
             mode=nothing, stype_in=SType.RAW_SYMBOL, limit=nothing)

Dollar cost preview for the same query as [`get_range`](@ref) under the
given [`FeedMode`](@ref) (default `HISTORICAL`). Returns a number in USD.
Free; call before issuing large queries.
Wraps `GET /v0/metadata.get_cost`.
"""
function get_cost(c::Historical;
                  dataset::AbstractString,
                  symbols,
                  schema::Schema.T,
                  start_dt,
                  end_dt = nothing,
                  mode::Union{Nothing,FeedMode.T,AbstractString} = nothing,
                  stype_in::SType.T = SType.RAW_SYMBOL,
                  limit::Union{Nothing,Integer} = nothing)
    mode_v = mode isa FeedMode.T ? lowercase(String(Symbol(mode))) :
             mode isa AbstractString ? String(mode) : nothing
    qpairs = _clean_params((
        dataset  = String(dataset),
        symbols  = symbols_str(symbols),
        schema   = schema_str(schema),
        stype_in = stype_str(stype_in),
        start    = ts_str(start_dt),
        end_     = ts_str(end_dt),
        mode     = mode_v,
        limit    = limit,
    ))
    for (i, (k, v)) in enumerate(qpairs)
        k == "end_" && (qpairs[i] = "end" => v)
    end
    return get_json(c.http, hist_path("metadata.get_cost"); query = qpairs)
end
