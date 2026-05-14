# Metadata endpoints: dataset listings, schema info, cost/billing helpers.

list_publishers(c::Historical) =
    get_json(c.http, hist_path("metadata.list_publishers"))

function list_datasets(c::Historical;
                       start_date::Union{Nothing,Date,AbstractString} = nothing,
                       end_date::Union{Nothing,Date,AbstractString}   = nothing)
    return get_json(c.http, hist_path("metadata.list_datasets");
                    query = (start_date = ts_str(start_date), end_date = ts_str(end_date)))
end

list_schemas(c::Historical; dataset::AbstractString) =
    get_json(c.http, hist_path("metadata.list_schemas"); query = (; dataset = String(dataset)))

function list_fields(c::Historical;
                     schema::Union{Nothing,Schema.T,AbstractString} = nothing,
                     encoding::Union{Nothing,Encoding.T,AbstractString} = nothing)
    schema_v   = schema   isa Schema.T   ? schema_str(schema)     : (schema   === nothing ? nothing : String(schema))
    encoding_v = encoding isa Encoding.T ? encoding_str(encoding) : (encoding === nothing ? nothing : String(encoding))
    return get_json(c.http, hist_path("metadata.list_fields");
                    query = (schema = schema_v, encoding = encoding_v))
end

list_unit_prices(c::Historical; dataset::AbstractString) =
    get_json(c.http, hist_path("metadata.list_unit_prices"); query = (; dataset = String(dataset)))

get_dataset_range(c::Historical; dataset::AbstractString) =
    get_json(c.http, hist_path("metadata.get_dataset_range"); query = (; dataset = String(dataset)))

function get_dataset_condition(c::Historical;
                               dataset::AbstractString,
                               start_date::Union{Nothing,Date,AbstractString} = nothing,
                               end_date::Union{Nothing,Date,AbstractString}   = nothing)
    return get_json(c.http, hist_path("metadata.get_dataset_condition");
                    query = (dataset = String(dataset),
                             start_date = ts_str(start_date),
                             end_date = ts_str(end_date)))
end

function get_record_count(c::Historical;
                          dataset::AbstractString,
                          symbols,
                          schema::Schema.T,
                          start,
                          end_ = nothing,
                          stype_in::SType.T = SType.RAW_SYMBOL,
                          limit::Union{Nothing,Integer} = nothing)
    qpairs = _clean_params((
        dataset  = String(dataset),
        symbols  = symbols_str(symbols),
        schema   = schema_str(schema),
        stype_in = stype_str(stype_in),
        start    = ts_str(start),
        end_     = ts_str(end_),
        limit    = limit,
    ))
    for (i, (k, v)) in enumerate(qpairs)
        k == "end_" && (qpairs[i] = "end" => v)
    end
    return get_json(c.http, hist_path("metadata.get_record_count"); query = qpairs)
end

function get_billable_size(c::Historical;
                           dataset::AbstractString,
                           symbols,
                           schema::Schema.T,
                           start,
                           end_ = nothing,
                           stype_in::SType.T = SType.RAW_SYMBOL,
                           limit::Union{Nothing,Integer} = nothing)
    qpairs = _clean_params((
        dataset  = String(dataset),
        symbols  = symbols_str(symbols),
        schema   = schema_str(schema),
        stype_in = stype_str(stype_in),
        start    = ts_str(start),
        end_     = ts_str(end_),
        limit    = limit,
    ))
    for (i, (k, v)) in enumerate(qpairs)
        k == "end_" && (qpairs[i] = "end" => v)
    end
    return get_json(c.http, hist_path("metadata.get_billable_size"); query = qpairs)
end

function get_cost(c::Historical;
                  dataset::AbstractString,
                  symbols,
                  schema::Schema.T,
                  start,
                  end_ = nothing,
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
        start    = ts_str(start),
        end_     = ts_str(end_),
        mode     = mode_v,
        limit    = limit,
    ))
    for (i, (k, v)) in enumerate(qpairs)
        k == "end_" && (qpairs[i] = "end" => v)
    end
    return get_json(c.http, hist_path("metadata.get_cost"); query = qpairs)
end
