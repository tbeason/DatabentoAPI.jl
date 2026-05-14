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
