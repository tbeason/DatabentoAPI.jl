# Batch endpoints — submit asynchronous jobs and download results.

function submit_job(c::Historical;
                    dataset::AbstractString,
                    symbols,
                    schema::Schema.T,
                    start,
                    end_ = nothing,
                    encoding::Union{Encoding.T,AbstractString} = "dbn",
                    compression::Union{Compression.T,AbstractString} = "zstd",
                    stype_in::SType.T = SType.RAW_SYMBOL,
                    stype_out::SType.T = SType.INSTRUMENT_ID,
                    split_duration::Union{Nothing,SplitDuration.T,AbstractString} = nothing,
                    split_size::Union{Nothing,Integer} = nothing,
                    split_symbols::Union{Nothing,Bool} = nothing,
                    packaging::Union{Nothing,Packaging.T,AbstractString} = nothing,
                    delivery::Union{Nothing,Delivery.T,AbstractString} = nothing,
                    limit::Union{Nothing,Integer} = nothing)
    enc_v = encoding isa Encoding.T ? encoding_str(encoding) : String(encoding)
    cmp_v = compression isa Compression.T ? compression_str(compression) : String(compression)
    sdur  = split_duration isa SplitDuration.T ? lowercase(String(Symbol(split_duration))) :
            split_duration isa AbstractString ? String(split_duration) : nothing
    pkg   = packaging isa Packaging.T ? lowercase(String(Symbol(packaging))) :
            packaging isa AbstractString ? String(packaging) : nothing
    dlv   = delivery isa Delivery.T ? lowercase(String(Symbol(delivery))) :
            delivery isa AbstractString ? String(delivery) : nothing

    body_pairs = _clean_params((
        dataset        = String(dataset),
        symbols        = symbols_str(symbols),
        schema         = schema_str(schema),
        start          = ts_str(start),
        end_           = ts_str(end_),
        encoding       = enc_v,
        compression    = cmp_v,
        stype_in       = stype_str(stype_in),
        stype_out      = stype_str(stype_out),
        split_duration = sdur,
        split_size     = split_size,
        split_symbols  = split_symbols,
        packaging      = pkg,
        delivery       = dlv,
        limit          = limit,
    ))
    for (i, (k, v)) in enumerate(body_pairs)
        k == "end_" && (body_pairs[i] = "end" => v)
    end
    return post_json(c.http, hist_path("batch.submit_job"); body = body_pairs)
end

function list_jobs(c::Historical;
                   states::Union{Nothing,AbstractVector,JobState.T,AbstractString} = nothing,
                   since::Union{Nothing,DateTime,AbstractString,Integer} = nothing)
    states_v = if states isa AbstractVector
        join((s isa JobState.T ? lowercase(String(Symbol(s))) : String(s) for s in states), ",")
    elseif states isa JobState.T
        lowercase(String(Symbol(states)))
    elseif states isa AbstractString
        String(states)
    else
        nothing
    end
    return get_json(c.http, hist_path("batch.list_jobs");
                    query = (states = states_v, since = ts_str(since)))
end

list_files(c::Historical; job_id::AbstractString) =
    get_json(c.http, hist_path("batch.list_files"); query = (; job_id = String(job_id)))

"""
    batch_download(client; job_id, output_dir)

Download every file for a completed batch job into `output_dir`. Returns the list of
written file paths. Named `batch_download` to avoid clashing with `Base.download`.
"""
function batch_download(c::Historical;
                        job_id::AbstractString,
                        output_dir::AbstractString)::Vector{String}
    isdir(output_dir) || mkpath(output_dir)
    files = list_files(c; job_id = String(job_id))
    out_paths = String[]
    for f in files
        filename = String(f["filename"])
        bytes = get_bytes(c.http, hist_path("batch.download");
                          query = (job_id = String(job_id), filename = filename))
        out = joinpath(String(output_dir), filename)
        open(out, "w") do io
            write(io, bytes)
        end
        push!(out_paths, out)
    end
    return out_paths
end
