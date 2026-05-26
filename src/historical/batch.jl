# Batch endpoints — submit asynchronous jobs and download results.

"""
    submit_job(client; dataset, symbols, schema, start, end_=nothing, ...) -> Dict

Submit an asynchronous batch job. The Historical API queues the request and
returns a `job_id`; poll [`list_jobs`](@ref) until the [`JobState`](@ref) is
`DONE`, then call [`list_files`](@ref) and [`batch_download`](@ref) to fetch
the results.

Useful when the synchronous [`get_range`](@ref) would be too large to stream
back in one HTTP response. Cost is the same as the equivalent `get_range`
query; preview it with [`get_cost`](@ref) before submitting.

Optional kwargs control output framing:

  - `encoding`     — `"dbn"` (default), `"csv"`, `"json"`.
  - `compression`  — `"zstd"` (default), `"none"`, `"zip"`.
  - `stype_out`    — output symbology (default `SType.INSTRUMENT_ID`).
  - `split_duration`/`split_size`/`split_symbols` — slice the output across
    multiple files; see [`SplitDuration`](@ref).
  - `packaging`    — bundle files for download; see [`Packaging`](@ref).
  - `delivery`     — destination; see [`Delivery`](@ref).
  - `limit`        — cap on records returned.

Wraps `POST /v0/batch.submit_job`.
"""
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

"""
    list_jobs(client; states=nothing, since=nothing)

List batch jobs submitted under the calling API key, optionally filtered
to one or more [`JobState`](@ref) values (`states` accepts a single state,
a vector, or a comma-separated string) and/or to jobs created after `since`
(`DateTime`, ISO-8601 string, or unix-ns integer). Returns a JSON array of
job objects.
Wraps `GET /v0/batch.list_jobs`.
"""
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

"""
    list_files(client; job_id)

List the output files for a batch job identified by `job_id`. Each entry
includes `filename`, `size`, `hash` (sha256), and a `urls.https` download
URL. Used internally by [`batch_download`](@ref) but exposed for callers
that need to inspect or selectively fetch files.
Wraps `GET /v0/batch.list_files`.
"""
list_files(c::Historical; job_id::AbstractString) =
    get_json(c.http, hist_path("batch.list_files"); query = (; job_id = String(job_id)))

"""
    batch_download(client; job_id, output_dir, verify_hash=true,
                   filenames=nothing, overwrite=false)

Download files from a completed batch job into `output_dir`. Returns the list of
written file paths.

Each file's download URL is taken from `list_files`' `urls.https` field. The
response also includes the file's sha256 hash; when `verify_hash=true` the
downloaded bytes are checked against it.

By default every file in the job is downloaded. Pass `filenames` to restrict to a
subset (matched by exact `filename`). Pass `overwrite=true` to re-download a file
that's already present and the right size.

Named `batch_download` to avoid clashing with `Base.download`.
"""
function batch_download(c::Historical;
                        job_id::AbstractString,
                        output_dir::AbstractString,
                        verify_hash::Bool = true,
                        filenames::Union{Nothing,AbstractVector{<:AbstractString}} = nothing,
                        overwrite::Bool = false)::Vector{String}
    isdir(output_dir) || mkpath(output_dir)
    files = list_files(c; job_id = String(job_id))
    want = filenames === nothing ? nothing : Set(String.(filenames))
    out_paths = String[]
    for f in files
        filename = String(f["filename"])
        want === nothing || filename in want || continue
        out = joinpath(String(output_dir), filename)
        expected_size = Int(f["size"])
        if !overwrite && isfile(out) && stat(out).size == expected_size
            push!(out_paths, out)
            continue
        end
        urls = f["urls"]
        url  = String(urls["https"])
        # Fetch the file directly; the URL is on a different host (api.databento.com)
        # but uses the same HTTP basic auth.
        resp = c.http.dispatcher("GET", url,
            ["Authorization" => basic_auth_header(c.http.api_key),
             "Accept"        => "application/octet-stream",
             "User-Agent"    => c.http.user_agent],
            UInt8[];
            status_exception = false,
            readtimeout      = c.http.timeout,
            connect_timeout  = c.http.connect_timeout,
            retry            = false,
            decompress       = false)
        if resp.status >= 400
            T = resp.status < 500 ? BentoClientError : BentoServerError
            throw(http_error_from_response(T, resp.status,
                                           String(copy(resp.body)), _request_id(resp)))
        end
        bytes = Vector{UInt8}(resp.body)
        if verify_hash
            hash_field = String(get(f, "hash", ""))
            if startswith(hash_field, "sha256:")
                expected = lowercase(hash_field[8:end])
                got      = lowercase(bytes2hex(SHA.sha256(bytes)))
                got == expected ||
                    throw(ErrorException("sha256 mismatch for $filename: " *
                                         "expected $expected, got $got"))
            end
        end
        open(out, "w") do io
            write(io, bytes)
        end
        push!(out_paths, out)
    end
    return out_paths
end
