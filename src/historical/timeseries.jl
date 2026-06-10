"""
    get_range(client; dataset, schema, symbols, start_dt, end_dt=nothing,
              stype_in=SType.RAW_SYMBOL, stype_out=SType.INSTRUMENT_ID,
              limit=nothing, typed=true, size_hint=nothing,
              chunk=nothing, concurrency=8)

Fetch a time range of records from the Databento Historical API. Returns a [`DBNStore`](@ref).

The endpoint streams zstd-compressed DBN bytes; this function decompresses and decodes
them in memory using DBN.jl. For very large ranges consider `submit_job` instead.

When `typed=true` (default) and the schema is type-pure (almost all schemas are,
the exception is `Schema.MIX`), the records vector is `Vector{T}` for the
concrete record type — roughly 10× faster decode and 60% less allocation than
the generic path. Pass `typed=false` to force the legacy `Vector{DBN.DBNRecord}`
return for callers that rely on the Union element type.

The typed path tolerates records the gateway legitimately interleaves with the
data: `ErrorMsg` records are logged with `@warn` (their text usually explains
why data is missing), `SystemMsg`/`SymbolMappingMsg` are skipped quietly, and
any other mismatched or unknown record types are skipped with one summary
warning rather than throwing.

`size_hint` lets callers who know the record-count bound (e.g. from a prior
`get_record_count` call) pre-size the records vector exactly. The default
heuristic over-allocates by 10–20% to avoid realloc churn under growth; an
exact hint avoids that waste.

!!! note "Long ranges and the read timeout"
    The gateway can spend minutes on server-side assembly (continuous-symbol
    resolution, day-partitioned scans) before streaming the first byte. If
    that exceeds the client's read timeout (default 600s) the call raises
    [`BentoTimeoutError`](@ref) — construct `Historical(timeout = ...)` with a
    larger budget or reduce the range.

# Chunked, concurrent fetching

Every `get_range` request carries a fixed server-side assembly latency that for
continuous-symbol queries can run ~25–30s regardless of payload size, so a
long range fetched as one request risks the read timeout, and fetched as
sequential chunks accumulates an hour of fixed latency over a hundred calls.
Pass `chunk` (a `Dates.Period`, e.g. `Year(1)` or `Month(1)`) to split
`[start_dt, end_dt)` into half-open chunks fetched concurrently — at most
`concurrency` (default 8) requests in flight at once:

```julia
store = get_range(client; dataset = "GLBX.MDP3", schema = Schema.STATISTICS,
                  symbols = ["ES.n.0"], stype_in = SType.CONTINUOUS,
                  start_dt = Date(2010, 1, 1), end_dt = Date(2025, 1, 1),
                  chunk = Year(1))
```

Records are concatenated in time order. A chunk that fails after the client's
usual retries is warned about and recorded in the returned store's
`failed_ranges` field (a `Vector{Tuple{DateTime,DateTime}}`) so you can
re-fetch exactly the missing ranges; the call only throws if *every* chunk
failed. Constraints: `chunk` requires an explicit `end_dt`, `start_dt`/`end_dt`
as `DateTime`/`Date`/parseable strings (not unix-ns integers), and is
incompatible with `limit` (a record cap can't be apportioned across chunks);
`size_hint` is ignored in chunked mode. For multi-GB pulls `submit_job` remains
the better tool.
"""
function get_range(c::Historical;
                   dataset::AbstractString,
                   schema::Schema.T,
                   symbols,
                   start_dt,
                   end_dt = nothing,
                   stype_in::SType.T  = SType.RAW_SYMBOL,
                   stype_out::SType.T = SType.INSTRUMENT_ID,
                   limit::Union{Nothing,Integer} = nothing,
                   typed::Bool = true,
                   size_hint::Union{Nothing,Integer} = nothing,
                   chunk::Union{Nothing,Dates.Period} = nothing,
                   concurrency::Integer = 8)
    chunk === nothing &&
        return _get_range_single(c; dataset, schema, symbols, start_dt, end_dt,
                                 stype_in, stype_out, limit, typed, size_hint)
    return _get_range_chunked(c; dataset, schema, symbols, start_dt, end_dt,
                              stype_in, stype_out, limit, typed, chunk, concurrency)
end

# The original single-request fetch: one HTTP GET, buffer the zstd payload,
# decode in memory. Chunked mode calls this once per chunk.
function _get_range_single(c::Historical;
                           dataset::AbstractString,
                           schema::Schema.T,
                           symbols,
                           start_dt,
                           end_dt,
                           stype_in::SType.T,
                           stype_out::SType.T,
                           limit::Union{Nothing,Integer},
                           typed::Bool,
                           size_hint::Union{Nothing,Integer})
    query = (
        dataset     = String(dataset),
        symbols     = symbols_str(symbols),
        schema      = schema_str(schema),
        stype_in    = stype_str(stype_in),
        stype_out   = stype_str(stype_out),
        start       = ts_str(start_dt),
        end_        = ts_str(end_dt),
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
        return decode_dbn_bytes(bytes, T; zstd = true, size_hint = size_hint)
    end
end

# Normalize a chunk endpoint to DateTime. Chunk boundaries are computed with
# Period arithmetic, so the endpoints must be calendar values — unix-ns
# integers are accepted by the single-request path (ts_str passes them
# through) but can't be split without assuming a timezone, so reject them.
_chunk_datetime(d::DateTime) = d
_chunk_datetime(d::Date) = DateTime(d)
_chunk_datetime(s::AbstractString) =
    try
        DateTime(s)
    catch
        DateTime(Date(s))
    end
_chunk_datetime(x) =
    throw(ArgumentError("chunked get_range requires start_dt/end_dt as DateTime, " *
                        "Date, or a parseable string; got $(typeof(x))"))

# Merge per-chunk response metadata into one header for the combined store.
# Identity fields (version/dataset/schema/stypes/ts_out) are taken from the
# first success; the time range spans all chunks; the symbol-resolution
# vectors are unioned because they legitimately differ per date-chunk under
# continuous/parent symbology (a contract that rolls mid-range maps
# differently in different chunks).
function _merge_chunk_metadata(mds::Vector{DBN.Metadata})
    md1 = first(mds)
    end_ts_vals = Int64[md.end_ts for md in mds if md.end_ts !== nothing]
    return DBN.Metadata(
        md1.version, md1.dataset, md1.schema,
        minimum(md.start_ts for md in mds),
        isempty(end_ts_vals) ? nothing : maximum(end_ts_vals),
        nothing,   # a per-chunk limit would be meaningless for the union
        md1.stype_in, md1.stype_out, md1.ts_out,
        unique(reduce(vcat, (md.symbols for md in mds))),
        unique(reduce(vcat, (md.partial for md in mds))),
        unique(reduce(vcat, (md.not_found for md in mds))),
        unique(reduce(vcat, (md.mappings for md in mds))),
    )
end

function _get_range_chunked(c::Historical;
                            dataset::AbstractString,
                            schema::Schema.T,
                            symbols,
                            start_dt,
                            end_dt,
                            stype_in::SType.T,
                            stype_out::SType.T,
                            limit::Union{Nothing,Integer},
                            typed::Bool,
                            chunk::Dates.Period,
                            concurrency::Integer)
    limit === nothing ||
        throw(ArgumentError("limit is not supported with chunk: a record cap cannot " *
                            "be apportioned across chunk requests"))
    end_dt === nothing &&
        throw(ArgumentError("chunked get_range requires an explicit end_dt"))
    concurrency >= 1 || throw(ArgumentError("concurrency must be ≥ 1"))

    start = _chunk_datetime(start_dt)
    stop  = _chunk_datetime(end_dt)
    start < stop || throw(ArgumentError("start_dt must be before end_dt"))
    start + chunk > start || throw(ArgumentError("chunk must be a positive Period"))

    # Half-open [t, min(t+chunk, stop)) chunks compose exactly: Databento
    # ranges are themselves [start, end), so no record is duplicated or lost
    # at the seams and the concatenation needs no dedup.
    chunks = Tuple{DateTime,DateTime}[]
    let t = start
        while t < stop
            nxt = min(t + chunk, stop)
            push!(chunks, (t, nxt))
            t = nxt
        end
    end

    # Fetch concurrently, gated by a semaphore. Each task writes only its own
    # index slot, so no lock is needed and chunk order (= time order) is
    # preserved for assembly. HTTPClient is safe to share across tasks, and
    # its built-in 429 retry-with-backoff absorbs rate-limit brushes from the
    # concurrency. Threads.@spawn (vs @async) lets the CPU-bound zstd+decode
    # work parallelize when threads are available; on a single-threaded
    # runtime it still overlaps the network waits.
    sem = Base.Semaphore(concurrency)
    results = Vector{Any}(nothing, length(chunks))
    @sync for (i, (cs, ce)) in pairs(chunks)
        Threads.@spawn Base.acquire(sem) do
            results[i] = try
                _get_range_single(c; dataset, schema, symbols,
                                  start_dt = cs, end_dt = ce,
                                  stype_in, stype_out,
                                  limit = nothing, typed, size_hint = nothing)
            catch e
                e isa InterruptException && rethrow()
                hint = e isa BentoTimeoutError ? " (consider a smaller chunk)" : ""
                @warn "get_range chunk failed; continuing with remaining chunks$hint" start_dt = cs end_dt = ce exception = e
                e
            end
        end
    end

    success_idx = [i for i in eachindex(results) if results[i] isa DBNStore]
    if isempty(success_idx)
        # An empty store would mask total failure — surface the first error.
        throw(first(e for e in results if e isa Exception))
    end

    R = eltype(results[first(success_idx)].records)
    records = Vector{R}()
    sizehint!(records, sum(i -> length(results[i].records), success_idx))
    for i in success_idx   # index order == chunk order == time order
        append!(records, results[i].records)
    end

    failed = Tuple{DateTime,DateTime}[chunks[i] for i in eachindex(results)
                                      if !(results[i] isa DBNStore)]
    isempty(failed) ||
        @warn "get_range: $(length(failed)) of $(length(chunks)) chunks failed; their ranges are in the returned store's failed_ranges for retry"

    md = _merge_chunk_metadata(DBN.Metadata[results[i].metadata for i in success_idx])
    return DBNStore{R}(md, records, failed)
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
    foreach_record(f, client::Historical; dataset, schema, symbols, start_dt, end_dt=nothing,
                   stype_in=SType.RAW_SYMBOL, stype_out=SType.INSTRUMENT_ID,
                   limit=nothing, record_type=nothing)

Streaming variant of [`get_range`](@ref). Calls `f(record)` for each `DBN` record as
it arrives off the wire — interleaves HTTP download with decompress + decode and never
materialises the compressed payload or the records vector in memory. Records are
pulled from the socket only as fast as `f` consumes them (backpressure), so a slow
consumer throttles the download rather than buffering it. Use this for very large
queries where `get_range`'s in-memory buffering would be wasteful.

By default, the concrete record type is inferred from `schema` (e.g.
`Schema.TRADES → DBN.TradeMsg`) and the type-specific zero-allocation decode path
is used (~2× faster than generic dispatch). Pass `record_type = DBN.DBNRecord` to
force the generic Union-typed path, or `record_type = SomeT` to override.

Like [`get_range`](@ref), the typed path tolerates interleaved non-schema
records: gateway `ErrorMsg` records are `@warn`-logged,
`SystemMsg`/`SymbolMappingMsg` skipped quietly, and other mismatched rtypes
skipped with a single summary warning — `f` only ever sees the schema's record
type. (Consequently an explicitly wrong `record_type` override yields zero
callbacks plus the summary warning rather than an error.)

Returns the `DBN.Metadata` from the response header.
"""
function foreach_record(f, c::Historical;
                        dataset::AbstractString,
                        schema::Schema.T,
                        symbols,
                        start_dt,
                        end_dt = nothing,
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
        start       = ts_str(start_dt),
        end_        = ts_str(end_dt),
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
            _foreach_typed_tolerant(f, decoder, T)
        end
        decoder.metadata
    end
end
