"""
    DBNStore{T}(metadata, records::Vector{T})

A bundle of decoded DBN data: the file's `Metadata` plus the parsed records.
Returned by `get_range`. Iterates over `records`.

When the schema is type-pure (the common case), `T` is the concrete record
struct (e.g. `DBN.TradeMsg`) and the records vector is type-stable. For
mixed-record streams or when `get_range` is called with `typed=false`, `T`
falls back to the `DBN.DBNRecord` Union.

`failed_ranges` is only populated by chunked `get_range` calls (the `chunk`
kwarg): each entry is the `(start_dt, end_dt)` of a chunk whose request failed
after retries, so the caller can re-fetch exactly the missing ranges. It is
always empty for single-request fetches.
"""
struct DBNStore{T}
    metadata::DBN.Metadata
    records::Vector{T}
    failed_ranges::Vector{Tuple{DateTime,DateTime}}
end

# 2-arg forms preserve the pre-failed_ranges construction API.
DBNStore(metadata, records::Vector{T}) where {T} =
    DBNStore{T}(metadata, records, Tuple{DateTime,DateTime}[])
DBNStore{T}(metadata, records::Vector{T}) where {T} =
    DBNStore{T}(metadata, records, Tuple{DateTime,DateTime}[])

Base.length(s::DBNStore) = length(s.records)
Base.iterate(s::DBNStore, st...) = iterate(s.records, st...)
Base.eltype(::Type{DBNStore{T}}) where {T} = T
Base.eltype(::Type{DBNStore}) = DBN.DBNRecord
Base.firstindex(s::DBNStore) = firstindex(s.records)
Base.lastindex(s::DBNStore) = lastindex(s.records)
Base.getindex(s::DBNStore, i) = getindex(s.records, i)

function Base.show(io::IO, s::DBNStore{T}) where {T}
    print(io, "DBNStore{", T, "}(dataset=", s.metadata.dataset,
              ", schema=", s.metadata.schema,
              ", n=", length(s.records))
    isempty(s.failed_ranges) || print(io, ", failed_chunks=", length(s.failed_ranges))
    print(io, ")")
end

# Typed record loop that tolerates interleaved non-schema records.
#
# Historical responses are *mostly* type-pure, but the gateway can legitimately
# interleave control records — ErrorMsg (e.g. partial continuous-symbol
# resolution), SystemMsg, SymbolMappingMsg — with the data (#30). DBN's
# `_foreach_record_impl` throws on the first rtype mismatch, which forced
# callers back to `typed=false` (10× slower) and silently discarded the
# ErrorMsg text explaining *why* data was missing. This loop keeps the
# zero-allocation typed hot path (peek header → read straight into a reused
# Ref{T} buffer) and handles everything else by category:
#
#   - ErrorMsg            → per-record @warn (rare, and the content is the
#                           whole point: it explains gaps in the response)
#   - SystemMsg / SymbolMappingMsg → decode + @debug (routine noise)
#   - other known rtypes  → skip by header length, count, one summary @warn
#   - unknown raw rtypes  → skip by header length, count, one summary @warn
#
# Mismatched data records get a single post-loop summary, never a per-record
# warn — a wrong `record_type` override against a million-record stream must
# not emit a million log lines. The summary (rather than silence) matters for
# data-loss visibility: a skipped data rtype means the caller's type and the
# stream disagree.
#
# Unlike DBN's loop, there is deliberately no `finally` that closes
# `decoder.io` — our decoders wrap HTTP bodies / TranscodingStreams whose
# lifecycle is owned by `open_stream` / the caller, not by this function.
function _foreach_typed_tolerant(f, decoder::DBN.DBNDecoder, ::Type{T}) where {T}
    expected = DBN._type_to_rtype_stream(T)
    buffer = Ref{T}()   # reused per record: the typed read is allocation-free
    skipped = 0
    while !eof(decoder.io)
        hd_result = DBN.read_record_header(decoder.io)
        # Unknown raw rtype byte: read_record_header returns a tuple after
        # consuming only the 2 (length, rtype) bytes; length is in 4-byte units.
        if hd_result isa Tuple
            _, _, len_units = hd_result
            skip(decoder.io, Int(len_units) * DBN.LENGTH_MULTIPLIER - 2)
            skipped += 1
            continue
        end
        hd = hd_result
        rt = hd.rtype
        if rt == expected ||
           (T === DBN.OHLCVMsg && (rt == DBN.RType.OHLCV_1S_MSG || rt == DBN.RType.OHLCV_1M_MSG ||
                                   rt == DBN.RType.OHLCV_1H_MSG || rt == DBN.RType.OHLCV_1D_MSG))
            buffer[] = DBN._read_typed_record_stream(decoder, T, hd)
            f(buffer[])
        elseif rt == DBN.RType.ERROR_MSG
            rec = DBN.read_error_msg(decoder, hd)
            @warn "Databento gateway error record interleaved in response stream" err = rec.err
        elseif rt == DBN.RType.SYSTEM_MSG || rt == DBN.RType.SYMBOL_MAPPING_MSG
            rec = DBN.read_record_dispatch(decoder, hd, rt)
            @debug "Skipping control record in typed decode" rtype = rt
        else
            # Known data rtype that doesn't match the schema's record type.
            # The full 16-byte header is already consumed; hd.length is in
            # 4-byte units and covers the whole record.
            skip(decoder.io, Int(hd.length) * DBN.LENGTH_MULTIPLIER - 16)
            skipped += 1
        end
    end
    skipped > 0 &&
        @warn "Typed decode skipped records whose rtype did not match the schema's record type" expected_type = T skipped
    return nothing
end

# Typed record loop that tolerates interleaved non-schema records.
#
# Historical responses are *mostly* type-pure, but the gateway can legitimately
# interleave control records — ErrorMsg (e.g. partial continuous-symbol
# resolution), SystemMsg, SymbolMappingMsg — with the data (#30). DBN's
# `_foreach_record_impl` throws on the first rtype mismatch, which forced
# callers back to `typed=false` (10× slower) and silently discarded the
# ErrorMsg text explaining *why* data was missing. This loop keeps the
# zero-allocation typed hot path (peek header → read straight into a reused
# Ref{T} buffer) and handles everything else by category:
#
#   - ErrorMsg            → per-record @warn (rare, and the content is the
#                           whole point: it explains gaps in the response)
#   - SystemMsg / SymbolMappingMsg → decode + @debug (routine noise)
#   - other known rtypes  → skip by header length, count, one summary @warn
#   - unknown raw rtypes  → skip by header length, count, one summary @warn
#
# Mismatched data records get a single post-loop summary, never a per-record
# warn — a wrong `record_type` override against a million-record stream must
# not emit a million log lines. The summary (rather than silence) matters for
# data-loss visibility: a skipped data rtype means the caller's type and the
# stream disagree.
#
# Unlike DBN's loop, there is deliberately no `finally` that closes
# `decoder.io` — our decoders wrap HTTP bodies / TranscodingStreams whose
# lifecycle is owned by `open_stream` / the caller, not by this function.
function _foreach_typed_tolerant(f, decoder::DBN.DBNDecoder, ::Type{T}) where {T}
    expected = DBN._type_to_rtype_stream(T)
    buffer = Ref{T}()   # reused per record: the typed read is allocation-free
    skipped = 0
    while !eof(decoder.io)
        hd_result = DBN.read_record_header(decoder.io)
        # Unknown raw rtype byte: read_record_header returns a tuple after
        # consuming only the 2 (length, rtype) bytes; length is in 4-byte units.
        if hd_result isa Tuple
            _, _, len_units = hd_result
            skip(decoder.io, Int(len_units) * DBN.LENGTH_MULTIPLIER - 2)
            skipped += 1
            continue
        end
        hd = hd_result
        rt = hd.rtype
        if rt == expected ||
           (T === DBN.OHLCVMsg && (rt == DBN.RType.OHLCV_1S_MSG || rt == DBN.RType.OHLCV_1M_MSG ||
                                   rt == DBN.RType.OHLCV_1H_MSG || rt == DBN.RType.OHLCV_1D_MSG))
            buffer[] = DBN._read_typed_record_stream(decoder, T, hd)
            f(buffer[])
        elseif rt == DBN.RType.ERROR_MSG
            rec = DBN.read_error_msg(decoder, hd)
            @warn "Databento gateway error record interleaved in response stream" err = rec.err
        elseif rt == DBN.RType.SYSTEM_MSG || rt == DBN.RType.SYMBOL_MAPPING_MSG
            rec = DBN.read_record_dispatch(decoder, hd, rt)
            @debug "Skipping control record in typed decode" rtype = rt
        else
            # Known data rtype that doesn't match the schema's record type.
            # The full 16-byte header is already consumed; hd.length is in
            # 4-byte units and covers the whole record.
            skip(decoder.io, Int(hd.length) * DBN.LENGTH_MULTIPLIER - 16)
            skipped += 1
        end
    end
    skipped > 0 &&
        @warn "Typed decode skipped records whose rtype did not match the schema's record type" expected_type = T skipped
    return nothing
end

"""
    decode_dbn_stream(io) -> DBNStore{DBN.DBNRecord}
    decode_dbn_stream(io, ::Type{T}) -> DBNStore{T}

Read a DBN header + all subsequent records from `io` into a `DBNStore`.
Used by Historical endpoints; the IO can be a raw HTTP body or a `TranscodingStream`
wrapping a zstd response.

The single-argument form decodes generically into a `Vector{DBN.DBNRecord}`
(slow GC-bound path). The two-argument form decodes directly into a
`Vector{T}` via DBN.jl's type-specific reader, which is ~10× faster and
has near-zero per-record allocation.

The typed form tolerates interleaved non-`T` records instead of throwing:
gateway `ErrorMsg` records are logged with `@warn` (their text explains why
data is missing), `SystemMsg`/`SymbolMappingMsg` are skipped quietly, and any
other mismatched or unknown rtypes are skipped with a single summary warning.
"""
function decode_dbn_stream(io::IO)::DBNStore{DBN.DBNRecord}
    decoder = DBN.DBNDecoder(io)
    DBN.read_header!(decoder)
    records = DBN.DBNRecord[]
    while true
        rec = DBN.read_record(decoder)
        rec === nothing && break
        push!(records, rec)
    end
    return DBNStore(decoder.metadata, records)
end

function decode_dbn_stream(io::IO, ::Type{T};
                           size_hint::Union{Nothing,Integer} = nothing)::DBNStore{T} where {T}
    decoder = DBN.DBNDecoder(io)
    DBN.read_header!(decoder)
    records = Vector{T}()
    # Pre-size to avoid capacity-doubling realloc cost in the hot loop.
    # Priority: caller-supplied hint > metadata.limit > skip.
    md_limit = decoder.metadata.limit
    if size_hint !== nothing && size_hint > 0
        sizehint!(records, Int(size_hint))
    elseif md_limit !== nothing && md_limit > 0
        sizehint!(records, Int(md_limit))
    end
    _foreach_typed_tolerant(decoder, T) do rec
        push!(records, rec)
    end
    return DBNStore(decoder.metadata, records)
end

"""
    decode_dbn_bytes(bytes; zstd=true) -> DBNStore{DBN.DBNRecord}
    decode_dbn_bytes(bytes, ::Type{T}; zstd=true) -> DBNStore{T}

Decode a complete DBN payload from a byte vector. When `zstd=true`
(Databento's default on the wire), the bytes are decompressed first.

The typed (two-argument) form uses the fast type-specific decoder; prefer
it whenever the schema is type-pure.
"""
function decode_dbn_bytes(bytes::AbstractVector{UInt8}; zstd::Bool = true)::DBNStore{DBN.DBNRecord}
    io = IOBuffer(bytes)
    if zstd
        return decode_dbn_stream(TranscodingStream(ZstdDecompressor(), io))
    else
        return decode_dbn_stream(io)
    end
end

function decode_dbn_bytes(bytes::AbstractVector{UInt8}, ::Type{T};
                          zstd::Bool = true,
                          size_hint::Union{Nothing,Integer} = nothing)::DBNStore{T} where {T}
    io = IOBuffer(bytes)
    # Pick a sizehint for the output Vector{T}. Caller-supplied wins; otherwise
    # use a rough heuristic — zstd-compressed Databento payloads expand ~6–8×
    # for typed market-data records. Over-allocating slightly is much cheaper
    # than the realloc dance under growth (the typed vector only holds
    # `sizeof(T)` per slot, so overshooting by 50% costs ~25 MB on a 1 M-record
    # query and avoids the GC churn).
    hint = if size_hint !== nothing && size_hint > 0
        Int(size_hint)
    elseif zstd
        max(64, div(length(bytes) * 8, sizeof(T)))
    else
        max(64, div(length(bytes), sizeof(T)))
    end
    if zstd
        return decode_dbn_stream(TranscodingStream(ZstdDecompressor(), io), T;
                                 size_hint = hint)
    else
        return decode_dbn_stream(io, T; size_hint = hint)
    end
end
