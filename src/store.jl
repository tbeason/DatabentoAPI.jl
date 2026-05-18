"""
    DBNStore{T}(metadata, records::Vector{T})

A bundle of decoded DBN data: the file's `Metadata` plus the parsed records.
Returned by `get_range`. Iterates over `records`.

When the schema is type-pure (the common case), `T` is the concrete record
struct (e.g. `DBN.TradeMsg`) and the records vector is type-stable. For
mixed-record streams or when `get_range` is called with `typed=false`, `T`
falls back to the `DBN.DBNRecord` Union.
"""
struct DBNStore{T}
    metadata::DBN.Metadata
    records::Vector{T}
end

DBNStore(metadata, records::Vector{T}) where {T} = DBNStore{T}(metadata, records)

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
              ", n=", length(s.records), ")")
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
    DBN._foreach_record_impl(decoder, T) do rec
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
                          zstd::Bool = true)::DBNStore{T} where {T}
    io = IOBuffer(bytes)
    # Rough heuristic: zstd-compressed Databento payloads expand ~6-8× for
    # typed market-data records. Over-allocating slightly is much cheaper
    # than the realloc dance under growth — the typed vector only holds
    # `sizeof(T)` per slot, so overshooting by 50% costs maybe 25 MB on a
    # 1 M-record query and saves the GC churn.
    hint = if zstd
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
