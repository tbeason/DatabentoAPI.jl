"""
    DBNStore(metadata, records)

A bundle of decoded DBN data: the file's `Metadata` plus the parsed records.
Returned by `get_range`. Iterates over `records`.
"""
struct DBNStore
    metadata::DBN.Metadata
    records::Vector{DBN.DBNRecord}
end

Base.length(s::DBNStore) = length(s.records)
Base.iterate(s::DBNStore, st...) = iterate(s.records, st...)
Base.eltype(::Type{DBNStore}) = DBN.DBNRecord
Base.firstindex(s::DBNStore) = firstindex(s.records)
Base.lastindex(s::DBNStore) = lastindex(s.records)
Base.getindex(s::DBNStore, i) = getindex(s.records, i)

function Base.show(io::IO, s::DBNStore)
    print(io, "DBNStore(dataset=", s.metadata.dataset,
              ", schema=", s.metadata.schema,
              ", n=", length(s.records), ")")
end

"""
    decode_dbn_stream(io)

Read a DBN header + all subsequent records from `io` into a `DBNStore`.
Used by Historical endpoints; the IO can be a raw HTTP body or a `TranscodingStream`
wrapping a zstd response.
"""
function decode_dbn_stream(io::IO)::DBNStore
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

"""
    decode_dbn_bytes(bytes; zstd=true)

Decode a complete DBN payload from a byte vector. When `zstd=true` (Databento's default
on the wire), the bytes are decompressed first.
"""
function decode_dbn_bytes(bytes::AbstractVector{UInt8}; zstd::Bool = true)::DBNStore
    io = IOBuffer(bytes)
    if zstd
        return decode_dbn_stream(TranscodingStream(ZstdDecompressor(), io))
    else
        return decode_dbn_stream(io)
    end
end
