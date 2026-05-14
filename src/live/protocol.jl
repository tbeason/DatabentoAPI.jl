# Databento Live wire protocol — control plane is pipe-delimited text frames
# terminated by '\n'; data plane (after `start_session`) is raw DBN.

"""
    read_text_frame(io) -> Dict{String,String}

Read one `\\n`-terminated text frame from `io` and return its `key=value` pairs as
a Dict. Throws `EOFError` on connection close.
"""
function read_text_frame(io::IO)::Dict{String,String}
    line = readline(io; keep = false)
    isempty(line) && eof(io) && throw(EOFError())
    out = Dict{String,String}()
    for kv in split(line, '|')
        eq = findfirst('=', kv)
        if eq === nothing
            isempty(kv) || (out[String(kv)] = "")
        else
            key = String(kv[1:prevind(kv, eq)])
            val = String(kv[nextind(kv, eq):end])
            out[key] = val
        end
    end
    return out
end

"""
    build_text_frame(; kwargs...) -> String

Build a Databento control-plane frame. Pairs whose value is `nothing` are dropped.
"""
function build_text_frame(; kwargs...)::String
    parts = String[]
    for (k, v) in kwargs
        v === nothing && continue
        push!(parts, string(k, "=", v))
    end
    return string(join(parts, "|"), "\n")
end

"""
    write_text_frame(io; kwargs...)

Build and write a control-plane frame, then `flush(io)`.
"""
function write_text_frame(io::IO; kwargs...)
    write(io, build_text_frame(; kwargs...))
    flush(io)
    return nothing
end
