"""
    subscribe!(client; schema, symbols, stype_in=SType.RAW_SYMBOL,
               snapshot=false, start=nothing) -> Int

Send a subscription request to the Live gateway. May be called multiple times before
[`start!`](@ref). Returns the subscription id.

`snapshot=true` (request an order book snapshot) is mutually exclusive with `start`.
"""
function subscribe!(c::Live;
                    schema::Schema.T,
                    symbols,
                    stype_in::SType.T = SType.RAW_SYMBOL,
                    snapshot::Bool = false,
                    start::Union{Nothing,DateTime,Integer} = nothing)
    c.connected || throw(ArgumentError("call connect!(client) before subscribing"))
    c.started   && throw(ArgumentError("cannot subscribe after start!"))
    snapshot && start !== nothing &&
        throw(ArgumentError("snapshot=true and start are mutually exclusive"))

    sub_id = c.next_sub_id
    c.next_sub_id += 1

    syms = symbols_str(symbols)
    start_ns = start === nothing ? nothing :
               (start isa Integer ? Int64(start) :
                Int64(round(Dates.datetime2unix(start) * 1_000_000_000)))

    write_text_frame(c.socket;
        schema   = schema_str(schema),
        stype_in = stype_str(stype_in),
        symbols  = syms,
        snapshot = snapshot ? 1 : 0,
        start    = start_ns,
        id       = sub_id,
        is_last  = 1,
    )

    push!(c.subscriptions, (id = sub_id, schema = schema, symbols = syms,
                            stype_in = stype_in, snapshot = snapshot, start = start_ns))
    return sub_id
end

"""
    start!(client)

Tell the gateway to begin streaming. After this returns, records flow into the client's
internal channel and can be consumed via iteration or [`subscribe_callback`](@ref).
"""
function start!(c::Live)
    c.connected || throw(ArgumentError("call connect!(client) before start!"))
    c.started && return c
    isempty(c.subscriptions) &&
        throw(ArgumentError("call subscribe!(client; ...) at least once before start!"))

    write_text_frame(c.socket; start_session = 0)

    c.reader_task = @async _reader_loop(c)
    bind(c.channel, c.reader_task)
    c.started = true
    return c
end

"""
    stop!(client)

Tell the gateway to stop streaming. Does not close the socket. Records already in the
channel can still be consumed.
"""
function stop!(c::Live)
    c.started || return c
    if c.socket !== nothing && isopen(c.socket)
        try
            write_text_frame(c.socket; stop = "0")
        catch
        end
    end
    c.started = false
    return c
end

# ---- consumer interfaces ----

Base.IteratorSize(::Type{Live}) = Base.SizeUnknown()
Base.eltype(::Type{Live}) = DBN.DBNRecord

function Base.iterate(c::Live, state = nothing)
    c.started || throw(ArgumentError("call start!(client) before iterating"))
    try
        rec = take!(c.channel)
        return (rec, nothing)
    catch e
        if e isa InvalidStateException
            return nothing
        end
        rethrow(e)
    end
end

"""
    subscribe_callback(client, fn) -> Task

Spawn a task that pulls every record from `client` and passes it to `fn(record)`.
Returns the task. Use this for low-overhead event-driven loops; otherwise iterate.
"""
function subscribe_callback(c::Live, fn)
    c.started || throw(ArgumentError("call start!(client) before subscribe_callback"))
    return @async try
        for rec in c
            fn(rec)
        end
    catch e
        @error "subscribe_callback consumer crashed" exception=(e, catch_backtrace())
    end
end
