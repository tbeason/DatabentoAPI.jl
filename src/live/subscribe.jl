"""
    subscribe!(client; schema, symbols, stype_in=SType.RAW_SYMBOL,
               snapshot=false, start=nothing) -> Int | Channel{T}

Send a subscription request to the Live gateway. May be called multiple times before
[`start!`](@ref).

Return type depends on the client's mode:
- **Untyped** (`Live(...; typed=false)`, the default): returns an `Int` sub_id.
  All records flow into the single `client.channel :: Channel{DBN.DBNRecord}`.
- **Typed** (`Live(...; typed=true)`): returns the `Channel{T}` for the
  concrete record type bound to this schema. Multiple subscribes to the
  same schema (different symbols) share one channel — records of the same
  type land together. Subscribing under typed mode to a schema with no
  concrete record type (e.g. `Schema.MIX`) errors.

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

    # Typed mode requires every subscribed schema to have a concrete record
    # type. Validate up front so the user sees a clear error at subscribe!
    # instead of at start! or later.
    typed_T = nothing
    if c.typed
        typed_T = DBN.record_type_for_dbn_schema(schema)
        typed_T === nothing && throw(ArgumentError(
            "typed mode requires schema $(schema) to have a concrete record " *
            "type, but DBN.record_type_for_dbn_schema returned nothing. " *
            "Use Live(...; typed=false) for this schema."))
    end

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

    if c.typed
        # Reuse the existing channel if this schema has already been subscribed
        # to; otherwise create a fresh Channel{T}.
        if haskey(c.typed_data_channels, schema)
            return c.typed_data_channels[schema]
        else
            ch = Channel{typed_T}(c.channel_size)
            c.typed_data_channels[schema] = ch
            return ch
        end
    end
    return sub_id
end

"""
    channel(client::Live, schema::Schema.T) -> Channel{T}

Return the typed data channel for the given subscribed schema. Errors if
the client is untyped or if the schema has not been subscribed.
"""
function channel(c::Live, schema::Schema.T)
    c.typed || throw(ArgumentError(
        "channel(client, schema) is only valid for typed-mode Live; " *
        "in untyped mode, use client.channel"))
    haskey(c.typed_data_channels, schema) ||
        throw(KeyError("no subscription for schema $(schema) on this Live"))
    return c.typed_data_channels[schema]
end

"""
    start!(client)

Tell the gateway to begin streaming. After this returns, records flow into
the client's channel(s) and can be consumed via iteration or
[`subscribe_callback`](@ref).
"""
function start!(c::Live)
    c.connected || throw(ArgumentError("call connect!(client) before start!"))
    c.started && return c
    isempty(c.subscriptions) &&
        throw(ArgumentError("call subscribe!(client; ...) at least once before start!"))

    write_text_frame(c.socket; start_session = 0)

    # Under ReconnectPolicy.NONE the reader task owns the channels' lifetime:
    # `bind` closes them when the reader exits, which is the right shutdown
    # signal for iterators / subscribe_callback loops since no new reader is
    # coming. Under RECONNECT the channels are owned by the Live client and
    # outlive any single reader incarnation — the supervisor (added later)
    # respawns a reader writing into the same channels after each drop, so
    # binding would prematurely terminate consumers across reconnects.
    bind_channels = c.reconnect_policy == ReconnectPolicy.NONE

    if c.typed
        c.reader_task = @async _reader_loop_typed(c)
        if bind_channels
            for ch in values(c.typed_data_channels)
                bind(ch, c.reader_task)
            end
            bind(c.control_channel, c.reader_task)
        end
    else
        c.reader_task = @async _reader_loop(c)
        bind_channels && bind(c.channel, c.reader_task)
    end
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
    c.typed && throw(ArgumentError(
        "iterating `for rec in client` is not supported in typed mode. " *
        "Iterate each subscribed channel directly (the value returned by " *
        "subscribe!), or use channel(client, schema)."))
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

Untyped mode only. For typed mode, register one task per subscribed
channel directly:

```julia
ch = subscribe!(client; schema = Schema.TRADES, symbols = ...)
@async for rec in ch; fn(rec); end
```
"""
function subscribe_callback(c::Live, fn)
    c.started || throw(ArgumentError("call start!(client) before subscribe_callback"))
    c.typed && throw(ArgumentError(
        "subscribe_callback(client, fn) is not supported in typed mode. " *
        "Spawn one @async task per subscribed channel instead."))
    return @async try
        for rec in c
            fn(rec)
        end
    catch e
        @error "subscribe_callback consumer crashed" exception=(e, catch_backtrace())
    end
end
