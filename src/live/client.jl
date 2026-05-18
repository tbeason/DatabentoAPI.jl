"""
    Live([key]; dataset, gateway=nothing, port=13000, ts_out=false,
         heartbeat_interval=nothing, channel_size=10_000, typed=false)

Client for the Databento Live (TCP) API.

Lifecycle: [`connect!`](@ref) → [`subscribe!`](@ref) (one or more times) → [`start!`](@ref)
→ iterate (`for rec in client`) or [`subscribe_callback`](@ref) → `close(client)`.

A single `Live` is bound to ONE dataset. Multi-dataset workflows need multiple clients.

# Typed mode (`typed = true`)

Under typed mode, each subscribed schema gets its own `Channel{T}` for the
concrete record type — no per-record Union boxing, type-stable consumer
code. `subscribe!` returns the typed channel directly; the user owns it
and reads via `for rec in ch; ...; end`. Control records (`ErrorMsg`,
`SystemMsg`, `SymbolMappingMsg`) route to a dedicated channel exposed as
`control_channel(client)`. Iterating `for rec in client` errors under
typed mode — point users at the per-channel pattern instead.

Typed mode requires every subscribed schema to map to a concrete record
type (i.e. `DBN.record_type_for_dbn_schema(schema)` returns non-nothing).
Subscribing to a mixed-record schema like `Schema.MIX` errors at
`subscribe!`.

Default `typed = false` preserves the original single-`Channel{DBN.DBNRecord}`
behaviour exactly — no migration needed for existing code.
"""
mutable struct Live
    api_key::String
    dataset::String
    gateway::String
    port::Int
    ts_out::Bool
    compression::Compression.T
    heartbeat_interval::Union{Nothing,Int}
    slow_reader_behavior::Union{Nothing,SlowReaderBehavior.T}
    user_agent::String

    socket::Union{Nothing,Sockets.TCPSocket}

    # Mode flag — frozen at construction.
    typed::Bool
    # `channel_size` retained on the struct so subscribe! can create
    # per-schema typed channels with the same capacity.
    channel_size::Int

    # Untyped mode: present (single Union channel); typed mode: nothing.
    channel::Union{Nothing,Channel{DBN.DBNRecord}}

    # Typed mode: keyed by subscribed schema, values are Channel{ConcreteT}.
    # Lazily populated by subscribe!; the eltype is concrete per entry but
    # erased to `Any` at the Dict level (the read/write hot path looks up
    # by RType into a parallel Dict — see _reader_loop_typed).
    typed_data_channels::Dict{Schema.T,Any}
    # Typed mode: ErrorMsg / SystemMsg / SymbolMappingMsg flow here.
    control_channel::Union{Nothing,Channel{DBN.DBNRecord}}

    reader_task::Union{Nothing,Task}

    connected::Bool
    started::Bool
    closed::Bool

    session_id::Union{Nothing,String}
    lsg_version::Union{Nothing,String}
    subscriptions::Vector{NamedTuple}
    next_sub_id::Int
end

function Live(key::Union{Nothing,AbstractString} = nothing;
              dataset::AbstractString,
              gateway::Union{Nothing,AbstractString} = nothing,
              port::Integer = DEFAULT_LIVE_PORT,
              ts_out::Bool = false,
              compression::Union{Compression.T,AbstractString} = Compression.NONE,
              heartbeat_interval::Union{Nothing,Integer} = nothing,
              slow_reader_behavior::Union{Nothing,SlowReaderBehavior.T,AbstractString} = nothing,
              channel_size::Integer = 10_000,
              user_agent::AbstractString = USER_AGENT,
              typed::Bool = false)
    api_key = load_api_key(key)
    gw = gateway === nothing ? gateway_for_dataset(dataset) : String(gateway)
    srb = if slow_reader_behavior isa AbstractString
        getfield(SlowReaderBehavior, Symbol(uppercase(slow_reader_behavior)))
    else
        slow_reader_behavior
    end
    cmp = compression isa AbstractString ?
          getfield(Compression, Symbol(uppercase(compression))) : compression

    # Channels are built per-mode. Typed mode lazily creates per-schema data
    # channels in subscribe!; the control channel is constructed up front so
    # consumers can reach for it before subscribe! is called.
    untyped_channel = typed ? nothing : Channel{DBN.DBNRecord}(Int(channel_size))
    control_chan    = typed ? Channel{DBN.DBNRecord}(Int(channel_size)) : nothing
    typed_chans     = Dict{Schema.T,Any}()

    return Live(
        api_key, String(dataset), gw, Int(port), ts_out, cmp,
        heartbeat_interval === nothing ? nothing : Int(heartbeat_interval),
        srb,
        String(user_agent),
        nothing,                                 # socket
        typed, Int(channel_size),
        untyped_channel, typed_chans, control_chan,
        nothing,                                 # reader_task
        false, false, false,
        nothing, nothing,
        NamedTuple[], 1,
    )
end

function Base.show(io::IO, c::Live)
    mode = c.typed ? "typed" : "untyped"
    print(io, "Live(dataset=", c.dataset, ", gateway=", c.gateway, ":", c.port,
              ", mode=", mode,
              ", connected=", c.connected, ", started=", c.started, ")")
end

"""
    control_channel(client::Live) -> Channel{DBN.DBNRecord}

Return the channel carrying control records (`ErrorMsg`, `SystemMsg`,
`SymbolMappingMsg`) for a typed-mode `Live`. Errors if the client was
constructed with `typed = false` (in that mode, control records arrive on
the main `client.channel` alongside data records).
"""
function control_channel(c::Live)
    c.typed || throw(ArgumentError(
        "control_channel(client) is only valid for typed-mode Live; " *
        "in untyped mode, all records (data + control) arrive on client.channel"))
    c.control_channel === nothing && error("control_channel not initialised")
    return c.control_channel
end

"""
    connect!(client)

Open the TCP connection, perform the CRAM authentication handshake. After this returns,
the client is ready for [`subscribe!`](@ref) calls. Throws `BentoAuthError` on failure.
"""
function connect!(c::Live)
    c.connected && return c
    c.closed && throw(ArgumentError("client is closed"))

    c.socket = Sockets.connect(c.gateway, c.port)

    greeting = read_text_frame(c.socket)
    c.lsg_version = get(greeting, "lsg_version", nothing)

    challenge_frame = read_text_frame(c.socket)
    challenge = get(challenge_frame, "cram", nothing)
    challenge === nothing && throw(BentoAuthError(
        "Live gateway did not send a CRAM challenge (got fields: $(collect(keys(challenge_frame))))"))

    response = cram_response(challenge, c.api_key)
    srb_str = c.slow_reader_behavior === nothing ? nothing :
              lowercase(String(Symbol(c.slow_reader_behavior)))
    write_text_frame(c.socket;
        auth                  = response,
        dataset               = c.dataset,
        encoding              = "dbn",
        ts_out                = c.ts_out ? "1" : "0",
        compression           = lowercase(String(Symbol(c.compression))),
        heartbeat_interval_s  = c.heartbeat_interval,
        slow_reader_behavior  = srb_str,
        client                = c.user_agent,
    )

    auth_resp = read_text_frame(c.socket)
    success = get(auth_resp, "success", "0")
    if success != "1"
        err = get(auth_resp, "error", "(no error message)")
        throw(BentoAuthError("Live authentication failed: $err"))
    end
    c.session_id = get(auth_resp, "session_id", nothing)
    c.connected = true
    return c
end

"""
    Base.close(client::Live)

Best-effort close: send `stop` if started, close the socket, close all
channels (the single Union channel in untyped mode; every typed data
channel plus the control channel in typed mode).
"""
function Base.close(c::Live)
    c.closed && return
    c.closed = true
    try
        if c.connected && c.socket !== nothing && isopen(c.socket)
            try
                write_text_frame(c.socket; stop = "0")
            catch
            end
        end
    catch
    end
    try
        c.socket === nothing || Sockets.close(c.socket)
    catch
    end
    # Close every channel we own. Each in its own try so a single bad close
    # doesn't leave others stranded.
    try
        c.channel === nothing || (isopen(c.channel) && close(c.channel))
    catch
    end
    if c.typed
        for ch in values(c.typed_data_channels)
            try
                isopen(ch) && close(ch)
            catch
            end
        end
        try
            c.control_channel === nothing ||
                (isopen(c.control_channel) && close(c.control_channel))
        catch
        end
    end
    return nothing
end

Base.isopen(c::Live) = !c.closed && c.connected
