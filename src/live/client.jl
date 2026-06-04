"""
    Live([key]; dataset, gateway=nothing, port=13000, ts_out=false,
         heartbeat_interval=nothing, channel_size=10_000,
         control_channel_size=nothing, typed=false)

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

The control channel never back-pressures the reader: if it fills (e.g. you
never drain it while a parent/continuous subscription floods
`SymbolMappingMsg` at start), the reader **drops** the overflow control
records rather than blocking — so data delivery is never starved. The drop
count is tracked in `client.dropped_control` and a one-time warning is
logged. Drain `control_channel(client)` if you care about those records.
`control_channel_size` (default `channel_size`) sizes that buffer
independently; raising it lets you keep more control records before drops
begin, but it is a buffering knob, not a correctness fix — the non-blocking
put is what prevents a deadlock.

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
    # Typed mode: count of control records dropped because control_channel
    # was full. The reader puts control records non-blockingly (see
    # _reader_loop_typed) so an undrained control_channel can never
    # back-pressure the reader and starve the data channels. Drain
    # control_channel(client) to keep this at zero.
    dropped_control::Int

    reader_task::Union{Nothing,Task}

    connected::Bool
    started::Bool
    closed::Bool

    session_id::Union{Nothing,String}
    lsg_version::Union{Nothing,String}
    subscriptions::Vector{NamedTuple}
    next_sub_id::Int

    # --- reconnect plumbing (wired up in subsequent commits) ---

    # Replay bookkeeping. Reader populates these per record; on reconnect we
    # pick the per-instrument minimum as the resubscribe start timestamp so
    # the new connection bridges the gap for instruments seen pre-drop.
    # BBO-family schemas key on ts_recv; others on ts_event.
    last_ts_event_by_id::Dict{UInt32,Int64}
    last_ts_recv_by_id::Dict{UInt32,Int64}
    # Gateway Metadata.start from the most recent (re-)connection. Used as
    # gap_end in the reconnect callback.
    last_metadata_start_ns::Union{Nothing,Int64}

    # Reconnect policy + retry budget.
    reconnect_policy::ReconnectPolicy.T
    max_reconnect_attempts::Union{Int,Nothing}
    immediate_reconnect_attempts::Int
    # Lifetime budget remaining; reset to max_reconnect_attempts whenever the
    # new reader successfully delivers ≥1 data record (so a long-lived
    # session that streams between drops keeps its full budget).
    attempts_remaining::Int
    records_since_reconnect::Int

    # Reconnect-callback registry. Each callback receives
    # `(gap_start_ns::Int64, gap_end_ns::Int64)`.
    reconnect_callbacks::Vector{Any}
    callbacks_lock::ReentrantLock

    # Supervisor task + state machine. `state` advances through
    # :fresh → :connecting → :connected → :streaming → :reconnecting → ...
    # → :closed | :failed. `state_lock` guards transitions so close() racing
    # the supervisor produces a deterministic shutdown.
    reconnect_supervisor::Union{Nothing,Task}
    state::Symbol
    state_lock::ReentrantLock
    # Set by the reader when a gateway ErrorMsg arrives; supervisor checks
    # this and refuses to reconnect (transitions to :failed).
    terminal_error::Union{Nothing,String}
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
              control_channel_size::Union{Nothing,Integer} = nothing,
              user_agent::AbstractString = USER_AGENT,
              typed::Bool = false,
              reconnect_policy::Union{ReconnectPolicy.T,Symbol,AbstractString} = ReconnectPolicy.RECONNECT,
              max_reconnect_attempts::Union{Integer,Nothing} = 10,
              immediate_reconnect_attempts::Integer = 3)
    api_key = load_api_key(key)
    gw = gateway === nothing ? gateway_for_dataset(dataset) : String(gateway)
    srb = if slow_reader_behavior isa AbstractString
        getfield(SlowReaderBehavior, Symbol(uppercase(slow_reader_behavior)))
    else
        slow_reader_behavior
    end
    cmp = compression isa AbstractString ?
          getfield(Compression, Symbol(uppercase(compression))) : compression
    rp = _coerce_reconnect_policy(reconnect_policy)
    immediate_reconnect_attempts >= 0 || throw(ArgumentError(
        "immediate_reconnect_attempts must be ≥ 0, got $immediate_reconnect_attempts"))
    if max_reconnect_attempts !== nothing && max_reconnect_attempts < 0
        throw(ArgumentError(
            "max_reconnect_attempts must be ≥ 0 or `nothing` (unlimited), got $max_reconnect_attempts"))
    end
    init_budget = max_reconnect_attempts === nothing ? typemax(Int) : Int(max_reconnect_attempts)

    # Channels are built per-mode. Typed mode lazily creates per-schema data
    # channels in subscribe!; the control channel is constructed up front so
    # consumers can reach for it before subscribe! is called. The control
    # channel can be sized independently of the data channels via
    # `control_channel_size` (defaults to `channel_size`) — useful to absorb
    # the burst of SymbolMappingMsg a parent/continuous subscription emits at
    # start. Sizing only delays a full control channel, though; the reader's
    # non-blocking control put (see _reader_loop_typed) is what guarantees an
    # undrained control channel can't stall data delivery.
    ctrl_size = control_channel_size === nothing ? Int(channel_size) : Int(control_channel_size)
    untyped_channel = typed ? nothing : Channel{DBN.DBNRecord}(Int(channel_size))
    control_chan    = typed ? Channel{DBN.DBNRecord}(ctrl_size) : nothing
    typed_chans     = Dict{Schema.T,Any}()

    return Live(
        api_key, String(dataset), gw, Int(port), ts_out, cmp,
        heartbeat_interval === nothing ? nothing : Int(heartbeat_interval),
        srb,
        String(user_agent),
        nothing,                                 # socket
        typed, Int(channel_size),
        untyped_channel, typed_chans, control_chan,
        0,                                       # dropped_control
        nothing,                                 # reader_task
        false, false, false,
        nothing, nothing,
        NamedTuple[], 1,
        # --- reconnect fields ---
        Dict{UInt32,Int64}(), Dict{UInt32,Int64}(),
        nothing,                                 # last_metadata_start_ns
        rp,
        max_reconnect_attempts === nothing ? nothing : Int(max_reconnect_attempts),
        Int(immediate_reconnect_attempts),
        init_budget,
        0,                                       # records_since_reconnect
        Any[], ReentrantLock(),
        nothing,                                 # reconnect_supervisor
        :fresh, ReentrantLock(),
        nothing,                                 # terminal_error
    )
end

# Internal: accept ReconnectPolicy.T directly, or Symbol / String spellings
# (`:none`/`"none"`, `:reconnect`/`"reconnect"`) for ergonomic call sites that
# don't want to import the enum.
function _coerce_reconnect_policy(rp)
    rp isa ReconnectPolicy.T && return rp
    s = rp isa Symbol ? rp : Symbol(uppercase(String(rp)))
    # Allow both lowercase symbols and the enum-style uppercase strings.
    sym = Symbol(uppercase(String(s)))
    sym === :NONE      && return ReconnectPolicy.NONE
    sym === :RECONNECT && return ReconnectPolicy.RECONNECT
    throw(ArgumentError(
        "reconnect_policy must be ReconnectPolicy.NONE or ReconnectPolicy.RECONNECT (or :none / :reconnect), got $(repr(rp))"))
end

function Base.show(io::IO, c::Live)
    mode = c.typed ? "typed" : "untyped"
    print(io, "Live(dataset=", c.dataset, ", gateway=", c.gateway, ":", c.port,
              ", mode=", mode,
              ", connected=", c.connected, ", started=", c.started, ")")
end

"""
    Live(f::Function, args...; kwargs...)

Do-block form mirroring `Base.open(f, path)`. Constructs the client, runs
`f(client)`, and guarantees `close(client)` in `finally` — so on
`InterruptException` (Ctrl-C), an exception, or normal exit the socket and
channels are torn down without the caller writing the `try/finally`
themselves.

```julia
Live(dataset = "GLBX.MDP3") do client
    connect!(client)
    subscribe!(client; schema = Schema.TRADES, symbols = ["ES.FUT"], stype_in = SType.PARENT)
    start!(client)
    for rec in client
        # … your code …
    end
end  # Ctrl-C here triggers a clean close(client)
```

The manual lifecycle (`Live(...)` → `connect!` → ... → `close(client)`) keeps
working unchanged — this is purely additive.
"""
function Live(f::Function, args...; kwargs...)
    client = Live(args...; kwargs...)
    try
        return f(client)
    finally
        try; close(client); catch; end
    end
end

"""
    live_session(fn; dataset, subscriptions, reconnect_policy=:reconnect, kwargs...) -> result of fn

Convenience wrapper that bundles `connect! → subscribe!(many) → start! → fn(client) → close`
into a single do-block. Defaults `reconnect_policy = :reconnect` so iteration
inside `fn` survives transient TCP drops by spawning the Live-layer reconnect
supervisor (see [`add_reconnect_callback`](@ref)).

`subscriptions` is a vector of NamedTuples describing one subscribe call each.
Each entry must have `schema` and `symbols`; `stype_in` (default
`SType.RAW_SYMBOL`), `snapshot` (default `false`), and `start_dt` (default
`nothing`) are optional.

Any extra `kwargs` are forwarded to `Live(...)` — typically `key`, `gateway`,
`port`, `compression`, `typed`, `max_reconnect_attempts`,
`immediate_reconnect_attempts`.

```julia
live_session(; dataset = "GLBX.MDP3",
               subscriptions = [(; schema = Schema.TRADES,
                                  symbols = ["ES.FUT"],
                                  stype_in = SType.PARENT)]) do client
    add_reconnect_callback(client, (g0, g1) -> @info "gap" gap_s=(g1-g0)/1e9)
    for rec in client
        handle(rec)
    end
end
```

The lower-level `Live(...) do client; ...; end` do-block (introduced in 0.1.1)
remains available for callers that need the explicit `connect!/subscribe!/start!`
lifecycle — e.g. when conditional subscription requires inspecting `client.session_id`
between the steps.
"""
function live_session(fn::Function;
                     dataset::AbstractString,
                     subscriptions,
                     reconnect_policy::Union{ReconnectPolicy.T,Symbol,AbstractString} = :reconnect,
                     key::Union{Nothing,AbstractString} = nothing,
                     kwargs...)
    isempty(subscriptions) && throw(ArgumentError(
        "live_session requires at least one subscription"))
    Live(key; dataset = dataset, reconnect_policy = reconnect_policy, kwargs...) do client
        connect!(client)
        for sub in subscriptions
            haskey(sub, :schema)  || throw(ArgumentError("subscription missing :schema"))
            haskey(sub, :symbols) || throw(ArgumentError("subscription missing :symbols"))
            subscribe!(client;
                schema   = sub.schema,
                symbols  = sub.symbols,
                stype_in = get(sub, :stype_in, SType.RAW_SYMBOL),
                snapshot = get(sub, :snapshot, false),
                start_dt = get(sub, :start_dt, nothing),
            )
        end
        start!(client)
        return fn(client)
    end
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

# Internal: open a TCP socket to the configured gateway and run the CRAM
# authentication handshake, populating c.socket / c.lsg_version / c.session_id.
# Does NOT touch lifecycle flags (c.connected, c.closed) so it can be reused
# from both the public `connect!` entry and the upcoming reconnect path,
# which manages those flags via its own state machine.
function _open_socket_and_auth!(c::Live)
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
    return nothing
end

"""
    connect!(client)

Open the TCP connection, perform the CRAM authentication handshake. After this returns,
the client is ready for [`subscribe!`](@ref) calls. Throws `BentoAuthError` on failure.
"""
function connect!(c::Live)
    c.connected && return c
    c.closed && throw(ArgumentError("client is closed"))
    _open_socket_and_auth!(c)
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
    # Flip the flag BEFORE any teardown so re-entry (e.g. user pressed
    # Ctrl-C twice, or close() is called from a finally inside f()) is a
    # no-op and doesn't double-close anything.
    c.closed = true
    # Mark terminal state so the supervisor (if running) drops out at its
    # next state check instead of attempting another reconnect against a
    # socket we're about to tear down.
    lock(c.state_lock) do
        c.state = :closed
    end
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
    # doesn't leave others stranded. Iteration runs regardless of c.typed
    # because untyped clients have an empty typed_data_channels dict and a
    # nothing control_channel, so the typed branches are no-ops.
    try
        c.channel === nothing || (isopen(c.channel) && close(c.channel))
    catch
    end
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
    return nothing
end

Base.isopen(c::Live) = !c.closed && c.connected
