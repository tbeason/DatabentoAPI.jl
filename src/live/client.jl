"""
    Live([key]; dataset, gateway=nothing, port=13000, ts_out=false,
         heartbeat_interval=nothing, channel_size=10_000)

Client for the Databento Live (TCP) API.

Lifecycle: [`connect!`](@ref) → [`subscribe!`](@ref) (one or more times) → [`start!`](@ref)
→ iterate (`for rec in client`) or [`subscribe_callback`](@ref) → `close(client)`.

A single `Live` is bound to ONE dataset. Multi-dataset workflows need multiple clients.
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
    channel::Channel{DBN.DBNRecord}
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
              user_agent::AbstractString = USER_AGENT)
    api_key = load_api_key(key)
    gw = gateway === nothing ? gateway_for_dataset(dataset) : String(gateway)
    srb = if slow_reader_behavior isa AbstractString
        getfield(SlowReaderBehavior, Symbol(uppercase(slow_reader_behavior)))
    else
        slow_reader_behavior
    end
    cmp = compression isa AbstractString ?
          getfield(Compression, Symbol(uppercase(compression))) : compression
    return Live(
        api_key, String(dataset), gw, Int(port), ts_out, cmp,
        heartbeat_interval === nothing ? nothing : Int(heartbeat_interval),
        srb,
        String(user_agent),
        nothing,
        Channel{DBN.DBNRecord}(Int(channel_size)),
        nothing,
        false, false, false,
        nothing, nothing,
        NamedTuple[], 1,
    )
end

Base.show(io::IO, c::Live) = print(io,
    "Live(dataset=", c.dataset, ", gateway=", c.gateway, ":", c.port,
    ", connected=", c.connected, ", started=", c.started, ")")

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

Best-effort close: send `stop` if started, close the socket, close the channel.
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
    try
        isopen(c.channel) && close(c.channel)
    catch
    end
    return nothing
end

Base.isopen(c::Live) = !c.closed && c.connected
