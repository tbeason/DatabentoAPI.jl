# Multi-schema live capture to compressed DBN files.
#
# Single Live, typed channels, schema-routed files. Records flow:
#
#     gateway
#       └── 1 Live(typed=true)
#            ├── subscribe!(schema=A) → Channel{TA} ── consumer_A → file_A
#            ├── subscribe!(schema=B) → Channel{TB} ── consumer_B → file_B
#            └── control_channel               ── ctrl_drainer → per-schema SessionStats
#
# One TCP connection serves all schemas; the gateway dedupes SymbolMappingMsg
# within a connection (~50% fewer mapping records than the per-schema-Live
# design we used to have). A monitor task per schema wakes every
# `heartbeat_log_interval_s` to log throughput and request a flush via
# `ctx.flush_request` (serviced from the main coordination loop under the
# file lock).
#
# Reconnect: on TCP drop, rebuild the Live and re-subscribe every schema with
# its own `start =` the lowest per-instrument timestamp seen so far (ts_recv
# for BBO families, ts_event otherwise), bounded to within Databento's 24h
# replay window. Between attempts we sleep `rand() * min(cap, base*2^(n-1))`
# (AWS-style full-jitter exponential backoff, capped at 60s) and give up
# after `max_reconnect_attempts` retries (default 10; pass `nothing` for
# unlimited). `ErrorMsg` from the gateway is still terminal for all schemas
# on this connection (no reconnect).
#
# Head-of-line caveat: the reader task `put!`s to each schema's typed channel
# in turn. If consumer N's disk write falls behind enough to fill its
# channel_size buffer, reader stalls the TCP socket for every schema. Default
# channel_size=10_000 is generally enough headroom; tune up for very bursty
# schemas paired with a slow archive disk.

const _RECONNECT_BASE_S        = 1.0
const _RECONNECT_CAP_S         = 60.0
const _STALE_THRESHOLD_S       = 30.0
const _SHUTDOWN_JOIN_TIMEOUT_S = 15.0
const _CONNECTION_POLL_S       = 0.5
const _REPLAY_WINDOW_NS        = Int64(24 * 3600) * 1_000_000_000

# Full-jitter exponential backoff (AWS-recommended): wait drawn uniformly from
# [0, min(cap, base * 2^(n-1))) where n is the backoff-phase attempt index.
# `attempt` is 1-based — `attempt=1` is the delay before the first reconnect.
#
# Optional `immediate` phase: when `attempt <= immediate`, return 0.0 so the
# first N reconnects fire with no sleep. Catches sub-second TCP blips that
# recover within a few packet retransmits, which the 1s+ backoff would
# otherwise stretch into multi-second downtime. The backoff phase starts at
# `attempt = immediate + 1` and is indexed from there (so the first backoff
# attempt still gets the small `base`-sized window, not a pre-stretched one).
function _reconnect_delay(attempt::Integer;
                          immediate::Integer = 0,
                          base::Real = _RECONNECT_BASE_S,
                          cap::Real  = _RECONNECT_CAP_S)
    attempt < 1         && return 0.0
    attempt <= immediate && return 0.0
    n = attempt - immediate
    upper = min(float(cap), float(base) * 2.0^(n - 1))
    return rand() * upper
end

const _SPARSE_SCHEMAS = (Schema.STATUS, Schema.DEFINITION, Schema.IMBALANCE)

# StatusMsg.action codes that warrant a console alert per Databento's status
# schema docs: 8=halt, 9=pause, 10=suspend, 14=SSR change,
# 15=not-available-for-trading.
const _ALARM_STATUS_ACTIONS = (UInt16(8), UInt16(9), UInt16(10), UInt16(14), UInt16(15))

# ---------- session stats ----------

Base.@kwdef mutable struct SessionStats
    schema::Schema.T
    data_count::Int    = 0
    mapping_count::Int = 0
    system_count::Int  = 0
    error::Union{Nothing,String} = nothing
    reconnects::Int    = 0
    last_record_at::Float64 = 0.0
    # Per-instrument replay timestamps moved to Live (populated by reader,
    # consumed by reconnect supervisor) — see Live.last_ts_*_by_id.
    status_state_by_id::Dict{UInt32,Tuple{UInt16,UInt8}} = Dict{UInt32,Tuple{UInt16,UInt8}}()
    alarm_status_count::Int = 0
end

# ---------- output file ----------

mutable struct RotatingDBNFile
    base_dir::String
    explicit_path::Union{Nothing,String}
    dataset::String
    schema::Schema.T
    symbols::Vector{String}
    stype_in::SType.T
    compress::Bool
    compress_level::Int
    rotate_seconds::Union{Nothing,Float64}
    raw_io::Union{Nothing,IO}
    zstd_io::Union{Nothing,IO}                  # == raw_io if !compress
    encoder::Union{Nothing,DBN.DBNEncoder}
    current_path::String
    opened_at::Float64
    rotations::Int
    all_paths::Vector{String}
    write_lock::ReentrantLock
    # In-file zstd frame rotation for crash safety. Every `frame_seconds`,
    # close the current zstd frame (writes footer to raw_io) and start a new
    # one on the same file. Multi-frame .dbn.zst is standards-compliant to
    # any zstd reader, so a hard kill loses ≤ one frame of records rather
    # than corrupting the whole file. `nothing` disables it (single frame).
    frame_seconds::Union{Nothing,Float64}
    frame_opened_at::Float64
    frame_count::Int
end

function RotatingDBNFile(; base_dir::AbstractString,
                          explicit_path::Union{Nothing,AbstractString} = nothing,
                          dataset::AbstractString,
                          schema::Schema.T,
                          symbols,
                          stype_in::SType.T,
                          compress::Bool,
                          compress_level::Integer,
                          rotate_seconds::Union{Nothing,Real},
                          frame_seconds::Union{Nothing,Real} = nothing)
    syms_vec = symbols isa AbstractString ? [String(symbols)] : String.(symbols)
    f = RotatingDBNFile(
        String(base_dir),
        explicit_path === nothing ? nothing : String(explicit_path),
        String(dataset), schema, syms_vec, stype_in,
        compress, Int(compress_level),
        rotate_seconds === nothing ? nothing : Float64(rotate_seconds),
        nothing, nothing, nothing, "", 0.0, 0, String[], ReentrantLock(),
        frame_seconds === nothing ? nothing : Float64(frame_seconds),
        0.0, 0,
    )
    _open!(f)
    return f
end

function _next_path(f::RotatingDBNFile)
    f.explicit_path === nothing || return f.explicit_path
    safe = replace(schema_str(f.schema), '/' => '_')
    dir  = joinpath(f.base_dir, f.dataset, safe)
    mkpath(dir)
    ts   = Dates.format(Dates.now(Dates.UTC), dateformat"yyyymmdd\THHMMSS\Z")
    ext  = f.compress ? ".dbn.zst" : ".dbn"
    return joinpath(dir, ts * ext)
end

function _open!(f::RotatingDBNFile)
    path = _next_path(f)
    raw  = open(path, "w")
    top  = f.compress ? TranscodingStream(ZstdCompressor(level = f.compress_level), raw) : raw
    md = DBN.Metadata(
        UInt8(DBN.DBN_VERSION),
        f.dataset,
        f.schema,
        Int64(round(time() * 1_000_000_000)),
        nothing,                                # end_ts unknown for live captures
        nothing,                                # limit
        f.stype_in,
        SType.INSTRUMENT_ID,                    # gateway resolves to instrument_id
        false,                                  # ts_out (we don't request gateway-side ts)
        f.symbols,
        String[], String[],
        Tuple{String,String,Int64,Int64}[],
    )
    enc = DBN.DBNEncoder(top, md)
    DBN.write_header(enc)
    f.raw_io          = raw
    f.zstd_io         = top
    f.encoder         = enc
    f.current_path    = path
    f.opened_at       = time()
    f.frame_opened_at = time()  # first frame opens with the file
    push!(f.all_paths, path)
    return nothing
end

# In-file zstd frame rotation. Writes TranscodingStreams' TOKEN_END through
# the active zstd stream + flushes, which makes the codec emit the zstd
# frame footer to raw_io. Subsequent writes start a fresh frame on the same
# stream — the resulting .dbn.zst is a multi-frame zstd file, fully
# standards-compliant. DBN-level layout is unaffected: the metadata header
# was written once at file open, and the decompressed output is concatenated
# across frames, so readers see "header then records".
#
# Crucially this does NOT close the underlying TranscodingStream (which
# would also close raw_io), and does NOT recreate the DBN encoder — same
# instance keeps working across the frame boundary.
function _rotate_frame_if_needed!(f::RotatingDBNFile)
    f.frame_seconds === nothing && return false
    f.compress                  || return false   # only meaningful for compressed files
    return lock(f.write_lock) do
        (time() - f.frame_opened_at) < f.frame_seconds && return false
        f.zstd_io === nothing && return false   # file closed concurrently
        try
            write(f.zstd_io, TranscodingStreams.TOKEN_END)
            flush(f.zstd_io)
        catch e
            @warn "frame rotation: writing frame footer raised" schema=f.schema exception=e
            return false
        end
        f.frame_opened_at = time()
        f.frame_count    += 1
        return true
    end
end

function _close_stack!(f::RotatingDBNFile)
    # ORDER MATTERS: flush + close the zstd stream so the frame footer is
    # emitted into raw_io, THEN close raw_io.
    try
        f.zstd_io === nothing || flush(f.zstd_io)
    catch
    end
    try
        if f.zstd_io !== nothing && f.compress
            close(f.zstd_io)  # writes zstd footer; does NOT close raw_io
        end
    catch
    end
    try
        f.raw_io === nothing || close(f.raw_io)
    catch
    end
    f.encoder = nothing
    f.zstd_io = nothing
    f.raw_io  = nothing
    return nothing
end

function _rotate_if_needed!(f::RotatingDBNFile)
    f.rotate_seconds === nothing && return false
    (time() - f.opened_at) < f.rotate_seconds && return false
    old = f.current_path
    _close_stack!(f)
    _open!(f)
    f.rotations += 1
    @info "rotated DBN file" schema=f.schema from=basename(old) to=basename(f.current_path)
    return true
end

function _write_record!(f::RotatingDBNFile, rec)
    lock(f.write_lock) do
        # File rotation (new .dbn.zst) takes precedence over frame rotation:
        # if we just opened a fresh file, its zstd frame is brand new anyway.
        _rotate_if_needed!(f) || _rotate_frame_if_needed!(f)
        DBN.write_record(f.encoder, rec)
    end
    return nothing
end

function _flush!(f::RotatingDBNFile)
    lock(f.write_lock) do
        f.zstd_io === nothing || flush(f.zstd_io)
        f.raw_io  === nothing || flush(f.raw_io)
    end
    return nothing
end

_close!(f::RotatingDBNFile) = lock(() -> _close_stack!(f), f.write_lock)

# ---------- public DBN writer wrapper ----------

"""
    open_dbn_writer(f::Function;
                    base_dir, dataset, schema, symbols, stype_in,
                    compress = true, compress_level = 1,
                    rotate_seconds = nothing,
                    frame_seconds = 60.0,
                    explicit_path = nothing)

Open a DBN file writer, run `f(writer)`, and guarantee the file is flushed
and closed in `finally` — so on `InterruptException` (Ctrl-C), an exception,
or normal exit the zstd frame footer is emitted and `current_path` is a
well-formed `.dbn.zst`.

Use this when iterating records from a `Live` client (or any other source)
and writing them to a Databento Binary Encoding file with the same
durability features `stream_to_file` uses internally:

  - Optional **time-based file rotation** via `rotate_seconds` (close the
    current file, open the next one with a fresh timestamp).
  - **In-file zstd frame rotation** via `frame_seconds` (default 60s) for
    crash safety. A hard kill loses at most one frame of records instead
    of corrupting the whole file. Pass `nothing` to disable.

Inside the do-block, call [`write_record!`](@ref)`(writer, rec)` for each
record. The writer also exposes `writer.current_path` if you need it.

```julia
Live(dataset = "GLBX.MDP3") do client
    connect!(client)
    subscribe!(client; schema = Schema.TRADES, symbols = ["ES.FUT"], stype_in = SType.PARENT)
    start!(client)
    open_dbn_writer(; base_dir = "./capture", dataset = "GLBX.MDP3",
                      schema = Schema.TRADES, symbols = ["ES.FUT"],
                      stype_in = SType.PARENT) do writer
        for rec in client
            write_record!(writer, rec)
        end
    end
end
```

Returns whatever `f(writer)` returns.
"""
function open_dbn_writer(f::Function;
                         base_dir::AbstractString,
                         dataset::AbstractString,
                         schema::Schema.T,
                         symbols,
                         stype_in::SType.T,
                         compress::Bool = true,
                         compress_level::Integer = 1,
                         rotate_seconds::Union{Nothing,Real} = nothing,
                         frame_seconds::Union{Nothing,Real} = 60.0,
                         explicit_path::Union{Nothing,AbstractString} = nothing)
    writer = RotatingDBNFile(
        base_dir = base_dir, explicit_path = explicit_path,
        dataset = dataset, schema = schema, symbols = symbols, stype_in = stype_in,
        compress = compress, compress_level = compress_level,
        rotate_seconds = explicit_path === nothing ? rotate_seconds : nothing,
        frame_seconds = frame_seconds,
    )
    try
        return f(writer)
    finally
        _close!(writer)
    end
end

"""
    write_record!(writer, rec)

Write one DBN record to a writer opened via [`open_dbn_writer`](@ref).
Thread-safe: holds the writer's internal lock for the duration of the
write so concurrent calls (e.g. from per-schema consumer tasks) cannot
interleave bytes.
"""
write_record!(w::RotatingDBNFile, rec) = _write_record!(w, rec)

# ---------- session context ----------

mutable struct SessionContext
    schema::Schema.T
    stats::SessionStats
    file::RotatingDBNFile
    current_client::Union{Nothing,Live}
    shutdown_requested::Threads.Atomic{Bool}
    flush_request::Channel{Nothing}
end

SessionContext(schema, stats, file) = SessionContext(
    schema, stats, file, nothing,
    Threads.Atomic{Bool}(false),
    Channel{Nothing}(1),
)

function _request_shutdown!(ctx::SessionContext)
    ctx.shutdown_requested[] = true
    c = ctx.current_client
    c === nothing && return
    try; stop!(c); catch; end
    try; close(c); catch; end
    return nothing
end

# ---------- record routing ----------

_is_bbo_family(s::Schema.T) = s in (
    Schema.BBO_1S, Schema.BBO_1M,
    Schema.CBBO_1S, Schema.CBBO_1M,
    Schema.TBBO,    Schema.TCBBO,
)

# Replay-state lookup used by the reconnect supervisor. Reads from the
# Live's per-instrument timestamp dicts (populated by the reader's
# `_record_replay!`). Returns the minimum-across-instruments — the earliest
# point we lost coverage on any subscribed instrument, which is the safest
# resubscribe `start=` value (replays a bit more rather than skipping).
function _replay_start_ts(c::Live, schema::Schema.T)
    d = _is_bbo_family(schema) ? c.last_ts_recv_by_id : c.last_ts_event_by_id
    isempty(d) && return nothing
    return minimum(values(d))
end

function _log_status_alarm!(stats::SessionStats, rec::DBN.StatusMsg, schema::Schema.T)
    iid   = rec.hd.instrument_id
    state = (UInt16(rec.action), UInt8(rec.is_trading))
    prev  = get(stats.status_state_by_id, iid, nothing)
    prev == state && return
    stats.status_state_by_id[iid] = state
    entering = UInt16(rec.action) in _ALARM_STATUS_ACTIONS
    leaving  = prev !== nothing && prev[1] in _ALARM_STATUS_ACTIONS
    if entering || leaving
        stats.alarm_status_count += 1
        @warn "status transition" schema=schema iid=iid action=Int(rec.action) is_trading=Char(rec.is_trading)
    end
    return nothing
end

# Data-record path: writes the record to disk + bumps counters. Replay
# bookkeeping is owned by the Live client (see Live.last_ts_*_by_id,
# populated by the reader's _record_replay! before the put!).
function _handle_data!(ctx::SessionContext, rec)
    s = ctx.stats
    s.data_count    += 1
    s.last_record_at = time()
    # StatusMsg additionally checks for alarm-worthy state transitions.
    if rec isa DBN.StatusMsg
        _log_status_alarm!(s, rec, ctx.schema)
    end
    _write_record!(ctx.file, rec)
    return nothing
end

# Control-record path: SymbolMappingMsg / SystemMsg / ErrorMsg.
# Counts + special handling per type. SymbolMappingMsg is written to the
# DBN file alongside data records — DatabentoBinaryEncoding ≥ 0.1.1 fixes
# the v1→v3 layout re-encoding so the on-disk record has a consistent
# header length and round-trips through DBN.read_dbn_with_metadata. System
# and Error messages are not written to disk (they're heartbeat / control
# noise that doesn't belong in a record stream).
function _handle_control!(ctx::SessionContext, rec)
    s = ctx.stats
    if rec isa DBN.SymbolMappingMsg
        s.mapping_count += 1
        _write_record!(ctx.file, rec)
    elseif rec isa DBN.SystemMsg
        s.system_count += 1
        is_heartbeat(rec) || @debug "SystemMsg" schema=ctx.schema msg=rec.msg
    elseif rec isa DBN.ErrorMsg
        s.error = String(rec.err)
        @error "gateway ErrorMsg (terminal)" schema=ctx.schema err=s.error
        # Caller's main loop checks ctx.stats.error and exits without reconnect.
    end
    return nothing
end

# Untyped-mode fallback (kept as a backward-compat dispatcher in case any
# external caller depends on the unified-channel _handle_record! path).
function _handle_record!(ctx::SessionContext, rec)
    if rec isa DBN.SymbolMappingMsg ||
       rec isa DBN.SystemMsg ||
       rec isa DBN.ErrorMsg
        _handle_control!(ctx, rec)
    else
        _handle_data!(ctx, rec)
    end
    return nothing
end

# ---------- unified reconnect loop (1 Live, N schemas, N files) ----------

function _bound_replay(start_ts)
    start_ts === nothing && return nothing
    ts_ns = if start_ts isa Dates.DateTime
        Int64(round(Dates.datetime2unix(start_ts) * 1_000_000_000))
    else
        Int64(start_ts)
    end
    now_ns = Int64(round(time() * 1_000_000_000))
    return max(ts_ns, now_ns - _REPLAY_WINDOW_NS)
end

_any_shutdown(ctxs) = any(c -> c.shutdown_requested[], values(ctxs))
_any_error(ctxs)    = any(c -> c.stats.error !== nothing, values(ctxs))

# Per-schema data drainer: pulls from this schema's typed channel and writes
# to its file. Exits on channel close, shutdown, deadline, or terminal error.
function _drain_data!(ctx::SessionContext, ch, deadline)
    try
        while true
            rec = try
                take!(ch)
            catch e
                e isa InvalidStateException && break
                rethrow()
            end
            _handle_data!(ctx, rec)
            ctx.shutdown_requested[]                   && break
            deadline !== nothing && time() >= deadline && break
            ctx.stats.error !== nothing                && break
        end
    catch e
        if !(e isa InvalidStateException) && !ctx.shutdown_requested[]
            @warn "data drainer error" schema=ctx.schema exception=(e, catch_backtrace())
        end
    end
    return nothing
end

# Shared control drainer: broadcasts each control record to every ctx so
# per-schema stats (mapping_count, system_count, error) stay comparable
# regardless of how many schemas share this Live.
function _drain_control!(ctxs::Dict{Schema.T,SessionContext}, ctrl_ch)
    try
        for rec in ctrl_ch
            for ctx in values(ctxs)
                _handle_control!(ctx, rec)
            end
        end
    catch e
        if !(e isa InvalidStateException)
            @warn "control drainer error" exception=(e, catch_backtrace())
        end
    end
    return nothing
end

function _run_unified_session(ctxs::Dict{Schema.T,SessionContext};
                              dataset::AbstractString,
                              stype_in::SType.T,
                              symbols,
                              reconnect::Bool,
                              max_reconnect_attempts::Union{Integer,Nothing},
                              deadline::Union{Nothing,Float64},
                              key, gateway, port::Integer,
                              wire_compression::Compression.T,
                              heartbeat_interval, slow_reader_behavior,
                              channel_size::Integer,
                              start_initial,
                              snapshot::Bool)
    # Stable subscribe! order keeps any future replay log deterministic.
    schemas = sort!(collect(keys(ctxs)); by = s -> Int(s))

    # Single Live for the lifetime of this session. Under reconnect=true,
    # the Live-layer supervisor reopens the socket + replays subscriptions
    # internally; the streaming layer no longer owns a reconnect loop.
    client = Live(key;
        dataset = String(dataset),
        gateway = gateway, port = port,
        ts_out = false,
        compression = wire_compression,
        heartbeat_interval = heartbeat_interval,
        slow_reader_behavior = slow_reader_behavior,
        channel_size = channel_size,
        typed = true,
        reconnect_policy = reconnect ? ReconnectPolicy.RECONNECT : ReconnectPolicy.NONE,
        max_reconnect_attempts = max_reconnect_attempts,
    )
    for ctx in values(ctxs); ctx.current_client = client; end

    # Mirror the supervisor's reconnect successes into per-schema stats so
    # the existing stats-based monitoring (heartbeat log, smoke assertions,
    # potential dashboards) keeps its `reconnects` counter live.
    if reconnect
        add_reconnect_callback(client, (_g0, _g1) -> begin
            for ctx in values(ctxs); ctx.stats.reconnects += 1; end
        end)
    end

    data_channels  = Dict{Schema.T,Any}()
    consumer_tasks = Dict{Schema.T,Task}()
    ctrl_drainer::Union{Nothing,Task} = nothing
    try
        # Initial connect with retry. The Live-layer supervisor only
        # kicks in after start! (it watches the reader task), so a
        # gateway that hangs up mid-handshake on the *first* attempt
        # would otherwise abort the whole session. Retry per the same
        # cadence — immediate phase then exp backoff — bounded by
        # max_reconnect_attempts (1 + cap total attempts: initial + cap
        # retries).
        connect_attempt = 0
        while true
            try
                connect!(client)
                break
            catch e
                e isa InvalidStateException && rethrow()
                connect_attempt += 1
                if reconnect && (max_reconnect_attempts === nothing ||
                                 connect_attempt <= max_reconnect_attempts)
                    for ctx in values(ctxs); ctx.stats.reconnects += 1; end
                    delay = _reconnect_delay(connect_attempt;
                                              immediate = client.immediate_reconnect_attempts)
                    @warn "live initial connect failed — retrying" attempt=connect_attempt delay_s=round(delay; digits=2)
                    sleep(delay)
                    if _any_shutdown(ctxs) ||
                       (deadline !== nothing && time() >= deadline)
                        rethrow()
                    end
                    continue
                end
                if reconnect
                    @warn "live initial connect — retry budget exhausted" attempts=connect_attempt limit=max_reconnect_attempts
                end
                rethrow()
            end
        end

        for sch in schemas
            ctx = ctxs[sch]
            # First-subscription start_ts comes from the caller. After a
            # reconnect the supervisor recomputes per-schema via
            # _replay_start_ts(c::Live, schema) and re-issues subscribe
            # frames itself — no streaming-level branching needed here.
            start_ts = _bound_replay(start_initial)
            data_channels[sch] = subscribe!(client;
                schema   = sch,
                symbols  = symbols,
                stype_in = stype_in,
                snapshot = snapshot,
                start    = start_ts,
            )
        end
        start!(client)

        ctrl_drainer = @async _drain_control!(ctxs, control_channel(client))
        for sch in schemas
            ctx = ctxs[sch]
            ch  = data_channels[sch]
            consumer_tasks[sch] = @async _drain_data!(ctx, ch, deadline)
        end

        # Main coordination loop: poll for shutdown/deadline/error and
        # service per-schema flush requests. Exits when every consumer task
        # has finished — supervisor reaching a terminal state (:failed /
        # :closed) closes the channels, which makes the take!s raise
        # InvalidStateException and the drainers return. Also exits on
        # explicit shutdown/deadline/error so we close(client) below and
        # tell the supervisor to stop reconnecting.
        try
            while !all(istaskdone, values(consumer_tasks))
                _any_shutdown(ctxs)                              && break
                deadline !== nothing && time() >= deadline       && break
                _any_error(ctxs)                                 && break
                for ctx in values(ctxs)
                    while isready(ctx.flush_request)
                        try; take!(ctx.flush_request); catch; end
                        _flush!(ctx.file)
                    end
                end
                sleep(_CONNECTION_POLL_S)
            end
        catch e
            if !(e isa InvalidStateException) && !_any_shutdown(ctxs)
                @warn "live session error" schemas=schemas exception=(e, catch_backtrace())
            end
        end
    catch e
        if !(e isa InvalidStateException) && !_any_shutdown(ctxs)
            @warn "live session error" schemas=schemas exception=(e, catch_backtrace())
        end
    finally
        for ctx in values(ctxs); ctx.current_client = nothing; end
        # close(client) transitions supervisor state→:closed (its next
        # check exits cleanly) and closes every typed channel + the
        # control channel; wait for drainers to flush in-flight records.
        try; close(client); catch; end
        for t in values(consumer_tasks); try; wait(t); catch; end; end
        ctrl_drainer === nothing || (try; wait(ctrl_drainer); catch; end)
        # Let the supervisor finish its current iteration so any in-flight
        # reconnect attempt observes c.closed and exits.
        if client.reconnect_supervisor !== nothing
            try; wait(client.reconnect_supervisor); catch; end
        end
    end
end

# ---------- heartbeat monitor ----------

function _session_monitor(ctx::SessionContext, interval_s::Float64,
                         stop::Threads.Atomic{Bool})
    last_count = 0
    last_tick  = time()
    while !stop[]
        slept = 0.0
        while slept < interval_s && !stop[]
            sleep(min(0.5, interval_s - slept))
            slept += 0.5
        end
        stop[] && break
        now   = time()
        dt_s  = max(now - last_tick, 1e-6)
        cur   = ctx.stats.data_count
        delta = cur - last_count
        rate  = (delta / dt_s) * 60.0
        size_mb = try; stat(ctx.file.current_path).size / 1_048_576; catch; 0.0; end
        is_sparse = ctx.schema in _SPARSE_SCHEMAS
        stale = !is_sparse && ctx.stats.last_record_at > 0 &&
                (now - ctx.stats.last_record_at) > _STALE_THRESHOLD_S
        msg = "$(schema_str(ctx.schema)): $(cur) rec  +$(delta)  $(round(Int, rate))/min  $(round(size_mb, digits=1)) MB"
        stale ? (@warn msg * "  STALE") : (@info msg)
        if isopen(ctx.flush_request) && !isready(ctx.flush_request)
            try; put!(ctx.flush_request, nothing); catch; end
        end
        # Directly flush + rotate frame from the monitor task. This is the
        # only mechanism that fires for QUIET schemas — _write_record! only
        # runs when records arrive, and the main coordination loop blocks
        # on a per-schema channel that's empty. Without this, a quiet
        # schema's in-memory zstd buffer (including the DBN metadata
        # header) never reaches disk and a hard kill loses the entire file.
        try; _flush!(ctx.file); catch; end
        try; _rotate_frame_if_needed!(ctx.file); catch; end
        last_count = cur
        last_tick  = now
    end
    return nothing
end

# ---------- public API ----------

"""
    read_capture(path) -> (DBN.Metadata, Vector{DBN.DBNRecord})

Decode a `.dbn` or `.dbn.zst` file written by [`stream_to_file`](@ref) /
[`stream_multi_to_files`](@ref) (or by a Databento batch download). Thin
wrapper around `DBN.read_dbn_with_metadata` — kept as a convenience export
so callers don't have to import `DBN` themselves.
"""
read_capture(path::AbstractString) = DBN.read_dbn_with_metadata(String(path))

"""
    default_live_path(; dataset, schema, base_dir = joinpath(pwd(), "live"), compress = true)

Compute the default output path for a live capture session.
Format: `{base_dir}/{dataset}/{schema_str(schema)}/{utc-yyyymmddTHHMMSSZ}.dbn[.zst]`.
"""
function default_live_path(; dataset::AbstractString,
                            schema::Schema.T,
                            base_dir::AbstractString = joinpath(pwd(), "live"),
                            compress::Bool = true)
    safe = replace(schema_str(schema), '/' => '_')
    dir  = joinpath(String(base_dir), String(dataset), safe)
    ts   = Dates.format(Dates.now(Dates.UTC), dateformat"yyyymmdd\THHMMSS\Z")
    ext  = compress ? ".dbn.zst" : ".dbn"
    return joinpath(dir, ts * ext)
end

"""
    stream_to_file(; schema, symbols, dataset, ...) -> String

Capture a single Live schema to a single (optionally rotating) DBN file.
Returns the path to the most recently opened output file.

The default `compress_level = 1` favours throughput; bench measurements show
L1 is ~35% faster on the write boundary than L3 in exchange for ~15% larger
files (see `benchmark/PERF_REPORT.md`). Pass a higher level for archival.

`reconnect = true` (default) re-establishes the TCP session after a drop using
full-jitter exponential backoff (1s base, 60s cap). After
`max_reconnect_attempts` retries (default 10) the loop gives up and returns;
pass `nothing` for unlimited.

`frame_seconds = 60.0` (default) closes the active zstd frame and starts a new
one every minute, so a hard kill (SIGKILL, OOM, power loss) loses at most one
frame of records instead of corrupting the whole file. Pass `nothing` to write
a single frame. The output is a standards-compliant multi-frame `.dbn.zst` and
reads back as one continuous record stream via any zstd reader.

Press `Ctrl-C` for a clean shutdown — the active zstd frame footer is flushed
before the function returns.
"""
function stream_to_file(; schema::Schema.T,
                        symbols,
                        dataset::AbstractString,
                        stype_in::SType.T = SType.RAW_SYMBOL,
                        path::Union{Nothing,AbstractString} = nothing,
                        base_dir::Union{Nothing,AbstractString} = nothing,
                        duration_s::Union{Nothing,Real} = nothing,
                        compress::Bool = true,
                        compress_level::Integer = 1,
                        rotate_seconds::Union{Nothing,Real} = nothing,
                        frame_seconds::Union{Nothing,Real} = 60.0,
                        reconnect::Bool = true,
                        max_reconnect_attempts::Union{Integer,Nothing} = 10,
                        key = nothing,
                        gateway = nothing,
                        port::Integer = DEFAULT_LIVE_PORT,
                        wire_compression::Compression.T = Compression.ZSTD,
                        heartbeat_interval = nothing,
                        slow_reader_behavior = nothing,
                        channel_size::Integer = 10_000,
                        heartbeat_log_interval_s::Real = 30.0,
                        start = nothing,
                        snapshot::Bool = false)::String
    base = base_dir === nothing ? joinpath(pwd(), "live") : String(base_dir)
    return open_dbn_writer(
        base_dir = base, explicit_path = path,
        dataset = dataset, schema = schema, symbols = symbols, stype_in = stype_in,
        compress = compress, compress_level = compress_level,
        rotate_seconds = rotate_seconds, frame_seconds = frame_seconds,
    ) do file
        ctx = SessionContext(schema, SessionStats(schema = schema), file)
        ctxs = Dict{Schema.T,SessionContext}(schema => ctx)

        deadline = duration_s === nothing ? nothing : time() + Float64(duration_s)
        monitor_stop = Threads.Atomic{Bool}(false)
        monitor_task = @async _session_monitor(ctx, Float64(heartbeat_log_interval_s), monitor_stop)

        try
            _run_unified_session(ctxs;
                dataset = dataset, stype_in = stype_in, symbols = symbols,
                reconnect = reconnect,
                max_reconnect_attempts = max_reconnect_attempts,
                deadline = deadline,
                key = key, gateway = gateway, port = port,
                wire_compression = wire_compression,
                heartbeat_interval = heartbeat_interval,
                slow_reader_behavior = slow_reader_behavior,
                channel_size = channel_size,
                start_initial = start, snapshot = snapshot,
            )
        finally
            monitor_stop[] = true
            try; istaskdone(monitor_task) || sleep(0.1); catch; end
        end
        return ctx.file.current_path
    end
end

"""
    stream_multi_to_files(; schemas, symbols, dataset, ...) -> Dict{Schema.T,String}

Capture N Live schemas concurrently, one rotating-DBN file per schema. Returns
a dict mapping each schema to its most recently opened output path. Press
`Ctrl-C` to stop early; honours `duration_s` if provided.

`reconnect`/`max_reconnect_attempts` work as in [`stream_to_file`](@ref):
full-jitter exponential backoff between retries (1s base, 60s cap), default
cap of 10 attempts, pass `nothing` for unlimited.

`frame_seconds = 60.0` (default) writes a fresh zstd frame every minute on
each per-schema file for crash safety (multi-frame `.dbn.zst`; reads back as
one continuous stream). Pass `nothing` to write a single frame.

Example:
```julia
paths = stream_multi_to_files(
    dataset  = "OPRA.PILLAR",
    schemas  = [Schema.TCBBO, Schema.CBBO_1S, Schema.STATUS],
    symbols  = ["SPXW.OPT"],
    stype_in = SType.PARENT,
    duration_s = 60,
)
```
"""
function stream_multi_to_files(; schemas::AbstractVector,
                               symbols,
                               dataset::AbstractString,
                               stype_in::SType.T = SType.RAW_SYMBOL,
                               base_dir::Union{Nothing,AbstractString} = nothing,
                               duration_s::Union{Nothing,Real} = nothing,
                               compress::Bool = true,
                               compress_level::Integer = 1,
                               rotate_seconds::Union{Nothing,Real} = nothing,
                               frame_seconds::Union{Nothing,Real} = 60.0,
                               reconnect::Bool = true,
                               max_reconnect_attempts::Union{Integer,Nothing} = 10,
                               key = nothing,
                               gateway = nothing,
                               port::Integer = DEFAULT_LIVE_PORT,
                               wire_compression::Compression.T = Compression.ZSTD,
                               heartbeat_interval = nothing,
                               slow_reader_behavior = nothing,
                               channel_size::Integer = 10_000,
                               heartbeat_log_interval_s::Real = 30.0,
                               start::Union{Nothing,Dates.DateTime,Integer} = nothing,
                               snapshot::Bool = false)::Dict{Schema.T,String}
    isempty(schemas) && throw(ArgumentError("schemas must not be empty"))
    base = base_dir === nothing ? joinpath(pwd(), "live") : String(base_dir)
    sch_vec = Schema.T[s isa Schema.T ? s :
                       getfield(Schema, Symbol(uppercase(replace(String(s), '-' => '_'))))
                       for s in schemas]

    contexts = Dict{Schema.T,SessionContext}()
    for sch in sch_vec
        file = RotatingDBNFile(
            base_dir = base, explicit_path = nothing,
            dataset = dataset, schema = sch, symbols = symbols, stype_in = stype_in,
            compress = compress, compress_level = compress_level,
            rotate_seconds = rotate_seconds,
            frame_seconds = frame_seconds,
        )
        contexts[sch] = SessionContext(sch, SessionStats(schema = sch), file)
    end

    deadline = duration_s === nothing ? nothing : time() + Float64(duration_s)
    monitor_stops = Dict(s => Threads.Atomic{Bool}(false) for s in keys(contexts))
    monitor_tasks = Dict{Schema.T,Task}()
    for (sch, ctx) in contexts
        monitor_tasks[sch] = @async _session_monitor(ctx, Float64(heartbeat_log_interval_s),
                                                     monitor_stops[sch])
    end

    session_task = @async try
        _run_unified_session(contexts;
            dataset = dataset, stype_in = stype_in, symbols = symbols,
            reconnect = reconnect,
            max_reconnect_attempts = max_reconnect_attempts,
            deadline = deadline,
            key = key, gateway = gateway, port = port,
            wire_compression = wire_compression,
            heartbeat_interval = heartbeat_interval,
            slow_reader_behavior = slow_reader_behavior,
            channel_size = channel_size,
            start_initial = start, snapshot = snapshot,
        )
    catch e
        @error "live session crashed" exception=(e, catch_backtrace())
    end

    try
        while !istaskdone(session_task)
            sleep(_CONNECTION_POLL_S)
            if deadline !== nothing && time() >= deadline
                for ctx in values(contexts); ctx.shutdown_requested[] = true; end
                break
            end
        end
    catch e
        if e isa InterruptException
            @warn "interrupt — stopping live capture"
            for ctx in values(contexts); _request_shutdown!(ctx); end
        else
            rethrow()
        end
    end

    join_deadline = time() + _SHUTDOWN_JOIN_TIMEOUT_S
    while !istaskdone(session_task) && time() < join_deadline
        sleep(0.1)
    end
    for s in keys(monitor_stops); monitor_stops[s][] = true; end
    for t in values(monitor_tasks)
        try; istaskdone(t) || sleep(0.1); catch; end
    end

    out = Dict{Schema.T,String}()
    for (sch, ctx) in contexts
        _close!(ctx.file)
        out[sch] = ctx.file.current_path
    end
    return out
end
