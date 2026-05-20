# Multi-schema live capture to compressed DBN files.
#
# Each schema gets its own Live client, reader task, monitor task, and output
# file. Records flow:
#
#     gateway → Live.channel → _run_session loop → _handle_record! → DBNEncoder
#
# A separate monitor task wakes every `heartbeat_log_interval_s` to log
# throughput and request a flush via `ctx.flush_request` (serviced on the
# consumer task under the file lock).
#
# Reconnect: on TCP drop, re-build the Live and resubscribe with `start =`
# the lowest per-instrument timestamp seen so far (ts_recv for BBO families,
# ts_event otherwise), bounded to within Databento's 24h replay window.
# `ErrorMsg` from the gateway is treated as terminal (no reconnect).

const _RECONNECT_WAIT_S        = 1.0
const _STALE_THRESHOLD_S       = 30.0
const _SHUTDOWN_JOIN_TIMEOUT_S = 15.0
const _CONNECTION_POLL_S       = 0.5
const _REPLAY_WINDOW_NS        = Int64(24 * 3600) * 1_000_000_000

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
    last_ts_event_by_id::Dict{UInt32,Int64} = Dict{UInt32,Int64}()
    last_ts_recv_by_id::Dict{UInt32,Int64}  = Dict{UInt32,Int64}()
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
    # Sidecar JSONL file capturing SymbolMappingMsg events. Rotates alongside
    # the main DBN file. We don't write mappings into the .dbn.zst itself
    # because the live gateway emits them in v1 layout (80 bytes) while our
    # file metadata is v3 — DBN.jl's encoder would write them as v3 (~180
    # bytes) with the wire hd.length=20, corrupting the file. JSONL sidecar
    # avoids that mismatch entirely and is human-readable / easy to join.
    sidecar_io::Union{Nothing,IO}
    sidecar_path::String
    sidecar_paths::Vector{String}
    # In-file zstd frame rotation for crash safety. Every `frame_seconds`,
    # close the current zstd frame (writes footer to raw_io) and open a new
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
        nothing, "", String[],
        frame_seconds === nothing ? nothing : Float64(frame_seconds),
        0.0, 0,
    )
    _open!(f)
    return f
end

# Sidecar path mirrors the main DBN file, swapping .dbn[.zst] for
# .symbology.jsonl. Lives in the same per-schema directory.
function _sidecar_path_for(dbn_path::AbstractString)
    p = String(dbn_path)
    base = endswith(p, ".dbn.zst") ? p[1:end-8] :
           endswith(p, ".dbn")     ? p[1:end-4] : p
    return base * ".symbology.jsonl"
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
    # Open the symbology sidecar alongside the DBN file.
    sc_path = _sidecar_path_for(path)
    f.sidecar_io   = open(sc_path, "w")
    f.sidecar_path = sc_path
    push!(f.sidecar_paths, sc_path)
    return nothing
end

# In-file zstd frame rotation. Writes TranscodingStreams' TOKEN_END through
# the active zstd stream + flushes, which makes the codec emit the zstd
# frame footer to raw_io. Subsequent writes start a fresh frame on the
# same stream — the resulting .dbn.zst is a multi-frame zstd file, fully
# standards-compliant. DBN-level layout is unaffected: the metadata
# header was written once at file open, and the decompressed output is
# concatenated across frames, so readers see "header then records".
#
# Crucially this does NOT close the underlying TranscodingStream (which
# would also close raw_io), and does NOT recreate the DBN encoder — same
# instance keeps working across the frame boundary.
function _rotate_frame_if_needed!(f::RotatingDBNFile)
    f.frame_seconds === nothing && return false
    f.compress                  || return false   # only meaningful for compressed files
    # Acquire write_lock so we can be called either from _write_record!
    # (already-held lock; ReentrantLock allows reentrance) or from the
    # session monitor (separate task, lock-free entry point).
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
    try
        f.sidecar_io === nothing || (flush(f.sidecar_io); close(f.sidecar_io))
    catch
    end
    f.encoder    = nothing
    f.zstd_io    = nothing
    f.raw_io     = nothing
    f.sidecar_io = nothing
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
        f.zstd_io    === nothing || flush(f.zstd_io)
        f.raw_io     === nothing || flush(f.raw_io)
        f.sidecar_io === nothing || flush(f.sidecar_io)
    end
    return nothing
end

_close!(f::RotatingDBNFile) = lock(() -> _close_stack!(f), f.write_lock)

# Append one JSON line per SymbolMappingMsg to the sidecar. Held under the
# same write_lock as DBN writes so it can't race with a rotation.
function _append_symbology!(f::RotatingDBNFile, rec::DBN.SymbolMappingMsg)
    lock(f.write_lock) do
        f.sidecar_io === nothing && return nothing
        m = (
            ts_event         = rec.hd.ts_event,
            instrument_id    = rec.hd.instrument_id,
            stype_in         = string(Symbol(rec.stype_in)),
            stype_in_symbol  = rec.stype_in_symbol,
            stype_out        = string(Symbol(rec.stype_out)),
            stype_out_symbol = rec.stype_out_symbol,
            start_ts         = rec.start_ts,
            end_ts           = rec.end_ts,
        )
        JSON3.write(f.sidecar_io, m)
        write(f.sidecar_io, '\n')
        # Mappings are sparse (one per (schema, instrument) at subscribe time,
        # then occasional updates). Flushing per-line is cheap and means a
        # crashed capture still has a recoverable, complete sidecar.
        flush(f.sidecar_io)
    end
    return nothing
end

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

function _replay_start_ts(stats::SessionStats, schema::Schema.T)
    d = _is_bbo_family(schema) ? stats.last_ts_recv_by_id : stats.last_ts_event_by_id
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

# Data-record path: writes the record to disk + tracks per-instrument
# replay timestamps. Under typed mode this runs once per record off the
# typed data channel; the record type is concrete at the call site.
function _handle_data!(ctx::SessionContext, rec)
    s = ctx.stats
    s.data_count    += 1
    s.last_record_at = time()
    if hasproperty(rec, :hd) && hasproperty(rec.hd, :instrument_id)
        iid = rec.hd.instrument_id
        if hasproperty(rec.hd, :ts_event)
            s.last_ts_event_by_id[iid] = rec.hd.ts_event
        end
        if hasproperty(rec, :ts_recv)
            s.last_ts_recv_by_id[iid] = rec.ts_recv
        end
    end
    # StatusMsg additionally checks for alarm-worthy state transitions.
    if rec isa DBN.StatusMsg
        _log_status_alarm!(s, rec, ctx.schema)
    end
    _write_record!(ctx.file, rec)
    return nothing
end

# Control-record path: SymbolMappingMsg / SystemMsg / ErrorMsg.
# Counts + special handling per type. SymbolMappingMsg goes to the per-file
# JSONL sidecar (not the .dbn.zst, because gateway emits v1-layout records
# that DBN.jl's v3 encoder would corrupt the file with). SystemMsg/ErrorMsg
# are just counted/logged.
function _handle_control!(ctx::SessionContext, rec)
    s = ctx.stats
    if rec isa DBN.SymbolMappingMsg
        s.mapping_count += 1
        _append_symbology!(ctx.file, rec)
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

# ---------- per-schema reconnect loop ----------

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

function _run_session(ctx::SessionContext;
                     dataset::AbstractString,
                     stype_in::SType.T,
                     symbols,
                     reconnect::Bool,
                     deadline::Union{Nothing,Float64},
                     key, gateway, port::Integer,
                     wire_compression::Compression.T,
                     heartbeat_interval, slow_reader_behavior,
                     channel_size::Integer,
                     start_initial,
                     snapshot::Bool)
    while true
        ctx.shutdown_requested[] && return
        deadline !== nothing && time() >= deadline && return

        start_ts = ctx.stats.reconnects == 0 ? start_initial :
                                               _replay_start_ts(ctx.stats, ctx.schema)
        start_ts = _bound_replay(start_ts)

        # typed=true: the reader splits data records (concrete type, typed
        # Channel{T}) from control records (Union, control_channel). Two
        # consumer paths inside this iteration of the reconnect loop:
        #   - main loop drains `data_ch` and writes records to file
        #   - a control-drainer task drains control_channel and updates stats
        client = Live(key;
            dataset = String(dataset),
            gateway = gateway, port = port,
            ts_out = false,
            compression = wire_compression,
            heartbeat_interval = heartbeat_interval,
            slow_reader_behavior = slow_reader_behavior,
            channel_size = channel_size,
            typed = true,
        )
        ctx.current_client = client
        ctrl_drainer::Union{Nothing,Task} = nothing
        try
            connect!(client)
            data_ch = subscribe!(client;
                schema = ctx.schema, symbols = symbols,
                stype_in = stype_in, snapshot = snapshot, start = start_ts)
            start!(client)

            ctrl_drainer = @async begin
                try
                    for rec in control_channel(client)
                        _handle_control!(ctx, rec)
                    end
                catch e
                    if !(e isa InvalidStateException) && !ctx.shutdown_requested[]
                        @warn "control drainer error" schema=ctx.schema exception=(e, catch_backtrace())
                    end
                end
            end

            try
                while true
                    rec = try
                        take!(data_ch)
                    catch e
                        e isa InvalidStateException && break
                        rethrow()
                    end
                    _handle_data!(ctx, rec)
                    ctx.shutdown_requested[] && break
                    deadline !== nothing && time() >= deadline && break
                    # Bail fast if the control drainer flagged a terminal
                    # ErrorMsg — no point reading more data records.
                    ctx.stats.error !== nothing && break
                    while isready(ctx.flush_request)
                        try; take!(ctx.flush_request); catch; end
                        _flush!(ctx.file)
                    end
                end
            catch e
                if !(e isa InvalidStateException) && !ctx.shutdown_requested[]
                    @warn "live session error" schema=ctx.schema exception=(e, catch_backtrace())
                end
            end
        catch e
            if !(e isa InvalidStateException) && !ctx.shutdown_requested[]
                @warn "live session error" schema=ctx.schema exception=(e, catch_backtrace())
            end
        finally
            ctx.current_client = nothing
            try; close(client); catch; end
            # Closing the client closes the control channel; wait for the
            # drainer to drain in-flight records and exit cleanly.
            ctrl_drainer === nothing || (try; wait(ctrl_drainer); catch; end)
        end

        ctx.shutdown_requested[]                         && return
        ctx.stats.error !== nothing                      && return
        !reconnect                                       && return
        deadline !== nothing && time() >= deadline       && return
        ctx.stats.reconnects += 1
        @warn "live disconnect — reconnecting" schema=ctx.schema attempt=ctx.stats.reconnects
        sleep(_RECONNECT_WAIT_S)
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
        # runs when records arrive, and the session loop blocks on take!
        # when the data channel is empty. Without this, a quiet schema's
        # in-memory zstd buffer (including the DBN metadata header) never
        # reaches disk and a hard kill loses the entire file.
        try; _flush!(ctx.file); catch; end
        try; _rotate_frame_if_needed!(ctx.file); catch; end
        last_count = cur
        last_tick  = now
    end
    return nothing
end

# ---------- sentinel watcher ----------

# Polls for the existence of `sentinel_path` once per second. On detection,
# calls _request_shutdown! on every context so the capture exits cleanly
# through its finally → _close_stack! path (no truncated zstd frames).
# Removes the sentinel after triggering so a leftover file doesn't cause
# the next launch to shut down immediately.
function _watch_sentinel(contexts, sentinel_path::String, stop::Threads.Atomic{Bool})
    while !stop[]
        try
            if isfile(sentinel_path)
                @info "stop sentinel detected — initiating clean shutdown" path=sentinel_path
                for ctx in contexts; _request_shutdown!(ctx); end
                try; rm(sentinel_path; force = true); catch; end
                return nothing
            end
        catch e
            @warn "sentinel watcher error" exception=e
        end
        sleep(1.0)
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
                        stop_sentinel::Union{Nothing,AbstractString} = nothing,
                        reconnect::Bool = true,
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
    file = RotatingDBNFile(
        base_dir = base, explicit_path = path,
        dataset = dataset, schema = schema, symbols = symbols, stype_in = stype_in,
        compress = compress, compress_level = compress_level,
        rotate_seconds = path === nothing ? rotate_seconds : nothing,
        frame_seconds = frame_seconds,
    )
    ctx = SessionContext(schema, SessionStats(schema = schema), file)

    deadline = duration_s === nothing ? nothing : time() + Float64(duration_s)
    monitor_stop = Threads.Atomic{Bool}(false)
    monitor_task = @async _session_monitor(ctx, Float64(heartbeat_log_interval_s), monitor_stop)

    sentinel = stop_sentinel === nothing ? joinpath(base, "STOP") : String(stop_sentinel)
    # Stale sentinel from a previous killed run would trigger immediate
    # shutdown — clear it on startup so the watcher only fires on a fresh
    # touch from this session onward.
    if isfile(sentinel)
        @warn "removing leftover stop sentinel" path=sentinel
        try; rm(sentinel; force = true); catch; end
    end
    sentinel_stop = Threads.Atomic{Bool}(false)
    sentinel_task = @async _watch_sentinel((ctx,), sentinel, sentinel_stop)

    try
        _run_session(ctx;
            dataset = dataset, stype_in = stype_in, symbols = symbols,
            reconnect = reconnect, deadline = deadline,
            key = key, gateway = gateway, port = port,
            wire_compression = wire_compression,
            heartbeat_interval = heartbeat_interval,
            slow_reader_behavior = slow_reader_behavior,
            channel_size = channel_size,
            start_initial = start, snapshot = snapshot,
        )
    finally
        monitor_stop[] = true
        sentinel_stop[] = true
        try; istaskdone(monitor_task) || sleep(0.1); catch; end
        try; istaskdone(sentinel_task) || sleep(0.1); catch; end
        _close!(file)
    end
    return ctx.file.current_path
end

"""
    stream_multi_to_files(; schemas, symbols, dataset, ...) -> Dict{Schema.T,String}

Capture N Live schemas concurrently, one rotating-DBN file per schema. Returns
a dict mapping each schema to its most recently opened output path. Press
`Ctrl-C` to stop early; honours `duration_s` if provided.

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
                               stop_sentinel::Union{Nothing,AbstractString} = nothing,
                               reconnect::Bool = true,
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
    worker_tasks  = Dict{Schema.T,Task}()
    for (sch, ctx) in contexts
        monitor_tasks[sch] = @async _session_monitor(ctx, Float64(heartbeat_log_interval_s),
                                                     monitor_stops[sch])
        worker_tasks[sch]  = @async try
            _run_session(ctx;
                dataset = dataset, stype_in = stype_in, symbols = symbols,
                reconnect = reconnect, deadline = deadline,
                key = key, gateway = gateway, port = port,
                wire_compression = wire_compression,
                heartbeat_interval = heartbeat_interval,
                slow_reader_behavior = slow_reader_behavior,
                channel_size = channel_size,
                start_initial = start, snapshot = snapshot,
            )
        catch e
            @error "live worker crashed" schema=sch exception=(e, catch_backtrace())
        end
    end

    sentinel = stop_sentinel === nothing ? joinpath(base, "STOP") : String(stop_sentinel)
    if isfile(sentinel)
        @warn "removing leftover stop sentinel" path=sentinel
        try; rm(sentinel; force = true); catch; end
    end
    sentinel_stop = Threads.Atomic{Bool}(false)
    sentinel_task = @async _watch_sentinel(values(contexts), sentinel, sentinel_stop)

    try
        while any(!istaskdone(t) for t in values(worker_tasks))
            sleep(_CONNECTION_POLL_S)
            if deadline !== nothing && time() >= deadline
                for ctx in values(contexts); ctx.shutdown_requested[] = true; end
                break
            end
        end
    catch e
        if e isa InterruptException
            @warn "interrupt — stopping all live captures"
            for ctx in values(contexts); _request_shutdown!(ctx); end
        else
            rethrow()
        end
    end

    join_deadline = time() + _SHUTDOWN_JOIN_TIMEOUT_S
    for t in values(worker_tasks)
        while !istaskdone(t) && time() < join_deadline
            sleep(0.1)
        end
    end
    for s in keys(monitor_stops); monitor_stops[s][] = true; end
    sentinel_stop[] = true
    for t in values(monitor_tasks)
        try; istaskdone(t) || sleep(0.1); catch; end
    end
    try; istaskdone(sentinel_task) || sleep(0.1); catch; end

    out = Dict{Schema.T,String}()
    for (sch, ctx) in contexts
        _close!(ctx.file)
        out[sch] = ctx.file.current_path
    end
    return out
end
