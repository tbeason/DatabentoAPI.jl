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
end

function RotatingDBNFile(; base_dir::AbstractString,
                          explicit_path::Union{Nothing,AbstractString} = nothing,
                          dataset::AbstractString,
                          schema::Schema.T,
                          symbols,
                          stype_in::SType.T,
                          compress::Bool,
                          compress_level::Integer,
                          rotate_seconds::Union{Nothing,Real})
    syms_vec = symbols isa AbstractString ? [String(symbols)] : String.(symbols)
    f = RotatingDBNFile(
        String(base_dir),
        explicit_path === nothing ? nothing : String(explicit_path),
        String(dataset), schema, syms_vec, stype_in,
        compress, Int(compress_level),
        rotate_seconds === nothing ? nothing : Float64(rotate_seconds),
        nothing, nothing, nothing, "", 0.0, 0, String[], ReentrantLock(),
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
    f.raw_io       = raw
    f.zstd_io      = top
    f.encoder      = enc
    f.current_path = path
    f.opened_at    = time()
    push!(f.all_paths, path)
    return nothing
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
        _rotate_if_needed!(f)
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

function _handle_record!(ctx::SessionContext, rec)
    s = ctx.stats
    if rec isa DBN.SymbolMappingMsg
        # Don't write SymbolMappingMsg to disk: live gateway sends v1-layout
        # records (80 bytes) but our file metadata is v3, so DBN.jl's encoder
        # would write them as v3 (~180 bytes) with the original hd.length=20,
        # desyncing the file. Schema-pure files are the simplest fix; if the
        # caller needs instrument_id → raw_symbol resolution, use the
        # `symbology.resolve` historical endpoint with the same symbols.
        s.mapping_count += 1
    elseif rec isa DBN.SystemMsg
        s.system_count += 1
        is_heartbeat(rec) || @debug "SystemMsg" schema=ctx.schema msg=rec.msg
        # Don't write SystemMsg to disk — heartbeats / status text are noise.
    elseif rec isa DBN.ErrorMsg
        s.error = String(rec.err)
        @error "gateway ErrorMsg (terminal)" schema=ctx.schema err=s.error
        # Don't write; let the channel close naturally and the session loop exit.
    elseif rec isa DBN.StatusMsg
        s.data_count    += 1
        s.last_record_at = time()
        _log_status_alarm!(s, rec, ctx.schema)
        _write_record!(ctx.file, rec)
    else
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
        _write_record!(ctx.file, rec)
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

        client = Live(key;
            dataset = String(dataset),
            gateway = gateway, port = port,
            ts_out = false,
            compression = wire_compression,
            heartbeat_interval = heartbeat_interval,
            slow_reader_behavior = slow_reader_behavior,
            channel_size = channel_size,
        )
        ctx.current_client = client
        try
            connect!(client)
            subscribe!(client; schema = ctx.schema, symbols = symbols,
                       stype_in = stype_in, snapshot = snapshot, start = start_ts)
            start!(client)

            for rec in client
                _handle_record!(ctx, rec)
                ctx.shutdown_requested[] && break
                deadline !== nothing && time() >= deadline && break
                while isready(ctx.flush_request)
                    try; take!(ctx.flush_request); catch; end
                    _flush!(ctx.file)
                end
            end
        catch e
            if !(e isa InvalidStateException) && !ctx.shutdown_requested[]
                @warn "live session error" schema=ctx.schema exception=(e, catch_backtrace())
            end
        finally
            ctx.current_client = nothing
            try; close(client); catch; end
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
        last_count = cur
        last_tick  = now
    end
    return nothing
end

# ---------- public API ----------

"""
    read_capture(path) -> (DBN.Metadata, Vector{DBN.DBNRecord})

Decode a `.dbn` or `.dbn.zst` file written by [`stream_to_file`](@ref) /
[`stream_multi_to_files`](@ref). Equivalent to calling `DBN.read_dbn(path)`,
but reads the file via an in-memory `IOBuffer` to sidestep a known
`DBN.BufferedReader` bug that loses bytes straddling its 64 KiB buffer
boundary on files larger than that.
"""
function read_capture(path::AbstractString)
    raw = open(read, String(path))
    body = endswith(String(path), ".zst") ?
           transcode(ZstdDecompressor, raw) : raw
    decoder = DBN.DBNDecoder(IOBuffer(body))
    DBN.read_header!(decoder)
    records = DBN.DBNRecord[]
    while true
        rec = DBN.read_record(decoder)
        rec === nothing && break
        push!(records, rec)
    end
    return decoder.metadata, records
end

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
"""
function stream_to_file(; schema::Schema.T,
                        symbols,
                        dataset::AbstractString,
                        stype_in::SType.T = SType.RAW_SYMBOL,
                        path::Union{Nothing,AbstractString} = nothing,
                        base_dir::Union{Nothing,AbstractString} = nothing,
                        duration_s::Union{Nothing,Real} = nothing,
                        compress::Bool = true,
                        compress_level::Integer = 3,
                        rotate_seconds::Union{Nothing,Real} = nothing,
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
    )
    ctx = SessionContext(schema, SessionStats(schema = schema), file)

    deadline = duration_s === nothing ? nothing : time() + Float64(duration_s)
    monitor_stop = Threads.Atomic{Bool}(false)
    monitor_task = @async _session_monitor(ctx, Float64(heartbeat_log_interval_s), monitor_stop)

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
        try; istaskdone(monitor_task) || sleep(0.1); catch; end
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
                               compress_level::Integer = 3,
                               rotate_seconds::Union{Nothing,Real} = nothing,
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
