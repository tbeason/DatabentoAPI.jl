# Reconnect supervisor: keeps a Live session alive across TCP drops by
# spawning fresh reader tasks against the same user-facing channels and
# re-issuing stored subscriptions on each reconnect.
#
# Lives in the Live module so any consumer (iteration, subscribe_callback,
# stream_to_file) benefits — reconnect is no longer streaming-layer-only.
#
# Schedule: c.immediate_reconnect_attempts immediate (no-sleep) retries to
# catch sub-second TCP blips, then full-jitter exponential backoff
# (_RECONNECT_BASE_S → _RECONNECT_CAP_S) for up to c.max_reconnect_attempts
# total attempts before giving up. The retry budget refreshes whenever the
# new reader successfully delivers ≥1 data record (see _record_replay! in
# reader.jl), so a long-lived session that streams between drops keeps its
# full budget across the connection's lifetime.

"""
    add_reconnect_callback(client::Live, cb)

Register `cb(gap_start_ns::Int64, gap_end_ns::Int64)` to fire after each
successful reconnect. Both timestamps are nanoseconds since the unix epoch.

`gap_start_ns` is the latest pre-drop per-instrument timestamp the client
had observed (min across instruments, using `ts_recv` for BBO-family
schemas and `ts_event` otherwise); `0` if no records were seen pre-drop.

`gap_end_ns` is the gateway's `Metadata.start_ts` from the freshly
reconnected session; `0` if unavailable.

Errors raised inside `cb` are logged via `@error` and swallowed — they do
not perturb the callback list or fail the reconnect.

Callbacks are invoked sequentially on the supervisor task in registration
order. Convert to `Dates.DateTime` via `Dates.unix2datetime(ns / 1e9)` if
needed (lossy — pandas-style nanosecond precision is preserved only in the
raw `Int64`).
"""
function add_reconnect_callback(c::Live, cb)
    cb isa Function || cb isa Base.Callable || throw(ArgumentError(
        "reconnect callback must be callable, got $(typeof(cb))"))
    lock(c.callbacks_lock) do
        push!(c.reconnect_callbacks, cb)
    end
    return nothing
end

# Snapshot callbacks under the lock, then fire them outside it so a slow
# callback can't block add_reconnect_callback.
function _fire_reconnect_callbacks(c::Live, gap_start_ns::Int64, gap_end_ns::Int64)
    cbs = lock(c.callbacks_lock) do
        copy(c.reconnect_callbacks)
    end
    for cb in cbs
        try
            cb(gap_start_ns, gap_end_ns)
        catch e
            @error "reconnect callback raised" dataset=c.dataset exception=(e, catch_backtrace())
        end
    end
    return nothing
end

# Best-effort close of every user-facing channel. Used on terminal-state
# transitions (:failed / :closed) to signal iterators / subscribe_callback
# consumers that no more records are coming, since under RECONNECT the
# reader's finally block leaves channels open for the supervisor.
function _close_channels!(c::Live)
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

function _channels_open(c::Live)
    if c.channel !== nothing && !isopen(c.channel)
        return false
    end
    for ch in values(c.typed_data_channels)
        isopen(ch) || return false
    end
    if c.control_channel !== nothing && !isopen(c.control_channel)
        return false
    end
    return true
end

# Pick the gap_start timestamp to report to callbacks. Use the smallest
# per-instrument timestamp seen across any subscribed schema — this is the
# earliest point we lost coverage. Falls back to 0 if no records were seen.
function _gap_start_ns(c::Live)
    best = typemax(Int64)
    seen = false
    if !isempty(c.last_ts_event_by_id)
        v = minimum(values(c.last_ts_event_by_id))
        best = min(best, v); seen = true
    end
    if !isempty(c.last_ts_recv_by_id)
        v = minimum(values(c.last_ts_recv_by_id))
        best = min(best, v); seen = true
    end
    return seen ? best : Int64(0)
end

# Compute how many attempts the supervisor has burned in the current burst
# (since the last successful first-record). Used as the 1-based `attempt`
# arg to _reconnect_delay so the cadence (immediate phase → backoff)
# tracks each disconnect's retries independently.
function _attempts_used_this_burst(c::Live)
    init = c.max_reconnect_attempts === nothing ? typemax(Int) : c.max_reconnect_attempts
    used = init - c.attempts_remaining
    return used < 0 ? 0 : used   # paranoia against any decrement-past-zero
end

# Polling sleep that wakes promptly on user-initiated close, so a long
# backoff window doesn't pin shutdown responsiveness.
function _interruptible_sleep(c::Live, delay::Float64)
    delay > 0.0 || return
    deadline = time() + delay
    while time() < deadline
        c.closed && return
        c.state == :closed && return
        sleep(min(_CONNECTION_POLL_S, deadline - time()))
    end
    return
end

# Re-issue a stored subscription on the freshly-reconnected socket. Doesn't
# touch c.subscriptions (already there) or c.next_sub_id (already advanced).
# `start_override` lets the supervisor inject the per-instrument-min replay
# timestamp instead of the original user-supplied start.
function _send_subscribe_frame(c::Live, sub; start_override::Union{Nothing,Integer} = nothing)
    write_text_frame(c.socket;
        schema   = schema_str(sub.schema),
        stype_in = stype_str(sub.stype_in),
        symbols  = sub.symbols,
        snapshot = sub.snapshot ? 1 : 0,
        start    = start_override === nothing ? sub.start : start_override,
        id       = sub.id,
        is_last  = 1,
    )
    return nothing
end

# Attempt a single reconnect: close stale socket, fresh socket + auth,
# replay subscriptions with computed start_ts, send start_session, spawn a
# new reader. Returns true on success (state advances to :streaming),
# false on any failure (state stays :reconnecting; supervisor will retry).
function _reconnect_once!(c::Live)
    # Best-effort teardown of the dead session's socket.
    try
        c.socket === nothing || Sockets.close(c.socket)
    catch
    end
    c.socket = nothing
    c.connected = false

    # User closed us mid-reconnect — bail.
    c.closed && return false

    # Fresh socket + CRAM handshake. _open_socket_and_auth! throws on
    # network or auth failure; supervisor's caller will catch.
    _open_socket_and_auth!(c)
    c.connected = true

    # Replay subscriptions. Don't use public subscribe! — it errors on
    # c.started=true (a sanity check for the *initial* lifecycle) and
    # would push duplicates onto c.subscriptions.
    saved = copy(c.subscriptions)
    for sub in saved
        start_override = _bound_replay(_replay_start_ts(c, sub.schema))
        _send_subscribe_frame(c, sub; start_override = start_override)
    end

    write_text_frame(c.socket; start_session = 0)

    # Bail if a racing close() closed our channels while we were re-auth'ing.
    if !_channels_open(c)
        return false
    end

    # Reset per-reader-incarnation counter before spawning so the first
    # record from the new reader triggers the budget refresh in
    # _record_replay!. Also clear last_metadata_start_ns so we can detect
    # when the new reader has populated it (post-spawn poll below).
    c.records_since_reconnect = 0
    prev_md_ns = c.last_metadata_start_ns
    c.last_metadata_start_ns = nothing

    # Spawn the new reader against the *same* channels. Under RECONNECT
    # policy start! did not bind them to the original reader_task, so they
    # remained open after the prior reader exited.
    c.reader_task = if c.typed
        @async _reader_loop_typed(c)
    else
        @async _reader_loop(c)
    end

    # Wait for the new reader to read its DBN header and populate
    # last_metadata_start_ns, so the reconnect callback can fire with the
    # correct gap_end. The first reconnect on a cold-JIT process can take
    # several seconds for the reader's read_header! path to compile; once
    # warm, this is sub-ms. Bounded — if the new reader stalls before the
    # header arrives, we fall back to the prior value (callback gap_end
    # ≈ pre-drop reference, the closest signal we have).
    md_deadline = time() + 10.0
    while time() < md_deadline && c.last_metadata_start_ns === nothing
        c.closed && break
        sleep(0.02)
    end
    if c.last_metadata_start_ns === nothing
        c.last_metadata_start_ns = prev_md_ns
    end

    lock(c.state_lock) do
        if c.state == :reconnecting
            c.state = :streaming
        end
    end
    return true
end

# Supervisor task. Created from start! when reconnect_policy != NONE.
# Lifecycle:
#   :streaming → (reader dies) → :reconnecting → :streaming (or :failed)
# Terminates when c.state hits :closed or :failed.
function _supervisor_loop(c::Live)
    while true
        # Block until the current reader exits. Reader exceptions are
        # already logged inside _reader_loop[_typed]; we swallow whatever
        # wait raises so the supervisor can keep running.
        rdr = c.reader_task
        rdr === nothing && return
        try
            wait(rdr)
        catch
        end

        # Decide: reconnect, or terminate?
        decision = lock(c.state_lock) do
            if c.state == :closed || c.state == :failed
                return :exit
            end
            if c.terminal_error !== nothing
                @error "live reconnect: gateway sent terminal ErrorMsg" dataset=c.dataset err=c.terminal_error
                c.state = :failed
                c.started = false
                c.connected = false
                return :terminate
            end
            if c.reconnect_policy == ReconnectPolicy.NONE
                # Defensive — we shouldn't have been spawned under NONE.
                c.state = :closed
                return :terminate
            end
            if c.attempts_remaining <= 0
                @error "live reconnect: budget exhausted; giving up" dataset=c.dataset
                c.state = :failed
                c.started = false
                c.connected = false
                return :terminate
            end
            c.state = :reconnecting
            c.connected = false
            return :reconnect
        end

        if decision == :terminate
            _close_channels!(c)
            return
        elseif decision == :exit
            return
        end

        # Sleep per the schedule. attempt index = used_this_burst + 1.
        attempt = _attempts_used_this_burst(c) + 1
        delay = _reconnect_delay(attempt;
                                  immediate = c.immediate_reconnect_attempts,
                                  base = _RECONNECT_BASE_S,
                                  cap  = _RECONNECT_CAP_S)
        _interruptible_sleep(c, delay)
        if c.state == :closed || c.closed
            _close_channels!(c)
            return
        end

        # Snapshot pre-drop replay state for the callback BEFORE the
        # reconnect attempt (after, c.last_ts_*_by_id may be advanced by
        # the new reader before the callback fires).
        gap_start = _gap_start_ns(c)

        ok = try
            _reconnect_once!(c)
        catch e
            if e isa BentoAuthError
                # Auth failures are deterministic and won't fix themselves —
                # don't burn budget retrying.
                @error "live reconnect: auth failed; giving up" dataset=c.dataset exception=(e, catch_backtrace())
                lock(c.state_lock) do
                    c.state = :failed
                    c.started = false
                    c.connected = false
                end
                _close_channels!(c)
                return
            end
            @warn "live reconnect attempt failed" dataset=c.dataset attempt=attempt exception=(e, catch_backtrace())
            false
        end

        c.attempts_remaining -= 1

        if ok
            @info "live reconnect: reconnected" dataset=c.dataset attempt=attempt session_id=c.session_id
            gap_end = c.last_metadata_start_ns === nothing ? Int64(0) : c.last_metadata_start_ns
            _fire_reconnect_callbacks(c, gap_start, gap_end)
            # Loop back to wait on the new reader. The budget will refresh
            # in _record_replay! when the first record arrives.
        else
            # _reconnect_once! returned false (couldn't open channels /
            # user closed) — re-check on next iteration. State stays
            # :reconnecting so the next wait() returns immediately
            # (reader_task is the just-spawned-but-doomed one, or unchanged
            # if we never got to spawn).
        end
    end
end
