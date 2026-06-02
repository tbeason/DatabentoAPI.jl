# Live Streaming

```@meta
CurrentModule = DatabentoAPI
```

The Live API is a TCP protocol at `live.databento.com`. The lifecycle is
deliberate so you can split connect / subscribe / start across functions and
control when records start flowing:

```julia
Live(dataset = "OPRA.PILLAR") do live
    connect!(live)                                # CRAM handshake
    subscribe!(live;                              # zero or more times
        schema   = Schema.TRADES,
        symbols  = ["SPY.OPT"],
        stype_in = SType.PARENT)
    start!(live)                                  # gateway begins sending
    for rec in live
        # … consume records …
    end
end                                               # close(live) runs in finally
```

## The do-block is the canonical form

`Live(args...; kwargs...) do client … end` mirrors `Base.open(f, path)`. It
runs your body inside a `try/finally` that calls `close(client)` on `Ctrl-C`,
exceptions, or normal exit — so the socket and channels are torn down without
your code carrying boilerplate.

The manual lifecycle works too if you prefer to thread the client through
your own structure:

```julia
live = Live(dataset = "OPRA.PILLAR")
try
    connect!(live); subscribe!(live; …); start!(live)
    for rec in live; …; end
finally
    close(live)
end
```

## Consumer patterns

### Untyped iterator (default)

The simplest pattern. Every record (data + control) arrives on a single
`Channel{DBN.DBNRecord}`; iterate, dispatch by type:

```julia
Live(dataset = "OPRA.PILLAR") do live
    connect!(live)
    subscribe!(live; schema = Schema.TRADES, symbols = ["SPY.OPT"],
                     stype_in = SType.PARENT)
    start!(live)
    for rec in live
        rec isa DBN.TradeMsg     && handle_trade(rec)
        rec isa DBN.SystemMsg    && @debug "system" rec.msg
        rec isa DBN.ErrorMsg     && (@error "gateway error" rec.err; break)
    end
end
```

Use [`subscribe_callback`](@ref) instead if you want a fire-and-forget task:

```julia
Live(dataset = "OPRA.PILLAR") do live
    connect!(live)
    subscribe!(live; schema = Schema.TRADES, symbols = ["SPY.OPT"],
                     stype_in = SType.PARENT)
    subscribe_callback(live, rec -> @info "tick" rec)
    start!(live)
    sleep(60)
end
```

### Typed mode (`typed = true`)

For high-throughput consumers, opt into per-schema typed channels:

```julia
Live(dataset = "OPRA.PILLAR", typed = true) do live
    connect!(live)
    ch_trades = subscribe!(live; schema = Schema.TRADES,  symbols = ["SPY.OPT"],
                                 stype_in = SType.PARENT)   # Channel{DBN.TradeMsg}
    ch_cbbo   = subscribe!(live; schema = Schema.CBBO_1S, symbols = ["SPY.OPT"],
                                 stype_in = SType.PARENT)   # Channel{DBN.CBBO1sMsg}
    start!(live)

    @async for rec in ch_trades              # type-stable: rec :: TradeMsg
        handle_trade(rec)
    end
    @async for rec in ch_cbbo                # type-stable: rec :: CBBO1sMsg
        handle_quote(rec)
    end
    @async for rec in control_channel(live)  # ErrorMsg / SystemMsg / SymbolMappingMsg
        rec isa DBN.ErrorMsg && @error "gateway error" rec.err
    end
    sleep(60)
end
```

In typed mode, [`subscribe!`](@ref) returns a `Channel{T}` for the concrete
record type rather than an `Int` sub_id. Per-record allocation drops ~85% on
real OPRA streams (see [Performance](@ref)). Control records route to
[`control_channel`](@ref).

Subscribing to a mixed-record schema like `Schema.MIX` errors at `subscribe!`
under typed mode — point those callers at the untyped iterator instead.

## Slow consumers: `SlowReaderBehavior`

If your consumer task can't keep up with the wire rate, the gateway has two
strategies:

- [`SlowReaderBehavior`](@ref)`.WARN` — buffer and send stale records, with
  periodic `SystemMsg` heartbeats noting the lag. Default for stateful
  schemas (MBO, MBP-10, status).
- `SlowReaderBehavior.SKIP` — drop records to keep current, then send an
  `ErrorMsg` summarizing what was skipped. Default for stateless schemas
  (MBP-1, CMBP-1, BBO-1s/1m, CBBO-1s/1m).

Override per-session:

```julia
Live(dataset = "OPRA.PILLAR",
     slow_reader_behavior = SlowReaderBehavior.WARN) do live
    # …
end
```

`channel_size` (default `10_000`) sets the per-channel buffer ahead of the
slow-reader logic. Bursty schemas paired with a slow disk archive benefit
from larger sizes.

## Reconnect: built into `Live`

Every `Live(...)` client has a reconnect supervisor on by default. When the
TCP socket drops, the supervisor reopens it, replays your stored
subscriptions with a per-instrument-min `start=` timestamp (so subscribed
instruments resume coverage at the earliest point we lost them, bounded
to a 24h replay window), and continues delivering records into the same
channels — your `for rec in live` loop sees a continuous stream with no
exception in between.

The retry schedule is *immediate-then-backoff*:

- The first `immediate_reconnect_attempts` retries (default `3`) fire
  with no sleep, to catch sub-second TCP blips (kernel-side RST plus a
  packet retransmit or two) without waiting on the 1s+ backoff floor.
- Subsequent retries use AWS-style full-jitter exponential backoff
  (`rand() * min(60, 2^(n-1))` seconds, where `n` is the backoff-phase
  attempt index) up to `max_reconnect_attempts` total (default `10`;
  pass `nothing` for unlimited).
- The retry budget refreshes whenever the new reader successfully
  delivers ≥1 data record, so a long-lived session that streams real
  data between drops keeps its full budget across the connection's
  lifetime.

To opt out of the supervisor entirely:

```julia
Live(dataset = "OPRA.PILLAR", reconnect_policy = :none) do live
    # Reader's EOF closes the channels and terminates iteration.
end
```

### Observing reconnects: [`add_reconnect_callback`](@ref)

Register a callback to be notified on each successful reconnect:

```julia
Live(dataset = "OPRA.PILLAR") do live
    add_reconnect_callback(live, (gap_start_ns, gap_end_ns) -> begin
        @info "reconnect gap" gap_s = (gap_end_ns - gap_start_ns) / 1e9
        # optional: gap-fill from historical, emit a metric, etc.
    end)
    connect!(live)
    subscribe!(live; schema = Schema.TRADES, symbols = ["SPY.OPT"],
                     stype_in = SType.PARENT)
    start!(live)
    for rec in live
        # …
    end
end
```

`gap_start_ns` is the latest per-instrument timestamp seen pre-drop
(minimum across instruments — the earliest point we lost coverage);
`gap_end_ns` is the gateway's `Metadata.start_ts` from the fresh
session. Both are `Int64` nanoseconds since the unix epoch. Convert to a
`DateTime` via `Dates.unix2datetime(ns / 1e9)` (lossy — Julia's
`DateTime` is millisecond resolution).

Callbacks fire from the supervisor task in registration order; errors
inside a callback are logged and swallowed (they do not perturb the
list or fail the reconnect).

### Gateway errors are terminal

If the gateway sends an `ErrorMsg` (auth failed, malformed subscription,
etc.), the supervisor *does not* reconnect — retrying would just re-hit
the same condition. The typed reader's control branch sets the
client's `terminal_error`, channels close, and your iterator exits
cleanly.

### Convenience: `live_session`

If you don't need to interleave logic between `connect!`, `subscribe!`,
and `start!`, [`live_session`](@ref) bundles the whole lifecycle into a
single do-block:

```julia
live_session(;
    dataset = "OPRA.PILLAR",
    subscriptions = [
        (; schema = Schema.TRADES,  symbols = ["SPY.OPT"], stype_in = SType.PARENT),
        (; schema = Schema.CBBO_1S, symbols = ["SPY.OPT"], stype_in = SType.PARENT),
    ],
) do live
    for rec in live
        # …
    end
end
```

`reconnect_policy` defaults to `:reconnect` (same as `Live(...)`).

## Closing the client

`close(client)` is idempotent. The Live do-block calls it for you on exit;
if you're managing the lifecycle manually, wrap subscribe/start/iterate in
a `try/finally close(client) end`. Channels close cleanly, the gateway
gets a final `stop` frame, the supervisor (if running) observes the
state transition and exits at its next iteration, and consumer tasks
exit on `InvalidStateException` from their pending `take!` calls.
