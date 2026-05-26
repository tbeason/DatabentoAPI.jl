# Capture to File

```@meta
CurrentModule = DatabentoAPI
```

The Live capture functions wrap a [`Live`](@ref) client + a rotating DBN
writer + a reconnect loop into one call. Two convenience functions cover the
common cases, plus a do-block writer if you want to drive the loop yourself.

## Single schema: `stream_to_file`

The simplest "subscribe and dump every record to disk" call:

```julia
using DatabentoAPI

path = stream_to_file(;
    schema   = Schema.TRADES,
    symbols  = ["ES.FUT"],
    dataset  = "GLBX.MDP3",
    stype_in = SType.PARENT,
    base_dir = "./capture",
    duration_s = 600)            # stop after 10 minutes; omit for run-forever
@show path                       # e.g. ./capture/GLBX.MDP3/trades/20260526T143012Z.dbn.zst
```

Output is a zstd-compressed DBN file by default. The path is
`{base_dir}/{dataset}/{schema}/{utc-yyyymmddTHHMMSSZ}.dbn.zst` —
see [`default_live_path`](@ref) for the format.

Press **Ctrl-C** for a clean shutdown — the active zstd frame footer is
flushed before the function returns.

## Multiple schemas on one TCP connection: `stream_multi_to_files`

```julia
paths = stream_multi_to_files(;
    schemas  = [Schema.TCBBO, Schema.CBBO_1S, Schema.STATUS],
    symbols  = ["SPXW.OPT"],
    dataset  = "OPRA.PILLAR",
    stype_in = SType.PARENT,
    base_dir = "./capture",
    duration_s = 60)
@show paths                       # Dict{Schema.T,String} — one file per schema
```

One Live client serves N schemas. The gateway dedupes `SymbolMappingMsg`
within a connection (~50% fewer mapping records than per-schema clients);
each schema's records route to its own `.dbn.zst`.

## In-file zstd frame rotation (`frame_seconds`)

Both functions accept `frame_seconds` (default `60.0`). Every minute, the
active zstd frame's footer is written and a fresh frame begins on the same
file. Multi-frame `.dbn.zst` is standards-compliant — any zstd reader
concatenates frames transparently — so a hard kill (SIGKILL, OOM, power
loss) loses at most one frame of records instead of corrupting the whole
file. Pass `frame_seconds = nothing` to write a single frame.

A quiet schema (e.g. STATUS) still has its zstd buffer flushed by the
session monitor on the same cadence, so even with no records flowing the
on-disk file stays current.

## Time-based file rotation (`rotate_seconds`)

Roll to a new output file at a fixed cadence:

```julia
stream_to_file(...; rotate_seconds = 3600)    # new file every hour
```

The previous file is closed cleanly (frame footer written) before the next
opens. Combine with `frame_seconds` for both crash safety and time-bucketed
files.

## Custom loops: `open_dbn_writer` + `write_record!`

For workflows where `stream_to_file` is too opinionated — e.g. you want to
filter records, dispatch to multiple writers, or write from a custom data
source — pair [`Live`](@ref) with [`open_dbn_writer`](@ref):

```julia
Live(dataset = "GLBX.MDP3") do live
    connect!(live)
    subscribe!(live; schema = Schema.TRADES, symbols = ["ES.FUT"], stype_in = SType.PARENT)
    start!(live)
    open_dbn_writer(;
        base_dir = "./capture", dataset = "GLBX.MDP3",
        schema   = Schema.TRADES, symbols = ["ES.FUT"], stype_in = SType.PARENT,
        frame_seconds = 60.0) do writer
        for rec in live
            rec isa DBN.TradeMsg && rec.size > 100 || continue
            write_record!(writer, rec)
        end
    end
end
```

The writer's `current_path` field exposes the most recently opened output
path. The do-block's `finally` closes the writer (frame footer + zstd footer
emitted in order).

## Reconnect semantics

`reconnect = true` (default) on `stream_to_file` / `stream_multi_to_files`
re-establishes the TCP session after a drop using full-jitter exponential
backoff (1s base, 60s cap):

```
sleep = rand() * min(cap, base * 2^(attempt-1))
```

After `max_reconnect_attempts` retries (default 10) the loop gives up and
returns the path it last wrote. Pass `nothing` for unlimited retries.

On reconnect, each schema re-subscribes with `start =` the lowest
per-instrument timestamp seen so far (ts_recv for BBO families, ts_event
otherwise), bounded to Databento's 24-hour replay window. Gaps shorter than
the disconnect duration get filled automatically.

## Decoding captured files

```julia
metadata, records = read_capture("capture/GLBX.MDP3/trades/20260526T143012Z.dbn.zst")
```

Returns the DBN metadata header plus the full record vector. Thin wrapper
around `DBN.read_dbn_with_metadata` re-exported here so you don't need to
`using DatabentoBinaryEncoding`.

For very large captures, decode with DBN.jl's typed callback API directly:

```julia
import DatabentoBinaryEncoding as DBN
DBN.foreach_record("capture/.../big.dbn.zst", DBN.TradeMsg) do trade
    # … process each trade …
end
```

## What ends up in the file?

Captured `.dbn.zst` files contain:

- All data records for the subscribed schema(s).
- `SymbolMappingMsg` records (interleaved with data records, in arrival
  order) so downstream tooling can resolve `instrument_id` → `raw_symbol`
  without a separate `resolve` call.

`SystemMsg` / `ErrorMsg` records are counted internally for the session log
but not written to disk — they're heartbeat / control noise that doesn't
belong in a record stream.
