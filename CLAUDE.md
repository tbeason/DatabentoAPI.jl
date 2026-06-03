# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

DatabentoAPI.jl is a Julia client for the [Databento](https://databento.com)
market-data APIs: the **Historical** HTTP API (backfills, metadata, batch jobs,
symbology) and the **Live** TCP API (real-time feeds with reconnect, multi-schema
routing, and crash-safe capture to disk). All binary DBN record decoding/encoding
is delegated to a separate package, `DatabentoBinaryEncoding` (imported as `DBN`).

## Commands

```bash
# Run the full offline test suite (~30s, 1500+ tests). Uses a temp env with Test.
julia --project=. -e 'using Pkg; Pkg.test()'

# Run a single test file. Test files are plain includes that assume `using Test`,
# `using DatabentoAPI`, and the tcp_mock are already loaded:
julia --project=. -e 'using Test, DatabentoAPI; include("test/mocks/tcp_mock.jl"); include("test/test_live_subscribe.jl")'

# Build the package (precompile deps)
julia --project=. -e 'using Pkg; Pkg.instantiate()'

# Benchmarks (separate benchmark/ project; writes CSVs to benchmark/results/)
julia --project=benchmark benchmark/runbench.jl                 # default tiers small,medium
julia --project=benchmark benchmark/runbench.jl --profile        # + profiling pass
julia --project=benchmark benchmark/runbench.jl --tiers=small    # override size tiers

# Docs (Documenter.jl) — first develop the repo package into the docs env, then build
julia --project=docs -e 'using Pkg; Pkg.develop(PackageSpec(path=pwd())); Pkg.instantiate()'
julia --project=docs docs/make.jl
```

### Live-network tests

`test/runtests.jl` runs offline tests by default and **only** runs the
`test/live/` smoke tests when `DATABENTO_LIVE_TESTS=1` is set (they hit the real
gateway and need a valid API key). The offline suite never touches the network —
the Live gateway is faked by an in-process TCP server (`test/mocks/tcp_mock.jl`,
`spawn_mock_gateway` / `spawn_mock_gateway_sequence`); HTTP is mocked by swapping
the `HTTPClient.dispatcher` function.

The offline `@testset` is wrapped in a `try/catch` so a failure still lets gated
live tests run, then re-throws at the end — i.e. exit status reflects offline
failures even when live tests are enabled.

## Architecture

### DBN delegation boundary

This package owns the **wire protocols** (HTTP requests, the Live TCP/CRAM
handshake, framing) and **session orchestration**. It owns *no* binary record
layout knowledge — every `read_*_msg` / `write_record` / `Metadata` / `Schema` /
`SType` / record struct comes from `DatabentoBinaryEncoding`, aliased throughout
as `DBN` (`import DatabentoBinaryEncoding as DBN`). DBN enums (`Schema`, `SType`,
`Compression`, `Action`, `Side`, `InstrumentClass`) are re-exported so users need
a single `using DatabentoAPI`. If a record decodes wrong or a new schema/rtype is
needed, the fix usually belongs in DBN, not here.

`src/DatabentoAPI.jl` is the include manifest and the single `export` list — the
public API surface is whatever it exports.

### Two clients

- **`Historical`** (`src/historical/`) wraps `HTTPClient` (`src/http.jl`). Leaf
  endpoints (`timeseries`, `metadata`, `batch`, `symbology`) are thin functions
  that build query params, call `get_json`/`get_bytes`/`open_stream`, and decode.
  `get_range` buffers the whole zstd payload then decodes; `foreach_record`
  streams (overlaps download with decode, never materializes the payload).
- **`Live`** (`src/live/`) is a `mutable struct` driving the TCP session through
  an explicit lifecycle: `connect!` → `subscribe!` (≥1) → `start!` → consume →
  `close`. A single `Live` is bound to **one dataset**.

### Cross-cutting conventions

- **Enum ↔ wire strings**: `schema_str`/`stype_str`/`compression_str` (in
  `src/historical/client.jl`) map DBN enums to Databento's wire spelling
  (`Schema.MBP_1 → "mbp-1"`, `SType.RAW_SYMBOL → "raw_symbol"`). Many kwargs also
  accept lowercase `Symbol`/`String` spellings, coerced via `getfield(EnumMod, ...)`.
- **API key resolution** (`src/auth.jl`, `load_api_key`): explicit arg →
  `~/.databento/config.toml` `[auth] api_key` → `DATABENTO_API_KEY`. Config path
  overridable via `DATABENTO_CONFIG_PATH` (used by tests).
- **Errors** (`src/errors.jl`): HTTP 4xx → `BentoClientError`, 5xx →
  `BentoServerError`, auth → `BentoAuthError`, all under `BentoError`.
- **Typed decode path**: `record_type_for_schema(schema)` returns the concrete
  record struct for type-pure schemas (all but `Schema.MIX`). The typed path
  (`DBNStore{T}` with a `Vector{T}`) is ~10× faster / near-zero-alloc vs. the
  generic `Vector{DBN.DBNRecord}` Union path. `typed=true` is the default for
  `get_range`; `Schema.MIX` falls back to the Union.

### Live: typed vs untyped mode (frozen at construction)

`Live(...; typed=false)` (default) uses one `Channel{DBN.DBNRecord}` — all data
*and* control records arrive on `client.channel`; consume via `for rec in client`
or `subscribe_callback`. `Live(...; typed=true)` gives each subscribed schema its
own `Channel{ConcreteT}` (returned by `subscribe!`, or via `channel(client,
schema)`), with `ErrorMsg`/`SystemMsg`/`SymbolMappingMsg` routed to a separate
`control_channel(client)`. Iterating `for rec in client` and `subscribe_callback`
**error** in typed mode — the hot path is type-stable per-channel `put!`s, so the
typed reader (`_reader_loop_typed`) is a verbose flat `if/elseif` tree, one branch
per rtype, rather than dynamic dispatch.

### Live: TCP read stack

`_reader_loop[_typed]` (`src/live/reader.jl`) is the `@async` task draining the
socket. The IO stack, bottom-up: `TCPSocket` → optional `ZstdDecompressor`
TranscodingStream (when `compression=zstd`) → `CountingIO` (wraps any IO to
implement `position`, which `TCPSocket` lacks but `DBN.BufferedReader` needs) →
`DBN.BufferedReader` → `DBN.DBNDecoder`. Mid-stream `EOFError` is treated as a
clean end-of-stream (not a crash) — relevant mostly to tests/replay, since the
real gateway streams continuously.

### Live: reconnect supervisor (the central design point)

Reconnect lives in the **Live client itself** (`src/live/reconnect.jl`), so every
consumer — iteration, `subscribe_callback`, `stream_to_file` — benefits from one
codepath. `Live`'s default `reconnect_policy = RECONNECT`. Mechanics:

- `start!` spawns `_supervisor_loop` (an `@async` task) when policy ≠ `NONE`. The
  supervisor `wait`s on the reader task; when it dies it decides reconnect vs.
  terminate based on a `state` machine (`:fresh → :connecting → :streaming →
  :reconnecting → :closed|:failed`) guarded by `state_lock`.
- **Channel ownership differs by policy**: under `NONE`, `start!` `bind`s channels
  to the reader so they close when it exits (correct shutdown signal, no
  replacement coming). Under `RECONNECT`, channels are owned by the `Live` and
  **outlive** each reader incarnation, so the supervisor can spawn a fresh reader
  writing into the same channels without terminating consumers. The reader's
  `finally` only closes channels under `NONE` or on user `close()`.
- **Retry schedule** (`_reconnect_delay`): the first `immediate_reconnect_attempts`
  (default 3) retries fire with no sleep (catch sub-second blips), then full-jitter
  exponential backoff (1s base, 60s cap), up to `max_reconnect_attempts` (default
  10; `nothing` = unlimited). The budget **refreshes** once a new reader delivers
  ≥1 record (`_record_replay!`), so a long-lived session keeps its full budget
  across the connection's lifetime.
- **Replay on reconnect**: the reader records per-instrument last timestamps
  (`last_ts_event_by_id` / `last_ts_recv_by_id`, BBO-family schemas key on
  `ts_recv`). On reconnect the supervisor resubscribes each schema with
  `start=` the min-across-instruments timestamp (bounded to Databento's 24h replay
  window), bridging the gap. `add_reconnect_callback(client, cb)` fires
  `cb(gap_start_ns, gap_end_ns)` per successful reconnect.
- **Terminal errors**: a gateway `ErrorMsg` sets `c.terminal_error` *before* the
  record is published, so the supervisor refuses to reconnect (gateway-side errors
  are deterministic). Auth failures during reconnect are likewise terminal.

### Live: multi-schema capture to file

`stream_to_file` / `stream_multi_to_files` (`src/live/streaming.jl`) capture one or
more schemas to compressed DBN files using a **single** typed `Live`: one TCP
connection, one typed channel + one drainer task + one `RotatingDBNFile` per
schema, plus a shared control drainer (the gateway dedupes `SymbolMappingMsg`
within a connection). `_run_unified_session` is the coordinator; reconnect is
handled by the Live supervisor (the streaming layer no longer owns its own loop).

**Durability** (`RotatingDBNFile`): in addition to optional time-based file
rotation (`rotate_seconds`), `frame_seconds` (default 60s) periodically closes the
active zstd frame and starts a new one on the same file — a standards-compliant
multi-frame `.dbn.zst` that reads back as one stream, so a hard kill loses ≤ one
frame instead of corrupting the whole file. A per-schema monitor task also flushes
quiet schemas (whose buffers, including the DBN header, would otherwise never reach
disk). `open_dbn_writer` + `write_record!` expose this writer publicly for custom
subscribe-and-iterate loops.

**Head-of-line caveat**: the reader `put!`s to each schema's channel in turn; if
one consumer's disk write falls behind and fills its `channel_size` buffer (default
10_000), the reader stalls the TCP socket for *all* schemas.

## Conventions for changes

- Match the existing heavy-comment style in `src/live/` — the *why* (race
  conditions, ordering constraints, policy rationale) is documented inline and is
  load-bearing; preserve it when editing.
- Public API changes mean editing the `export` list in `src/DatabentoAPI.jl` and
  adding to the matching `test/test_*.jl` (offline, mock-driven) — add a `live/`
  smoke test too if it touches the real gateway path.
- Update `CHANGELOG.md` (Keep-a-Changelog format) under `## [Unreleased]`.
- Requires Julia ≥ 1.12. CI matrix: Julia `1.12` and `1` × {ubuntu, macOS, windows}.
```
