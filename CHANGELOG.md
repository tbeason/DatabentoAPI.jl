# Changelog

All notable changes to DatabentoAPI.jl are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **`BentoTimeoutError`.** HTTP read timeouts are now mapped to a `BentoError`
  subtype whose message names the remedy (raise `Historical(timeout = ...)` or
  reduce the range) instead of surfacing HTTP.jl's bare
  `TimeoutError: Connection closed after N seconds` (#31).

### Changed
- **Default HTTP read timeout raised from 100s to 600s.** Long-range
  `get_range` queries (e.g. multi-year continuous-symbol pulls) can spend well
  over 100s in server-side assembly before the first byte streams, so every
  such request used to die on the client's own timeout (#31). Note the
  timeout is inactivity-based, so a genuinely hung connection now takes up to
  10 minutes to surface.
- **Read timeouts are no longer retried.** Server-side assembly time is
  deterministic for a given query shape, so retrying a timed-out request burns
  the same timeout again with zero success probability. Connect errors and
  transient statuses (429/5xx) are retried as before (#31).

### Fixed
- **Typed `get_range`/`foreach_record` no longer throw on interleaved
  non-schema records** (#30). Historical responses can legitimately carry
  gateway `ErrorMsg` (e.g. partial continuous-symbol resolution), `SystemMsg`,
  and `SymbolMappingMsg` records among the data; the typed decode path used to
  die on the first one (`Expected ... but got rtype=ERROR_MSG`), forcing
  `typed=false`. Now `ErrorMsg` records are surfaced via `@warn` (they explain
  why data is missing), `SystemMsg`/`SymbolMappingMsg` are skipped quietly,
  and any other mismatched or unknown record types are skipped with a single
  summary warning. Behavior note: an explicitly wrong `record_type` override
  on `foreach_record` now yields zero callbacks plus the summary warning
  instead of throwing.
- **Docs: corrected the `foreach_record` example in the Historical guide.** It
  showed a nonexistent positional record-type method
  (`foreach_record(client, DBN.TradeMsg; ...)`), which raises a `MethodError`.
  The record type is inferred from `schema` and overridable via the
  `record_type` keyword (#32).

## [0.2.0] - 2026-06-04

### Added
- **Automatic HTTP retries on the Historical API.** Transient failures — HTTP
  `429` (rate limit) and `5xx`, plus connection/timeout errors — are now retried
  with full-jitter exponential backoff. A `Retry-After` response header, when
  present, overrides the computed backoff (capped to guard against hostile
  values). Configurable via the new `max_retries` keyword on `Historical(...)`
  (default 3); the retry budget, backoff, and sleep hook are all injectable for
  testing. Once the budget is exhausted the final status is mapped to its
  `BentoClientError`/`BentoServerError` as before.

### Changed
- **Breaking: renamed the time-range keyword arguments to `start_dt` / `end_dt`.**
  The old `start` / `end_` pair was asymmetric (the trailing underscore only
  existed because `end` is a Julia reserved word). All affected entry points are
  updated: `get_range`, `foreach_record`, `submit_job`, `get_record_count`,
  `get_billable_size`, `get_cost`, and the Live `subscribe!` / `live_session`
  subscription / `stream_to_file` / `stream_multi_to_files` `start` keyword
  (now `start_dt`). The Databento wire parameters (`start` / `end`) are
  unchanged. Update call sites from `start = …, end_ = …` to
  `start_dt = …, end_dt = …`.
- **`foreach_record` now applies real backpressure.** The streaming download
  previously drained the connection into an unbounded `Base.BufferStream` on a
  background task, which could buffer the entire compressed payload in memory and
  left the producer task running (blocking until server EOF/timeout) when the
  consumer aborted or finished early. It now reads the response body synchronously
  on the calling task, so records are pulled from the socket only as fast as the
  consumer processes them, and the connection is torn down deterministically on
  every exit path — including an early return or an exception out of the callback.

## [0.1.2] - 2026-06-01

### Added
- **Live-layer reconnect supervisor.** Reconnect handling moves into the
  `Live` client itself, so any consumer — `for rec in client`,
  `subscribe_callback`, or `stream_to_file` — benefits. Previously
  reconnect only existed inside the streaming layer; iteration consumers
  silently saw an `InvalidStateException` on a TCP drop. There is now
  one reconnect codepath: `_run_unified_session` constructs a single
  Live with `reconnect_policy = ReconnectPolicy.RECONNECT`, the
  supervisor handles drops, and the streaming layer no longer carries
  its own outer reconnect loop.
- **Hybrid immediate-then-backoff retry schedule.** Under
  `reconnect_policy = :reconnect`, the first
  `immediate_reconnect_attempts` retries (default 3) fire with no sleep
  to catch sub-second TCP blips, then fall back to PR #16's full-jitter
  exponential backoff (1s base, 60s cap). The retry budget refreshes
  whenever the new reader successfully delivers ≥1 data record, so a
  long-lived session that streams real data between drops keeps its
  full `max_reconnect_attempts` budget across the connection lifetime.
- **`add_reconnect_callback(client, cb)`.** Register
  `cb(gap_start_ns::Int64, gap_end_ns::Int64)` to observe each successful
  reconnect. `gap_start_ns` is the min per-instrument timestamp seen
  pre-drop; `gap_end_ns` is the gateway's `Metadata.start_ts` from the
  freshly reconnected session. Useful for gap-fill from historical,
  alerting, or metric emission. Callbacks are lock-protected and
  errors are logged-and-swallowed.
- **`live_session(fn; dataset, subscriptions, kwargs...)`.** Convenience
  do-block bundling `connect! → subscribe!(many) → start! → fn(client)
  → close`. Defaults `reconnect_policy = :reconnect` so the simple-API
  path gets the supervisor by default.
- **`ReconnectPolicy.{NONE, RECONNECT}` enum is now load-bearing.**
  Previously exposed for parity but unused by code; now drives whether
  `start!` spawns the supervisor task.
- Gateway `ErrorMsg` records in typed mode now set
  `client.terminal_error` *before* being forwarded to `control_channel`,
  so the supervisor refuses to reconnect (gateway-side errors are
  deterministic — retrying would just re-hit the same condition).

### Changed
- **`Live(...)` default `reconnect_policy` is now `RECONNECT`.** Bare
  `Live(...)` clients automatically reconnect on TCP drops. Pass
  `reconnect_policy = :none` for the previous single-shot behaviour
  (the reader's exit closes the channels and terminates iteration).
- `_run_unified_session` no longer owns a reconnect loop — it leans on
  the supervisor. `stream_to_file` / `stream_multi_to_files` users now
  get the same immediate-then-backoff schedule as iteration consumers
  (previously their first retry waited at least `_RECONNECT_BASE_S = 1s`).
- `SessionStats.last_ts_event_by_id` / `last_ts_recv_by_id` removed —
  replay bookkeeping lives entirely on `Live` (populated by the reader,
  consumed by the supervisor). `_replay_start_ts` now only has the
  `::Live` overload; the `::SessionStats` overload is gone.
- `_reconnect_delay(attempt; immediate=0, base, cap)` gains the
  `immediate` kwarg. Default `0` preserves the v0.1.1 backoff curve
  exactly; the Live supervisor passes the user's
  `immediate_reconnect_attempts`.
- Under `reconnect_policy = :reconnect`, `start!` does NOT bind the
  user-facing channels to the reader task. The channels are owned by
  the Live client and outlive any single reader incarnation so the
  supervisor can respawn a reader writing into the same channels.
  Channels are still closed explicitly by `Base.close(client)` and on
  supervisor terminal-state transitions (`:failed` / `:closed`), so
  iteration consumers still terminate cleanly on user shutdown.
- **Require `DatabentoBinaryEncoding` ≥ 0.1.2.** That release tolerates the
  unset (`0xFF`) `stype` sentinel in v3 `SymbolMappingMsg` control records
  (DatabentoBinaryEncoding.jl#23/#24). Before this, decoding a real v3 live
  capture through the untyped/generic reader crashed with
  `invalid value for Enum SType: 255` on the first symbol-mapping record —
  so live OPRA streams and v3 capture replay were effectively undecodable.
  Verified against a 12.6 GB `OPRA.PILLAR` cbbo-1s v3 capture: 5,000,000
  records (incl. 73,003 `SymbolMappingMsg`) decode cleanly.

## [0.1.1] - 2026-05-26

First release after v0.1.0. Bundles all work since the initial registry tag —
the dependency rename, the unified-Live multi-schema streaming refactor, live
capture durability features, and the Documenter.jl site.

### Added
- **Live do-block constructor.** `Live(f, args...; kwargs...) do client … end`
  mirrors `Base.open` — guarantees `close(client)` on `Ctrl-C`, exceptions, or
  normal exit. The manual `connect! → subscribe! → start! → close` lifecycle
  still works unchanged.
- **`open_dbn_writer` + `write_record!`** — public do-block file writer for
  custom subscribe-and-iterate loops, with the same crash-safety features
  `stream_to_file` uses internally.
- **In-file zstd frame rotation** in `RotatingDBNFile`, controlled by the new
  `frame_seconds` kwarg on `stream_to_file` / `stream_multi_to_files` (default
  `60.0`). Multi-frame `.dbn.zst` is standards-compliant; a hard kill loses ≤
  one frame of records instead of corrupting the whole file. Pass `nothing`
  to write a single frame.
- **Full-jitter exponential backoff on reconnect** (1s base, 60s cap) with a
  configurable `max_reconnect_attempts` (default 10, `nothing` = unlimited).
- **Unified-Live multi-schema streaming** (#12): one TCP connection serves N
  schemas via per-schema typed channels, routed to per-schema files by
  `stream_multi_to_files`. The gateway dedupes `SymbolMappingMsg` within a
  connection, halving the mapping-record count on multi-schema captures.
- **Documenter.jl documentation site** at
  https://tbeason.github.io/DatabentoAPI.jl — home, install, quick start,
  five user guides (Authentication, Historical, Live, Capture, Conversion),
  per-topic API reference, performance, and troubleshooting pages.
- `CHANGELOG.md` (this file).
- Docstrings for every exported function/type — `to_csv`/`to_json`/`to_parquet`,
  the `BentoError` hierarchy, the historical metadata/batch/symbology
  endpoints, and every DatabentoAPI-specific enum.

### Changed
- **Renamed dependency:** `DBN.jl` → `DatabentoBinaryEncoding.jl` (#13).
  Adopted the new package name; updated `[compat]` and all `import DBN`
  sites. Re-exports of `Schema`, `SType`, `Compression`, `Encoding`,
  `Action`, `Side`, `InstrumentClass` keep user code source-compatible.
  Compat bumped to `DatabentoBinaryEncoding = "0.1.1"` (required for the
  `SymbolMappingMsg` write path below).
- **`SymbolMappingMsg` records are now written to the `.dbn.zst` capture
  file** via `_write_record!`. Previously they were dropped on the floor
  due to a v1→v3 encoder layout bug; that bug is fixed upstream in
  [DatabentoBinaryEncoding 0.1.1](https://github.com/tbeason/DatabentoBinaryEncoding.jl/pull/21).
- **`Base.close(::Live)` hardened:** flag-flip happens up-front so re-entry
  is a no-op; channel cleanup runs unconditionally so a single bad close
  doesn't strand the others.
- `stream_to_file` internally uses `open_dbn_writer` (dogfooding the new API).
- `README.md` slimmed and reorganised; long examples migrated into the
  Documenter guides.

### Fixed
- Backoff sleep is now interruptible by the deadline / shutdown signal at
  0.5s granularity, so `duration_s` is honoured even if the deadline is
  crossed mid-backoff.
- `stream_to_file` no longer hangs when a mock TCP gateway closes its half
  of the socket mid-stream — the reader now treats mid-stream `EOFError` as
  a clean end-of-stream (#11).
- Typed channel routing now keys on schema, not record type, so subscribing
  to multiple schemas with the same concrete record type works (#8).

## [0.1.0] - 2026-05-18

Initial public release. Registered in the General registry.

### Added
- `Historical` client wrapping the Databento HTTP API:
  - Bulk fetch via `get_range` → `DBNStore`.
  - Streaming fetch via `foreach_record` with typed callbacks.
  - Metadata endpoints: `list_publishers`, `list_datasets`, `list_schemas`,
    `list_fields`, `list_unit_prices`, `get_dataset_range`,
    `get_dataset_condition`, `get_record_count`, `get_billable_size`,
    `get_cost`.
  - Batch jobs: `submit_job`, `list_jobs`, `list_files`, `batch_download`.
  - Symbology: `resolve`.
- `Live` client wrapping the Databento TCP API:
  - CRAM-MD5 handshake (`connect!`), subscribe (`subscribe!`), start
    (`start!`), stop (`stop!`), close (`close`).
  - Untyped iterator (`for rec in client`) and callback variant
    (`subscribe_callback`).
  - Typed mode (`typed = true`) with per-schema concrete-typed channels
    and a control channel (`control_channel`) for `ErrorMsg`/`SystemMsg`/
    `SymbolMappingMsg` (#7).
- `stream_to_file` and `stream_multi_to_files` — high-level capture-to-disk
  helpers built on the Live client.
- `DBNStore` + conversion helpers `to_dataframe`, `to_csv`, `to_json`,
  `to_parquet`, `to_file`, `read_capture`.
- Authentication: `load_api_key`, `default_config_path`. Resolution order
  is kwarg → `~/.databento/config.toml` → `DATABENTO_API_KEY`.
- Error hierarchy: `BentoError`, `BentoAuthError`, `BentoHttpError`,
  `BentoClientError`, `BentoServerError`.
- Enums for the wire protocol and HTTP API: `HistoricalGateway`, `FeedMode`,
  `ReconnectPolicy`, `JobState`, `SplitDuration`, `Packaging`, `Delivery`,
  `SymbologyResolution`, `RollRule`, `SlowReaderBehavior`.

[Unreleased]: https://github.com/tbeason/DatabentoAPI.jl/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/tbeason/DatabentoAPI.jl/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/tbeason/DatabentoAPI.jl/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/tbeason/DatabentoAPI.jl/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/tbeason/DatabentoAPI.jl/releases/tag/v0.1.0
