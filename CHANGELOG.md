# Changelog

All notable changes to DatabentoAPI.jl are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Documenter.jl documentation site at https://tbeason.github.io/DatabentoAPI.jl.
- This `CHANGELOG.md`.
- Docstrings for every exported function/type (`to_csv`/`to_json`/`to_parquet`,
  the `BentoError` hierarchy, the historical metadata/batch/symbology
  endpoints, and every DatabentoAPI-specific enum).

### Changed
- `README.md` slimmed and reorganised; long examples migrated into the
  Documenter guides.

## [0.1.2] - 2026-05-26

### Added
- `Live(f, args...; kwargs...) do client … end` do-block constructor (mirrors
  `Base.open`) — guarantees `close(client)` on `Ctrl-C`, exceptions, or
  normal exit. The manual `connect! → subscribe! → start! → close` lifecycle
  still works unchanged.
- `open_dbn_writer(f; kwargs...) do writer … end` and `write_record!(writer,
  rec)` — public do-block file writer for custom subscribe-and-iterate
  loops, with the same crash-safety features `stream_to_file` uses
  internally.
- In-file zstd frame rotation in `RotatingDBNFile`, controlled by the new
  `frame_seconds` kwarg on `stream_to_file` / `stream_multi_to_files` (default
  `60.0`). Multi-frame `.dbn.zst` is standards-compliant; a hard kill loses ≤
  one frame of records instead of corrupting the whole file. Pass `nothing`
  to write a single frame.
- Full-jitter exponential backoff on reconnect (1s base, 60s cap) with a
  configurable `max_reconnect_attempts` (default 10, `nothing` = unlimited).

### Changed
- `SymbolMappingMsg` records are now written to the `.dbn.zst` capture file
  via `_write_record!`. Previously they were dropped on the floor due to a
  v1→v3 encoder layout bug; that bug is fixed upstream in
  [DatabentoBinaryEncoding 0.1.1](https://github.com/tbeason/DatabentoBinaryEncoding.jl/pull/21)
  and compat has been raised to `"0.1.1"`.
- `Base.close(::Live)` hardened: flag-flip happens up-front so re-entry is a
  no-op; channel cleanup runs unconditionally so a single bad close doesn't
  strand the others.
- `stream_to_file` internally uses `open_dbn_writer` (dogfooding the new API).

### Fixed
- Backoff sleep is now interruptible by the deadline / shutdown signal at
  0.5s granularity, so `duration_s` is honoured even if the deadline is
  crossed mid-backoff.

## [0.1.1] - 2026-05-22

### Changed
- **Renamed dependency:** `DBN.jl` → `DatabentoBinaryEncoding.jl` (#13).
  Adopted the new package name; updated `[compat]` and all `import DBN`
  sites. Re-exports of `Schema`, `SType`, `Compression`, `Encoding`,
  `Action`, `Side`, `InstrumentClass` keep user code source-compatible.

### Added
- Unified-Live multi-schema streaming (#12): one TCP connection serves N
  schemas via per-schema typed channels, routed to per-schema files by
  `stream_multi_to_files`. The gateway dedupes `SymbolMappingMsg` within a
  connection, halving the mapping-record count on multi-schema captures.

### Fixed
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

[Unreleased]: https://github.com/tbeason/DatabentoAPI.jl/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/tbeason/DatabentoAPI.jl/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/tbeason/DatabentoAPI.jl/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/tbeason/DatabentoAPI.jl/releases/tag/v0.1.0
