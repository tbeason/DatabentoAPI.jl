# Performance

Headline numbers from the offline benchmark suite at `benchmark/`. Run
locally with:

```powershell
julia --project=benchmark benchmark/runbench.jl
```

The full report (`benchmark/PERF_REPORT.md`) covers methodology, hardware,
record-by-record allocation profiles, and a flat profile per hot path.

## Read throughput

| Path                                                       | Records/sec  | Allocation         | Notes                                  |
|------------------------------------------------------------|--------------|--------------------|----------------------------------------|
| `foreach_record(file, T) do …`                             | **45 M/sec** | 0.13 bytes/record  | Typed callback. Allocation-free hot path. |
| `read_dbn` with schema-aware fast path                     | 30 M/sec     | low                | Eager into a typed `Vector{T}`.        |
| `get_range` (`Historical` HTTP, decoded into a `DBNStore`) | ~5 M/sec     | moderate           | Includes network + HTTP framing.       |

For million-record-plus queries, prefer [`foreach_record`](@ref) over
[`get_range`](@ref) — it streams record-by-record with effectively no GC
pressure.

## Live throughput

In-process loopback measurements against a mock TCP gateway:

| Wire format    | Records/sec | Notes                                |
|----------------|-------------|--------------------------------------|
| Plain DBN      | 8.87 M/sec  | No zstd; line-rate bound.            |
| zstd-compressed| 6.16 M/sec  | Decompression on the read side.      |

These are upper bounds. Real network feeds are typically far slower (the
gateway throttles to wire rate) and the bottleneck shifts to the gateway, not
the client.

## Capture: compression level

`stream_to_file` and `stream_multi_to_files` default to `compress_level = 1`.
Empirically L1 is ~35% faster than L3 on the write boundary in exchange for
~15% larger files. For archival captures where storage cost dominates,
bump to L3-L9; for low-latency / high-throughput captures keep the default.

## Typed channels in Live

`Live(...; typed = true)` returns concrete-typed channels (`Channel{T}` per
schema) from [`subscribe!`](@ref). For the OPRA CMBP-1 replay benchmark this
delivers **~25% higher per-cycle throughput** over untyped mode. Allocation is
roughly equal — Julia's stdlib `Channel.put!`/`take!` allocates Condition
bookkeeping regardless of element type — so the win is in dispatch, not GC.

## When to use what

- **Historical, small query (≤ 1 GB):** [`get_range`](@ref) → [`DBNStore`](@ref)
  → conversion helpers. Easy and ergonomic.
- **Historical, large query:** [`foreach_record`](@ref) with a typed callback,
  or [`submit_job`](@ref) + [`batch_download`](@ref) and decode the resulting
  files with `DBN.foreach_record` directly.
- **Live, single consumer:** [`Live`](@ref) do-block + untyped iterator. Easy
  to write, no setup.
- **Live, multi-schema high throughput:** typed mode (`typed = true`), one
  `@async` per schema reading the typed channel.
- **Live capture to disk:** [`stream_to_file`](@ref) or
  [`stream_multi_to_files`](@ref). For custom loops, [`open_dbn_writer`](@ref)
  + [`write_record!`](@ref).
