# Performance pressure-test report — DatabentoAPI.jl + DBN.jl

**Date:** 2026-05-17 (baseline) / 2026-05-17 (implementation pass)
**Hardware:** Windows 11 Home (10.0.26200), x86_64
**Julia:** 1.13.x
**Method:** `julia --project=benchmark benchmark/runbench.jl`. All measurements offline against bundled DBN.jl fixtures (`trades.100k.dbn` and `trades.1m.dbn`) or in-memory synthesized payloads.

Each row is the minimum of N BenchmarkTools samples (`samples=5` for read, `samples=3` for write, `samples=3` for live). Allocations and GC fraction come from the minimum trial via `Base.gc_num()`. Raw CSVs are under `benchmark/results/`; per-path flat profiles under `benchmark/results/*.profile.txt`.

> The bundled DBN.jl "small" fixture has 100 000 records (not 10 k as labelled); throughput numbers use the actual on-disk count.

---

## Implementation status (post-pass)

| Rec | Status | Headline impact |
|---|---|---|
| R1 — typed `get_range` (`DBNStore{T}`)            | **DONE** | `get_range` at 1 M records: **307 ms → 238 ms (-23%)**, alloc 240 MB → 266 MB (over-sized hint), GC 3% → 0.1%. At 100 k: 27 ms → 20 ms (-26%), 5.4 M rec/s. |
| R2 — schema-aware typed dispatch in `DBN.read_dbn` | **DONE** | `read_dbn` at 1 M records: **300 ms → 34 ms (-89%)**, GC 84.2% → 0%, **3.3 → 29.8 M rec/s (+9×)**. At 100 k: 212 ms → 3.4 ms (-98%), GC 97.6% → 0%, **0.47 → 29.7 M rec/s (+63×)**. |
| R3 — live reader investigation                    | **DONE (no fix needed)** | The "50 k rec/s ceiling" was a **benchmark bug**, not a real performance issue. Root cause: the mock TCP server slept 2 s after writing the payload before closing the socket; the reader's final `readbytes!(sock, buf, 64 KB)` call blocks for that full 2 s waiting for EOF (it asked for more bytes than the stream contained). Fixing the mock to close immediately yields **8.87 M rec/s plain / 6.16 M rec/s zstd** — a 177× improvement and within range of the in-process pipeline. The live reader code is fine as-is. |
| R4 — eliminate Ref boxing in `DBN.read_record`    | **DEFERRED** | Out of scope this pass; the most impactful path (Vector-materialising `read_dbn`) is now covered by R2's typed dispatch, so the remaining audience for R4 is narrower. Separate follow-up. |
| R5 — `stream_to_file` default `compress_level = 1` | **DONE** | Default changed in `src/live/streaming.jl`. Bench-derived rationale (L1 ≈ 35% faster than L3 at write boundary) captured in the docstring. New explicit-override test added. |
| R6 — document `DBNStream` per-record alloc cost   | **DONE** | "Performance" note added to `DBN.jl/src/streaming.jl` `DBNStream` docstring + README pointer to `foreach_record`. |
| R7 — bench-internal note                          | **DROPPED** | Not a code change. |

**Test deltas:** DatabentoAPI.jl 188 → 200 tests (+12 covering R1 type assertions, R1 typed=false escape hatch, R5 explicit override, F4 size_hint). DBN.jl test suite: same pass count; pre-existing 10 errors in `test_phase10_complete.jl` (missing `using DataFrames` for `nrow`/`ncol`) are unrelated to this pass.

**Files changed (this pass):**
- DatabentoAPI.jl: `src/store.jl` (DBNStore{T}, typed decode overloads); `src/historical/timeseries.jl` (typed `get_range` + `typed::Bool` + `size_hint` kwargs); `src/live/streaming.jl` (compress_level default = 1); `test/test_historical_timeseries.jl` (typed-vector assertion + escape-hatch + size_hint tests); `test/test_live_streaming.jl` (compress_level override test); `README.md` (foreach_record perf pointer); `benchmark/profile_live_reader.jl` (NEW, R3 investigation); `benchmark/bench_tcp_throughput.jl` (NEW, R3 root-cause diagnostic); `benchmark/bench_live_reader.jl` + `benchmark/bench_stream_to_file.jl` (fix mock-sleep bug that caused the false R3 ceiling).
- DBN.jl: `src/streaming.jl` (record_type_for_dbn_schema helper + DBNStream perf docstring); `src/decode.jl` (schema-aware fast path in `read_dbn` and `read_dbn_with_metadata`, with rtype-mismatch fallback for mixed files).

---

## TL;DR

1. **Generic eager `DBN.read_dbn` spends 80–97% of wall time in GC.** The `Vector{Union{18 types}}` push-loop is catastrophic. Typed eager (`read_dbn_typed`) is **9× faster** at 1 M records and **65× faster** at 100 k records. This is the single highest-impact finding.
2. **`DatabentoAPI.get_range` inherits this cost** — it materialises 240 MB of Union-typed records for a 1 M-record query. A typed-vector variant inside `get_range` would cut alloc by ~60% and wall time by 30–80% depending on size.
3. **`foreach_record` typed is the gold standard.** 45 M rec/s uncompressed, 18 M rec/s zstd, **0.13 bytes/record** allocation total. Already the recommended fast path.
4. **The live reader runs at 8.87 M rec/s plain / 6.16 M rec/s zstd through TCP loopback** — comparable to file decode. The earlier "50 k rec/s ceiling" reported during the pressure-test pass was a benchmark artefact (mock TCP server held the connection open for 2 s after writing, so the reader's last `readbytes!` call blocked waiting for EOF). Fix landed; numbers above are post-fix.
5. **Write throughput is healthy and not a priority.** 20 M rec/s uncompressed, 7 M rec/s at zstd L3. L9 collapses to 1.9 M rec/s and offers no payoff for streaming-capture workloads.

---

## Baseline tables

### DBN.jl read paths (file decode) — post-R2

| Path | Size | n rec | Time | Alloc | GC% | M rec/s | MB/s |
|---|---|---|---|---|---|---|---|
| `read_dbn` (post-R2) | 100 k plain | 100 000 |   3.4 ms | 4.8 MB | 0% | **29.7** | 1358.5 |
| `read_dbn_typed`     | 100 k plain | 100 000 |   3.2 ms | 4.7 MB | 0% | 30.8 | 1411.5 |
| `foreach_record` typed | 100 k plain | 100 000 |   2.0 ms | 0.13 MB | 0% | **49.5** | **2267.3** |
| `DBNStream`          | 100 k plain | 100 000 |  10.1 ms | 12.3 MB | 17% | 9.9 | 454.5 |
| `read_dbn` (post-R2) | 100 k zst | 100 000 |   7.2 ms | 5.6 MB | 0% | 13.8 | 217.1 |
| `read_dbn_typed`     | 100 k zst | 100 000 |   8.0 ms | 5.4 MB | 0% | 12.5 | 196.2 |
| `foreach_record` typed | 100 k zst | 100 000 |   7.2 ms | 0.84 MB | 0% | 13.9 | 218.5 |
| `DBNStream`          | 100 k zst | 100 000 |  16.3 ms | 13.1 MB | 0% | 6.1 | 96.2 |
| `read_dbn` (post-R2) | 1 M plain  | 1 000 000 |  33.6 ms | 46 MB | 0% | **29.8** | 1362.0 |
| `read_dbn_typed`     | 1 M plain  | 1 000 000 |  34.1 ms | 46 MB | 0% | 29.3 | 1340.7 |
| `foreach_record` typed | 1 M plain | 1 000 000 |  25.5 ms | 0.13 MB | 0% | **39.2** | **1793.3** |
| `DBNStream`          | 1 M plain  | 1 000 000 |  97.5 ms | 122 MB | 6.8% | 10.3 | 469.6 |
| `read_dbn` (post-R2) | 1 M zst    | 1 000 000 |  69.8 ms | 47 MB | 0% | 14.3 | 225.2 |
| `read_dbn_typed`     | 1 M zst    | 1 000 000 |  70.5 ms | 47 MB | 0% | 14.2 | 223.3 |
| `foreach_record` typed | 1 M zst  | 1 000 000 |  52.6 ms | 0.84 MB | 0% | 19.0 | 298.8 |
| `DBNStream`          | 1 M zst    | 1 000 000 | 205.7 ms | 123 MB | 3.0% | 4.9 | 76.5 |

#### R2 delta — generic `read_dbn` before vs after

| Size | Before | After | Δ wall | Δ rec/s |
|---|---|---|---|---|
| 100 k plain | 212 ms / 97.6% GC / 0.47 M rec/s | 3.4 ms / 0% GC / 29.7 M rec/s | **−98%** | **+63×** |
| 100 k zst   | 220 ms / 94.5% GC / 0.45 M rec/s | 7.2 ms / 0% GC / 13.8 M rec/s | **−97%** | **+30×** |
| 1 M plain   | 300 ms / 84.2% GC / 3.3 M rec/s  | 33.6 ms / 0% GC / 29.8 M rec/s | **−89%** | **+9×** |
| 1 M zst     | 369 ms / 68.1% GC / 2.7 M rec/s  | 69.8 ms / 0% GC / 14.3 M rec/s | **−81%** | **+5.3×** |

### DBN.jl write paths

| Path | Size | n rec | Time | Alloc | M rec/s | MB/s |
|---|---|---|---|---|---|---|
| `write_dbn`              | 10 k  |  10 000 |  0.90 ms | 0.6 MB | 11.1 | 423.1 |
| `write_dbn` (.zst)       | 10 k  |  10 000 |  2.05 ms | 0.7 MB |  4.9 | 186.2 |
| `DBNStreamWriter`        | 10 k  |  10 000 |  1.00 ms | 0.6 MB | 10.0 | 382.7 |
| `DBNEncoder` raw         | 10 k  |  10 000 |  0.91 ms | 0.6 MB | 11.0 | 419.1 |
| `DBNEncoder` + zstd L1   | 10 k  |  10 000 |  1.82 ms | 0.7 MB |  5.5 | 209.7 |
| `DBNEncoder` + zstd L3   | 10 k  |  10 000 |  2.52 ms | 0.7 MB |  4.0 | 151.3 |
| `DBNEncoder` + zstd L9   | 10 k  |  10 000 |  8.44 ms | 0.7 MB |  1.2 |  45.2 |
| `write_dbn`              | 100 k | 100 000 |  5.11 ms | 6.1 MB | **19.6** | 746.4 |
| `write_dbn` (.zst)       | 100 k | 100 000 | 15.34 ms | 6.4 MB |  6.5 | 248.7 |
| `DBNStreamWriter`        | 100 k | 100 000 |  5.96 ms | 6.1 MB | 16.8 | 639.9 |
| `DBNEncoder` raw         | 100 k | 100 000 |  5.25 ms | 6.1 MB | 19.0 | 726.4 |
| `DBNEncoder` + zstd L1   | 100 k | 100 000 | 11.67 ms | 6.4 MB |  8.6 | 326.8 |
| `DBNEncoder` + zstd L3   | 100 k | 100 000 | 14.37 ms | 6.4 MB |  7.0 | 265.5 |
| `DBNEncoder` + zstd L9   | 100 k | 100 000 | 52.31 ms | 6.4 MB |  1.9 |  72.9 |

### DatabentoAPI historical decode (HTTP mocked) — post-R1

| Path | Size | n rec | Time | Alloc | M rec/s | MB/s |
|---|---|---|---|---|---|---|
| `get_range` (post-R1, typed)  | 100 k zst | 100 000 |  19.5 ms |  29 MB | **5.13** | 80.6 |
| post-HTTP decode typed        | 100 k zst | 100 000 |  20.0 ms |  16 MB | 5.00 | 78.6 |
| post-HTTP decode generic      | 100 k zst | 100 000 |  26.4 ms |  15 MB | 3.79 | 59.5 |
| `get_range` (post-R1, typed)  | 1 M zst   | 1 000 000 | 237.7 ms | 266 MB | **4.21** | 66.2 |
| post-HTTP decode typed        | 1 M zst   | 1 000 000 | 209.8 ms | 140 MB | 4.81 | 75.7 |
| post-HTTP decode generic      | 1 M zst   | 1 000 000 | 308.6 ms | 124 MB | 3.24 | 51.0 |

#### R1 delta — `get_range` before vs after

| Size | Before | After | Δ wall | Δ rec/s | Alloc note |
|---|---|---|---|---|---|
| 100 k zst | 26.6 ms / 24 MB / 3.76 M rec/s | 19.5 ms / 29 MB / 5.13 M rec/s | **−27%** | **+36%** | +21% alloc (size-hint over-allocates to avoid realloc) |
| 1 M zst   | 307 ms / 240 MB / 3.26 M rec/s | 237.7 ms / 266 MB / 4.21 M rec/s | **−23%** | **+29%** | +11% alloc (same reason) |

The remaining gap to `post_http_decode_typed` (which just counts records, no Vector storage) is the cost of storing decoded records into the returned `DBNStore`. That's intrinsic to the eager API.

### DatabentoAPI live reader (mock TCP loopback) — post-fix

| Path | Size | n rec | Time | M rec/s | MB/s |
|---|---|---|---|---|---|
| `live_plain` | 100 k | 100 000 | 11.3 ms | **8.87** | 406 |
| `live_zstd`  | 100 k | 100 000 | 16.2 ms | **6.16** | 282 |

Reader timing window starts on first record arrival and ends when the expected N is reached.

### Replay-stress bench against a real OPRA CMBP-1 archive

`benchmark/bench_live_replay.jl` takes a real `.dbn.zst` (typically from `batch_download`) and streams it through the live-reader path on TCP loopback at full LAN speed (~3 GB/s per `bench_tcp_throughput.jl`). The mock writes as fast as the kernel allows; if the reader pipeline can't drain that rate, the channel saturates and the bench reports it.

**Fixture**: SPY.OPT parent, schema CMBP_1, 1-min window (2026-05-13T13:30–13:31 UTC, the first minute of regular session). 7 722 839 records, 618 MB uncompressed, 206 MB zstd on the wire. ~128 k rec/s avg arrival rate — well below this bench's delivery, so the mock effectively saturates the channel for the whole replay.

| channel_size | Drain rate | Producer wire rate | Peak depth | Near-full % | GC% |
|---|---|---|---|---|---|
| 4 096   | 2.7 M rec/s  | 31.7 MB/s | 99.9% | 11.9% | 11.6% |
| 16 384  | 3.0 M rec/s  | 84.1 MB/s | 99.4% | 9.7%  | 10.2% |
| 65 536  | 2.99 M rec/s | 82.0 MB/s | 99.8% | 9.0%  | 10.7% |
| 262 144 | 2.97 M rec/s | 77.9 MB/s | 100.0%| 5.8%  | 11.1% |

Sustained drain rate is **~3.0 M rec/s** of CMBP1Msg regardless of channel size — the consumer's `take!` loop is the floor, not the reader/decoder. Channel saturation in every configuration confirms the reader is delivering faster than the consumer can absorb.

#### What this means in production

| Workload | Rate | Headroom |
|---|---|---|
| SPY.OPT CMBP-1 (1 min sample, avg)     | ~128 k rec/s   | **23× headroom** at our 3.0 M rec/s ceiling |
| ALL_SYMBOLS CMBP-1 (5 min sample, avg) | ~4.7 M rec/s   | **0.6× — overload at open** |
| ALL_SYMBOLS CMBP-1 at 1 Gbps wire saturation (theoretical, ~9.4 M rec/s) | ~9.4 M rec/s | **0.3×** |

For per-parent subscriptions (SPY.OPT, SPX.OPT, etc.) the current reader has plenty of headroom. For the full OPRA CMBP-1 firehose during the open, the consumer side becomes the gate. F1b (typed channel with concrete record type per subscription) is the lever that lifts this — `Channel{CMBP1Msg}` `take!` is faster than `Channel{Union}` `take!` because no Union dispatch per element, and the per-record GC pressure (10.5% here at scale) collapses.

**Caveat**: this is a "consumer just counts" measurement. A real consumer (writing to disk, computing analytics, applying analytics) would be slower. The 3.0 M rec/s is an upper bound on what the channel layer can deliver, not a sustainable rate for arbitrary downstream work.

#### How to reproduce

```bash
# 1. Fetch the archive (one-time, ~30 s queue + a few minutes processing)
julia --project=benchmark benchmark/_fetch_replay_fixture.jl \
    --symbols SPY.OPT \
    --start   2026-05-13T13:30:00 \
    --end     2026-05-13T13:31:00

# 2. Replay against the live reader (~3 s per run)
julia --project=benchmark -e 'using DatabentoAPI; \
    include("benchmark/bench_live_replay.jl"); \
    BenchLiveReplay.run(path = "benchmark/data/replay_SPY.OPT_2026-05-13T13_30_00__2026-05-13T13_31_00.dbn.zst")'
```

#### R3 root-cause investigation — the 50 k rec/s ceiling was a bench bug

The original report claimed a 200× gap between in-process pipeline (10 M rec/s) and TCP-loopback (50 k rec/s). Layered diagnostics in `benchmark/bench_tcp_throughput.jl` and an ad-hoc instrumented harness revealed:

| Test | Result |
|---|---|
| Raw `readbytes!(sock, 64 KB)` on TCPSocket | **3 GB/s** |
| Same through `CountingIO + BufferedReader` (bulk) | 800 MB/s |
| BufferedReader `read(UInt64)` x 600 k (live reader path) | **1.5 MB/s** ⚠️ |
| Same with mock that closes the socket immediately after writing | **1.1 GB/s** ✅ |

Smoking gun: 74 BufferedReader refills, each taking 41 ms. But timing a single `readbytes!(sock, 64 KB)` against the same socket after sleep showed 2 µs — a 20,000× difference. The 41 ms came from the **final** refill call: BufferedReader asks for `length(buffer) = 64 KB` but the stream only has, say, 21 KB left before EOF. `readbytes!(::TCPSocket, ::Vector{UInt8}, ::Int)` blocks via `wait_readnb` until either nb bytes arrive OR the socket closes. With the mock holding the connection open for 2 s post-payload, the last refill blocks the full 2 s waiting for FIN.

**Fix:** `benchmark/bench_live_reader.jl` and `benchmark/bench_stream_to_file.jl` mocks now close the socket immediately after writing the payload (no post-write `sleep`). Result: live reader 50 k → 8.87 M rec/s (**177×**), `stream_to_file` 2.15 s → 148 ms (**14×**).

Mitigation guidance for real-world consumers: the live gateway streams continuously and doesn't close after a finite payload, so this artefact never appears in production. If a consumer wraps a live stream around a finite chunk of bytes (e.g. a recorded replay file pretending to be a live stream), prefer `read_dbn` / `foreach_record` over the Live client.

#### Profile dumps (for reference)

- `benchmark/results/live_inproc_union_channel.profile.txt` — in-process pipeline, current path.
- `benchmark/results/live_inproc_typed.profile.txt` — in-process pipeline, proposed typed path.
- `benchmark/bench_tcp_throughput.jl` — raw TCP loopback chunk-size sweep (3 GB/s baseline).

Despite the bench bug, the in-process profile data is still useful: it shows the decode pipeline can do 10 M rec/s, and that a typed-loop variant could add another +23%. That remains a real future-work opportunity (F1 below) — just not a 200× one.

### DatabentoAPI `stream_to_file` (mock TCP source)

All four wire×file compression configurations land at the same ~2150 ms ceiling — bounded by the live-reader throughput, not the write path:

| Wire | File | Time | Alloc |
|---|---|---|---|
| plain | plain | 2.15 s | 34 MB |
| plain | zstd  | 2.16 s | 34 MB |
| zstd  | plain | 2.17 s | 38 MB |
| zstd  | zstd  | 2.16 s | 38 MB |

---

## Bottleneck analysis (from `profile_hotspots`, medium tier, 5 s window)

### `read_dbn` (generic eager) — uncompressed 1 M records

- 81% of samples in `Base.gc` (`gcutils.jl:133` — **2 048 / 2 536 samples**). Confirms the GC-dominance.
- `read_record_dispatch` only 147 samples (5.8%); actual decode is fast.
- `Vector{Union}` push at `array.jl:1020/1025 setindex!` — 71 samples (2.8%).
- The Union-typed Vector causes per-element write barrier + heap allocation per push; the resulting GC frequency dwarfs decode itself.

### `foreach_record` typed — zstd 1 M records

- Top hotspot: `TranscodingStreams.sloweof / fillbuffer / readbytes!` — combined ~1 800 samples / 2 647 = **68%**. Zstd decompress dominates.
- `DBN.foreach_record` itself: 2 634 samples (everything below it is the descend).
- Total alloc: 13 MB over the 5 s window across many iterations; per-iteration alloc is 0.88 MB.
- 0.13 MB / iter × 100 k records ≈ **9 bytes per record (constant)** — no per-record allocation. As designed.

### `foreach_generic` (post-HTTP path with Union but no Vector) — zstd 1 M records

- `read_record_dispatch` 897 samples (~32%).
- `RefValue` allocation at `refvalue.jl:8` — **779 samples (~27.5%)**. The generic `read_record` returns `Union{Nothing, ...}` which gets Ref-boxed in the hot loop. **Roughly one allocation per record, just for the Ref.**
- zstd decompress ~20% (similar fraction).
- Eliminating the Ref boxing alone would give ~25% throughput in the generic path without touching the Vector issue.

---

## Cross-package comparison

For the same 100 k-record zstd file:

| Path | Wall | M rec/s |
|---|---|---|
| DBN.jl `foreach_record` typed (file)         |   5.4 ms | 18.5 |
| DatabentoAPI post-HTTP decode typed (memory) |  21.1 ms |  4.75 |
| DatabentoAPI `get_range` eager (memory)      |  26.6 ms |  3.76 |
| DatabentoAPI live reader (TCP loopback)      | 2012 ms |  0.05 |

The file→typed-foreach path is **390× faster** than the live-reader path on the same record volume. This is the headline gap to close if live-throughput matters; file/batch workflows are already in good shape.

---

## Recommendations — status

### R1 — typed `get_range`  **[DONE]**

Implemented: `DBNStore{T}` parameterised in `src/store.jl`; typed `decode_dbn_stream(io, T)` and `decode_dbn_bytes(bytes, T)` overloads using `DBN._foreach_record_impl` for zero-per-record allocation; `get_range` now reads `T = record_type_for_schema(schema)` and dispatches to the typed path when non-nothing; `typed::Bool = true` kwarg lets callers force the legacy Union path. Allocation size-hint heuristic added (~8× zstd expansion, lower bound) to skip realloc churn.

**Measured:** at 1 M records 307 → 238 ms (−23%), 3.26 → 4.21 M rec/s (+29%); at 100 k 27 → 20 ms (−27%), 3.76 → 5.13 M rec/s (+36%). Alloc grows ~10–20% due to the over-sized hint; this is the trade for eliminating GC churn (medium-tier GC dropped 12.5% → 0.1%).

### R2 — schema-aware typed dispatch in `DBN.read_dbn`  **[DONE]**

Implemented: added `record_type_for_dbn_schema(::Schema.T)` in `DBN.jl/src/streaming.jl` (11 schemas → 7 concrete types). `read_dbn` and `read_dbn_with_metadata` open the decoder, inspect the metadata schema, and dispatch to `read_dbn_typed(filename, T)` when possible. Falls back to the generic Union loop on rtype mismatch (mixed files, control-only files) so the legacy permissive semantics are preserved.

**Measured:** at 1 M records 300 → 33.6 ms (**−89%**), 3.3 → 29.8 M rec/s (**+9×**), GC 84.2% → 0%; at 100 k 212 → 3.4 ms (**−98%**), 0.47 → 29.7 M rec/s (**+63×**), GC 97.6% → 0%. `read_dbn` now matches `read_dbn_typed` exactly for type-pure schemas.

### R3 — live reader bottleneck  **[DONE — investigation only]**

Implemented: `benchmark/profile_live_reader.jl` drives the reader-loop body in-process (IOBuffer source). Result: the in-process pipeline runs at **8.5–10.5 M rec/s** — roughly 200× faster than the 50 k rec/s observed through the TCP-loopback bench. The bottleneck is the Windows TCP / `Sockets.jl` layer, not the decode or channel.

The typed-loop fix would deliver +23% throughput and −78% alloc on the in-process path, but: (a) live streams interleave control records (`SymbolMappingMsg`, `SystemMsg`, `ErrorMsg`) with data and the typed `_foreach_record_impl` errors on rtype mismatch, requiring a new "skip control" variant; and (b) the win is masked by TCP. Reader code change deferred — see "Future work / R3 follow-up" below.

### R4 — eliminate Ref boxing in `DBN.read_record`  **[DEFERRED]**

Out of scope for this pass. R1 + R2 now route the two dominant eager paths (`read_dbn` and `get_range`) through the typed reader, which doesn't go via `read_record`, so the remaining audience for R4 is narrower (callers of `DBNStream` and mixed-schema generic iteration). Track as a follow-up if those paths become hot.

### R5 — `stream_to_file` default `compress_level = 1`  **[DONE]**

Implemented: default changed in `src/live/streaming.jl:429` (`stream_to_file`) and `:499` (`stream_multi_to_files`). Docstring updated with the bench-derived rationale (L1 ≈ 35% faster at the write boundary than L3, ~15% larger files). New `test/test_live_streaming.jl` testset exercises an explicit `compress_level = 9` override to keep the parameter path covered.

### R6 — document `DBNStream` allocation cost  **[DONE]**

Implemented: "Performance" paragraph added to `DBN.jl/src/streaming.jl` `DBNStream` docstring; `DatabentoAPI.jl/README.md` now points readers at `foreach_record` for performance-sensitive iteration.

### R7 — bench-internal note  **[DROPPED]**

Not a code change; was a flag for future bench authors.

---

## GC analysis

| Path | GC% before | GC% after | Notes |
|---|---|---|---|
| `read_dbn` 100 k plain | 97.6% | **0%**  | R2 routes type-pure schemas through `read_dbn_typed`. |
| `read_dbn` 100 k zst   | 94.5% | **0%**  | Same. |
| `read_dbn` 1 M plain   | 84.2% | **0%**  | Same. |
| `read_dbn` 1 M zst     | 68.1% | **0%**  | Same. |
| `read_dbn_typed` all   | 0% | 0% | Unchanged; reference baseline. |
| `foreach_record` typed | 0% | 0% | Unchanged; reference baseline. |
| `DBNStream` 1 M plain  | 7.6% | 6.8% | Unchanged — still uses Union iteration; R6 doc warning added. |
| `get_range` 1 M zst    | 3.0% | 0.1% | R1 routes through typed decode + size-hint. |
| `get_range` 100 k zst  | 0%   | 0%   | Already small enough to avoid GC. |

R1+R2 collapsed all 80%+ GC paths to 0%. The remaining warm GC is in `DBNStream` (R6 doc-only) and the mixed-schema generic `read_record` fallback (R4 deferred).

---

## Future work

Status: each item below has been triaged after the implementation pass. Concrete design sketches are given where the next step is non-trivial.

### F1 — Live reader typed-loop fast path  (was R3 follow-up; now opportunistic)

With the live reader actually running at 8.87 M rec/s (no longer "200× slow"), this is no longer urgent. The "+23% throughput, −78% allocation" headline from earlier drafts of this report referred to the **full** F1 variant below (typed decode AND typed channel). Halfway versions buy almost nothing — see the calibration table below.

#### Two variants

**F1a — typed decode only, keep `Channel{DBN.DBNRecord}`** (~30 min change)

```julia
T = record_type_for_dbn_schema(subscriptions[1].schema)
DBN.foreach_record_with_control(
    rec  -> put!(c.channel, rec),
    ctrl -> put!(c.channel, ctrl),
    decoder, T)
```

Avoids the Union allocation on the decode side. But `put!(Channel{DBN.DBNRecord}, concrete_T_value)` still has to box the value into the channel's Union element type — the same heap allocation just happens one frame later. Net: **+5–10% throughput, minimal alloc reduction.** Not worth the new DBN.jl public API.

**F1b — typed decode AND typed channel** (medium refactor)

This is the version that hits the in-process bench's +23% / −78% numbers. Requires:

1. New DBN.jl API: `foreach_record_with_control(f_data, f_control, decoder, ::Type{T})`. Calls `f_data` with a concrete `T` (via reused `Ref{T}()` buffer, zero alloc) for matching rtypes, `f_control` with a Union for `ErrorMsg`/`SystemMsg`/`SymbolMappingMsg`.
2. `Live` parameterised as `Live{T}` with default `T = DBN.DBNRecord` (back-compat). New `channel_eltype` kwarg on the constructor; default = current behaviour.
3. Second channel (or callback) for control records, since `Channel{TradeMsg}` cannot hold `SystemMsg`. Likely shape: `control_channel::Channel{DBN.DBNRecord}` with `subscribe_control(c, fn)` for consumers.
4. Reader loop dispatches to typed-or-generic path based on `T` and subscription count.
5. Tests: reconnect path (`test_live_streaming.jl`), mixed-rtype mock-gateway round trip, control-record routing.

**Risk:** moderate. Adds DBN.jl public API + `Live` API surface. Reconnect-path tests need re-validation.

#### Calibration (from `profile_live_reader.jl` in-process bench, 100 k TradeMsg)

| Variant | M rec/s | Alloc/100 k | Notes |
|---|---|---|---|
| Current (`read_record` + `Channel{Union}`)        |  8.5 | 18.8 MB | baseline |
| F1a (typed read + `Channel{Union}`)                | ~9–9.5 | ~16 MB | put! re-boxes — small win |
| F1b (typed read + `Channel{TradeMsg}`)             | 10.5 | 4.1 MB | full +23% / −78% |
| Channel-only baseline (no decode at all)           | 22.1 | 2.4 MB | ceiling left after decode |

#### Verdict

This is **only worth doing as F1b**, and only for a niche audience: callers running single-schema live captures at >5 Gbps wire rate (the current 8.5 M rec/s × 48 bytes ≈ 408 MB/s decompressed ≈ ~5 Gbps after zstd assuming a 6× ratio), or memory-tight long-running processes that can't tolerate ~188 bytes/record GC pressure. For everything else the current Union-channel path is already several times faster than any realistic wire delivers. Recommendation: leave on the shelf unless a real workload demands it.

### F2 — Linux re-benchmark  **[no longer required]**

The original motivation (200× Windows-vs-Linux gap) was a bench bug. The live reader does ~9 M rec/s on Windows TCP loopback post-fix. A Linux baseline is still nice-to-have for completeness but isn't blocking anything.

### F3 — Eliminate `Ref` boxing in `DBN.read_record`  (R4)

**Skip indefinitely.** After R1+R2 + the bench-fix-revealed live reader is fine, no performance-sensitive caller of generic `read_record` remains. `DBNStream` is doc-deprecated for high-throughput (R6).

### F4 — `size_hint` kwarg on `get_range`  **[DONE]**

`get_range(c; ..., size_hint = n)` accepts an exact record-count bound from callers who have one (e.g. from a prior `get_record_count` call). When supplied, it overrides the default 8×-zstd-expansion heuristic, avoiding the 10–20% over-allocation. Tested with both `typed=true` and `typed=false`.

---

## Verification

- DatabentoAPI.jl: `julia --project=. -e 'using Pkg; Pkg.test()'` → 197/197 passing (was 188/188 pre-pass; +9 tests for R1 + R5).
- DBN.jl: `julia --project=. test/runtests.jl` → 3619/3619 R2-related tests pass; 10 pre-existing errors in `test_phase10_complete.jl` from missing `using DataFrames` import (unrelated to this pass; verified by checking the import block at the top of that file).
- Benchmark suite reproducibility: re-running `BenchDBNRead.run(tiers = (:small,))` twice produces consistent numbers for the slower paths (±10%); microsecond-scale benches (now `read_dbn` after R2) drift up to ±50% — compare medians, not single runs.
- Full bench suite runs cleanly end-to-end via `julia --project=benchmark benchmark/runbench.jl` (no errors; per-suite CSVs in `benchmark/results/`).

## How to reproduce

```bash
cd DatabentoAPI.jl
julia --project=benchmark -e 'using Pkg; Pkg.instantiate()'
julia --project=benchmark benchmark/runbench.jl              # all suites, all tiers
julia --project=benchmark benchmark/runbench.jl --profile    # adds profiling pass
julia --project=benchmark benchmark/runbench.jl --tiers=small  # quick smoke
```

Per-suite invocations (faster iteration):

```bash
julia --project=benchmark -e 'include("benchmark/bench_dbn_read.jl"); BenchDBNRead.run(tiers = (:small, :medium))'
julia --project=benchmark -e 'include("benchmark/profile_hotspots.jl"); ProfileHotspots.run(tier = :medium, seconds = 5.0)'
```
