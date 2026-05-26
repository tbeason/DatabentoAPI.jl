# Troubleshooting

Common gotchas and how to diagnose them.

## Authentication

### `BentoAuthError: ENV var DATABENTO_API_KEY not set …`

The package found neither an explicit `key` argument, a config file at
`~/.databento/config.toml`, nor a `DATABENTO_API_KEY` environment variable.
See [Authentication](guide/authentication.md).

### `BentoAuthError: Live authentication failed: invalid api_key`

The key resolved fine locally but the gateway rejected it. Causes:

- The key was rotated or revoked on the Databento side.
- The key doesn't have the `live` permission for the dataset you're
  subscribing to (Historical-only keys can read historical data but
  can't open a Live TCP session).
- The `dataset` kwarg doesn't match a dataset your key is provisioned for.

### `BentoClientError(401)` on Historical requests

Same root cause as the Live variant — wrong key or wrong dataset. The
`request_id` field in the error is the right thing to give to Databento
support.

## Historical

### Query too large to materialize

If `get_range` exhausts memory or returns extremely slowly, switch to
[`foreach_record`](@ref) with a typed callback. For multi-GB queries,
[`submit_job`](@ref) → [`batch_download`](@ref) is the right tool.

### Cost surprise

Always preview before issuing a large `get_range`:

```julia
get_record_count(client; dataset, schema, symbols, start, end_)
get_billable_size(client; dataset, schema, symbols, start, end_)
get_cost(client; dataset, schema, symbols, start, end_, mode = FeedMode.HISTORICAL)
```

All three are free and run server-side.

## Live

### "Slow reader" warnings or skipped records

The gateway is sending records faster than your consumer can pull them
off the channel. Two knobs:

- Increase `channel_size` (default `10_000`) when constructing the
  [`Live`](@ref) client. Larger buffers absorb bursts.
- Override [`SlowReaderBehavior`](@ref) explicitly if the default for your
  schema doesn't match your tolerance for stale-vs-skipped records.

If your consumer is doing CPU-heavy work per record, move the work to a
separate task and have the iterator just `put!` records into a worker
channel.

### Ctrl-C doesn't stop the stream cleanly

`Ctrl-C` raises `InterruptException` on the main task. The `Live` do-block
form catches it in `finally` and runs `close(client)`:

```julia
Live(...) do client
    # …
end   # Ctrl-C lands here cleanly
```

If you use the manual lifecycle, wrap your loop in
`try/finally close(client) end` so cleanup runs regardless.

On Windows console, Ctrl-C can be slow to register if Julia is mid-`take!`;
this is a Julia / libuv interaction, not the package. The capture functions
poll for shutdown every 0.5s so they exit within that window.

### `stream_to_file` returned after the duration even though Ctrl-C was hit

By design — Ctrl-C unwinds through the do-block and closes the writer
cleanly, then the function returns the path it last wrote. The file is
a well-formed `.dbn.zst` (frame footer flushed). If you want a
"return immediately, no cleanup" exit, kill the process; the multi-frame
zstd file format means you lose at most the last in-flight frame.

## Capture files

### Multi-frame zstd in tools that don't auto-concatenate

Most decoders (including `zstd`, libzstd-backed tools, and DatabentoAPI.jl's
own [`read_capture`](@ref)) read multi-frame `.dbn.zst` transparently. If
you hit a tool that doesn't, decode once with
`zstd -d file.dbn.zst -o file.dbn` and then feed the uncompressed file.

### Captured file is empty after a quick kill

If you SIGKILL'd within the first `frame_seconds` of the capture, the file
may have no frame footer — the zstd header is present but the first frame
is incomplete. Set `frame_seconds` shorter for short-lived captures, or
let the process exit cleanly via Ctrl-C (clean shutdown always flushes
the active frame).

## Tests / development

### Running live-network smoke tests

```powershell
$env:DATABENTO_LIVE_TESTS = "1"
julia --project=. -e 'using Pkg; Pkg.test()'
```

Override the defaults:

| Variable                        | Default        | Notes                               |
|---------------------------------|----------------|-------------------------------------|
| `DATABENTO_LIVE_DATASET`        | `OPRA.PILLAR`  | Live test dataset                   |
| `DATABENTO_LIVE_SYMBOLS`        | provider-default | Comma-separated                   |
| `DATABENTO_LIVE_STYPE`          | `parent`        | `raw_symbol` / `parent` / `continuous` |
| `DATABENTO_HIST_DATASET`        | varies          | Historical smoke dataset            |
| `DATABENTO_HIST_SYMBOLS`        | varies          | Historical smoke symbols            |
| `DATABENTO_HIST_START`/`END`    | varies          | Historical smoke timeframe          |

These tests use your real API key and incur real billing — keep them off
unless you specifically want to validate against the production gateway.

### `precompile` fails or hangs

DatabentoAPI.jl precompiles in <10s on a normal laptop. If you see a hang,
it's almost always the transitive `DataFrames` / `JSON3` precompile, not
this package. `rm -rf ~/.julia/compiled/v1.12` and retry.
