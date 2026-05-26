"""
    HistoricalGateway

Historical API gateway selector. Currently only one entry:

  - `BO1` — the default historical gateway at `hist.databento.com`.

Used implicitly by [`Historical`](@ref); most callers never construct this directly.
"""
@enumx HistoricalGateway BO1

"""
    FeedMode

How a Historical price-quote/cost preview should be charged:

  - `HISTORICAL` — vanilla historical replay.
  - `HISTORICAL_STREAMING` — historical replay over the streaming API.
  - `LIVE` — live streaming feed.

Passed to [`get_cost`](@ref) (default `HISTORICAL`).
"""
@enumx FeedMode HISTORICAL HISTORICAL_STREAMING LIVE

"""
    ReconnectPolicy

Reconnect behavior for the Live client.

  - `NONE` — disconnect is terminal.
  - `RECONNECT` — re-establish the TCP session and re-subscribe on drop.

Exposed for parity with Databento's other clients; in this package the
behavior is selected via the `reconnect::Bool` kwarg on
[`stream_to_file`](@ref) and friends.
"""
@enumx ReconnectPolicy NONE RECONNECT

"""
    JobState

State of a batch job returned by [`list_jobs`](@ref):

  - `RECEIVED` — accepted by the API.
  - `QUEUED` — waiting to start.
  - `PROCESSING` — currently being assembled.
  - `DONE` — files ready for [`list_files`](@ref) / [`batch_download`](@ref).
  - `EXPIRED` — past the retention window; files no longer available.
"""
@enumx JobState RECEIVED QUEUED PROCESSING DONE EXPIRED

"""
    SplitDuration

Batch-output file rotation cadence — when a single job's records are sliced
into multiple files by time:

  - `NONE` — one file per job.
  - `DAY`, `WEEK`, `MONTH` — rotate at the corresponding UTC boundary.

Passed to [`submit_job`](@ref).
"""
@enumx SplitDuration NONE DAY WEEK MONTH

"""
    Packaging

How the batch-output files are bundled for delivery:

  - `NONE` — files served individually.
  - `ZIP` — single .zip archive.
  - `TAR` — single .tar archive.

Passed to [`submit_job`](@ref).
"""
@enumx Packaging NONE ZIP TAR

"""
    Delivery

Where the batch-output files are delivered:

  - `DOWNLOAD` — fetched from the API via [`batch_download`](@ref).
  - `S3` — pushed to a customer-configured S3 bucket.
  - `DISK` — written to a customer-configured local disk (on-prem).

Passed to [`submit_job`](@ref).
"""
@enumx Delivery DOWNLOAD S3 DISK

"""
    SymbologyResolution

Per-symbol resolution status returned by [`resolve`](@ref):

  - `OK` — fully resolved within the requested date range.
  - `PARTIAL` — resolved for a sub-interval only (instrument expired or rolled).
  - `NOT_FOUND` — no mapping in the requested date range.
"""
@enumx SymbologyResolution OK PARTIAL NOT_FOUND

"""
    RollRule

Continuous-contract roll rule for futures symbology:

  - `VOLUME` — roll when next-contract volume exceeds the front.
  - `OPEN_INTEREST` — roll when next-contract open interest exceeds the front.
  - `CALENDAR` — roll at a fixed calendar offset (e.g. N days before expiry).
"""
@enumx RollRule VOLUME OPEN_INTEREST CALENDAR

"""
    SlowReaderBehavior

How the Live gateway handles a client that reads slower than realtime:

  - `WARN` — gateway buffers and sends stale records, emits periodic
    `SystemMsg` with `code = SlowReaderWarning (2)`. Default for stateful
    schemas where dropping records would corrupt downstream state.
  - `SKIP` — gateway drops records until the session is current, then sends
    an `ErrorMsg` with `code = SkippedRecordsAfterSlowReading (7)`. Default
    for stateless schemas (MBP-1/10, CMBP-1, BBO-1s/1m, CBBO-1s/1m).

Set via the `slow_reader_behavior` kwarg on [`Live`](@ref) or
[`stream_to_file`](@ref).
"""
@enumx SlowReaderBehavior WARN SKIP

const HISTORICAL_GATEWAY_URLS = Dict(
    HistoricalGateway.BO1 => "https://hist.databento.com",
)

gateway_url(g::HistoricalGateway.T) = HISTORICAL_GATEWAY_URLS[g]
