@enumx HistoricalGateway BO1

@enumx FeedMode HISTORICAL HISTORICAL_STREAMING LIVE

@enumx ReconnectPolicy NONE RECONNECT

@enumx JobState RECEIVED QUEUED PROCESSING DONE EXPIRED

@enumx SplitDuration NONE DAY WEEK MONTH

@enumx Packaging NONE ZIP TAR

@enumx Delivery DOWNLOAD S3 DISK

@enumx SymbologyResolution OK PARTIAL NOT_FOUND

@enumx RollRule VOLUME OPEN_INTEREST CALENDAR

# How the Live gateway handles a client that reads slower than realtime.
#   WARN: gateway buffers and sends stale records, emits periodic SystemMsg
#         with code = SlowReaderWarning (2). Default for stateful schemas.
#   SKIP: gateway drops records until the session is current, then sends an
#         ErrorMsg with code = SkippedRecordsAfterSlowReading (7).
#         Default for stateless schemas (MBP-1/10, CMBP-1, BBO-1s/1m, CBBO-1s/1m).
@enumx SlowReaderBehavior WARN SKIP

const HISTORICAL_GATEWAY_URLS = Dict(
    HistoricalGateway.BO1 => "https://hist.databento.com",
)

gateway_url(g::HistoricalGateway.T) = HISTORICAL_GATEWAY_URLS[g]
