# Enums — API Reference

```@meta
CurrentModule = DatabentoAPI
```

## DatabentoAPI-specific enums

```@docs
HistoricalGateway
FeedMode
ReconnectPolicy
JobState
SplitDuration
Packaging
Delivery
SymbologyResolution
RollRule
SlowReaderBehavior
```

## Re-exported from DatabentoBinaryEncoding

These are the wire-format enums that appear throughout the API. Their full
documentation lives in the
[DatabentoBinaryEncoding docs](https://tbeason.github.io/DatabentoBinaryEncoding.jl).

| Enum | Members (abbreviated) |
|------|------------------------|
| `Schema`          | `MBO`, `MBP_1`, `MBP_10`, `BBO_1S`, `BBO_1M`, `CBBO_1S`, `CBBO_1M`, `TBBO`, `TCBBO`, `TRADES`, `OHLCV_1S/1M/1H/1D`, `DEFINITION`, `STATUS`, `IMBALANCE`, `STATISTICS`, … |
| `SType`           | `RAW_SYMBOL`, `INSTRUMENT_ID`, `PARENT`, `CONTINUOUS`, … |
| `Compression`     | `NONE`, `ZSTD` |
| `Encoding`        | `DBN`, `CSV`, `JSON` |
| `Action`          | `MODIFY`, `TRADE`, `FILL`, `CANCEL`, `ADD`, `CLEAR`, `NONE` |
| `Side`            | `BID`, `ASK`, `NONE` |
| `InstrumentClass` | `BOND`, `CALL`, `FUTURE`, `SPREAD`, `STOCK`, … |
