"""
    DatabentoAPI

Julia client for the Databento Historical (HTTP) and Live (TCP) market data APIs.

Wraps Databento's wire protocols on top of
[DatabentoBinaryEncoding.jl](https://github.com/tbeason/DatabentoBinaryEncoding.jl),
which handles all binary record decoding.

# Quick start

```julia
using DatabentoAPI, Dates

# Historical
client = Historical()                       # API key from ~/.databento/config.toml or DATABENTO_API_KEY
store  = get_range(client;
    dataset  = "XNAS.ITCH",
    schema   = Schema.TRADES,
    symbols  = ["AAPL"],
    start    = DateTime(2024, 1, 2, 14, 30),
    end_     = DateTime(2024, 1, 2, 14, 31),
    stype_in = SType.RAW_SYMBOL)
df = to_dataframe(store)

# Live
live = Live(dataset = "GLBX.MDP3")
connect!(live)
subscribe!(live; schema = Schema.TRADES, symbols = ["ES.FUT"], stype_in = SType.PARENT)
start!(live)
for rec in live
    println(rec)
end
close(live)
```
"""
module DatabentoAPI

using Base64
using CodecZstd
using DataFrames
using Dates
using EnumX
using HTTP
using JSON3
using Logging
using SHA
using Sockets
using TOML
using TranscodingStreams
using URIs

import DatabentoBinaryEncoding as DBN
using DatabentoBinaryEncoding: Schema, SType, Compression, Encoding, Action, Side, InstrumentClass

include("errors.jl")
include("enums.jl")
include("auth.jl")
include("http.jl")
include("store.jl")
include("conversion.jl")

include("historical/client.jl")
include("historical/timeseries.jl")
include("historical/metadata.jl")
include("historical/batch.jl")
include("historical/symbology.jl")

include("live/cram.jl")
include("live/gateway.jl")
include("live/protocol.jl")
include("live/client.jl")
include("live/reader.jl")
include("live/subscribe.jl")
include("live/heartbeat.jl")
include("live/streaming.jl")

# Re-exported DatabentoBinaryEncoding enums (single import for users)
export Schema, SType, Compression, Encoding, Action, Side, InstrumentClass

# DatabentoAPI-specific enums
export HistoricalGateway, FeedMode, ReconnectPolicy, JobState
export SplitDuration, Packaging, Delivery, SymbologyResolution, RollRule
export SlowReaderBehavior

# Errors
export BentoError, BentoAuthError, BentoHttpError, BentoClientError, BentoServerError

# Auth
export load_api_key, default_config_path

# Store + conversion
export DBNStore, to_dataframe, to_csv, to_json, to_parquet, to_file

# Clients
export Historical, Live

# Historical leaf endpoints
export get_range, foreach_record
export list_publishers, list_datasets, list_schemas, list_fields
export list_unit_prices, get_dataset_range, get_dataset_condition
export get_record_count, get_billable_size, get_cost
export submit_job, list_jobs, list_files, batch_download
export resolve

# Live lifecycle
export connect!, subscribe!, subscribe_callback, start!, stop!
export channel, control_channel

# Live capture to file
export stream_to_file, stream_multi_to_files, default_live_path, read_capture

end # module DatabentoAPI
