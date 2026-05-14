const DEFAULT_HIST_GATEWAY = "https://hist.databento.com"
const HIST_API_VERSION = "v0"

"""
    Historical([key]; gateway=DEFAULT_HIST_GATEWAY, timeout=100, user_agent=...)

Client for the Databento Historical (HTTP) API.

`key` falls back to `~/.databento/config.toml` then `ENV["DATABENTO_API_KEY"]` (see
[`load_api_key`](@ref)).

```julia
client = Historical()
store  = get_range(client; dataset="XNAS.ITCH", schema=Schema.TRADES,
                          symbols=["AAPL"], start=DateTime(2024,1,2,14,30),
                          end_=DateTime(2024,1,2,14,31), stype_in=SType.RAW_SYMBOL)
```
"""
struct Historical
    http::HTTPClient
end

function Historical(key::Union{Nothing,AbstractString} = nothing;
                    gateway::AbstractString = DEFAULT_HIST_GATEWAY,
                    timeout::Integer = DEFAULT_TIMEOUT,
                    connect_timeout::Integer = DEFAULT_CONNECT_TIMEOUT,
                    user_agent::AbstractString = USER_AGENT,
                    dispatcher::Function = _default_http_request)
    api_key = load_api_key(key)
    return Historical(HTTPClient(api_key, gateway;
                                 timeout = timeout,
                                 connect_timeout = connect_timeout,
                                 user_agent = user_agent,
                                 dispatcher = dispatcher))
end

Base.show(io::IO, c::Historical) =
    print(io, "Historical(gateway=", c.http.base_url, ")")

# ---- shared encoding helpers ----

# Schema.MBP_1 -> "mbp-1", Schema.OHLCV_1S -> "ohlcv-1s", Schema.TRADES -> "trades"
schema_str(s::Schema.T) = lowercase(replace(String(Symbol(s)), "_" => "-"))

# SType.RAW_SYMBOL -> "raw_symbol", SType.INSTRUMENT_ID -> "instrument_id"
stype_str(s::SType.T) = lowercase(String(Symbol(s)))

compression_str(c::Compression.T) = lowercase(String(Symbol(c)))
encoding_str(e::Encoding.T) = lowercase(String(Symbol(e)))

# Databento accepts ISO 8601 strings; Unix-nanosecond ints also work.
ts_str(d::DateTime) = Dates.format(d, dateformat"yyyy-mm-ddTHH:MM:SS")
ts_str(d::Date) = Dates.format(d, dateformat"yyyy-mm-dd")
ts_str(s::AbstractString) = String(s)
ts_str(n::Integer) = string(n)
ts_str(::Nothing) = nothing

symbols_str(s::AbstractString) = String(s)
symbols_str(v::AbstractVector) = join(string.(v), ",")
symbols_str(t::Tuple) = join(string.(t), ",")

# Path helper
hist_path(endpoint::AbstractString) = string("/", HIST_API_VERSION, "/", endpoint)
