const USER_AGENT = "DatabentoAPI.jl/0.1.2"
const DEFAULT_TIMEOUT = 100
const DEFAULT_CONNECT_TIMEOUT = 30

# Retry policy. Transient failures (HTTP 429 / 5xx and connection/timeout
# errors) are retried with full-jitter exponential backoff; a `Retry-After`
# header, when present, takes precedence over the computed backoff.
const DEFAULT_MAX_RETRIES = 3
const RETRY_BASE_S = 0.5
const RETRY_CAP_S = 30.0
# Cap an honored `Retry-After` so a misbehaving/hostile header can't pin a call.
const RETRY_AFTER_CAP_S = 60.0

"""
    basic_auth_header(api_key)

Build the value of the `Authorization` header for HTTP basic auth where the API key
is the username and the password is empty (Databento's convention).
"""
basic_auth_header(api_key::AbstractString) = "Basic " * base64encode(string(api_key, ":"))

# Default HTTP dispatcher. Tests substitute their own to mock responses.
_default_http_request(method, url, headers, body; kwargs...) =
    HTTP.request(method, url, headers, body; kwargs...)

# Default low-level streaming opener. Opens a connection, reads the response
# head, and invokes `consume(status, headers, io)` with a readable `io`
# positioned at the body start. `consume` drives the IO synchronously, so the
# socket itself provides backpressure (no unbounded buffering), and the
# `HTTP.open` do-block tears the connection down on every exit path —
# including an early return or an exception out of `consume`. Tests substitute
# their own opener to feed canned bodies/statuses without a live socket.
#
# `consume` is the first parameter so callers can use do-block syntax
# (`stream_opener(c, method, url, headers, qpairs) do status, headers, io ...`).
function _default_http_stream(consume, c, method, url, headers, qpairs)
    HTTP.open(method, url, headers;
              query = qpairs,
              status_exception = false,
              decompress = false,
              retry = false,
              readtimeout = c.timeout,
              connect_timeout = c.connect_timeout) do http
        HTTP.startread(http)
        return consume(http.message.status, http.message.headers, http)
    end
end

mutable struct HTTPClient
    api_key::String
    base_url::String
    timeout::Int
    connect_timeout::Int
    user_agent::String
    dispatcher::Function
    max_retries::Int
    retry_sleep::Function
    stream_opener::Function
end

HTTPClient(api_key::AbstractString, base_url::AbstractString;
           timeout::Integer = DEFAULT_TIMEOUT,
           connect_timeout::Integer = DEFAULT_CONNECT_TIMEOUT,
           user_agent::AbstractString = USER_AGENT,
           dispatcher::Function = _default_http_request,
           max_retries::Integer = DEFAULT_MAX_RETRIES,
           retry_sleep::Function = sleep,
           stream_opener::Function = _default_http_stream) =
    HTTPClient(String(api_key), String(base_url), Int(timeout), Int(connect_timeout),
               String(user_agent), dispatcher, Int(max_retries), retry_sleep, stream_opener)

# A transient HTTP status worth retrying: 429 (rate limit) and 5xx, except
# 501 Not Implemented (a permanent "this server can't do that").
_is_retryable_status(status::Integer) = status == 429 || (500 <= status < 600 && status != 501)

# Transient transport-layer failures from HTTP.jl (connect/timeout/request
# errors). Status errors don't appear here because we pass status_exception=false.
_is_retryable_exception(e) = e isa HTTP.Exceptions.HTTPError

# Parse a `Retry-After` header. The delta-seconds form is honored (capped);
# the rarer HTTP-date form falls back to `nothing` so the caller uses backoff.
function _retry_after_seconds(headers)
    for (k, v) in headers
        if lowercase(String(k)) == "retry-after"
            n = tryparse(Float64, strip(String(v)))
            n === nothing && return nothing
            return clamp(n, 0.0, RETRY_AFTER_CAP_S)
        end
    end
    return nothing
end

# Full-jitter exponential backoff for a 1-based attempt index.
_retry_delay(attempt::Integer; base::Real = RETRY_BASE_S, cap::Real = RETRY_CAP_S) =
    rand() * min(cap, base * 2.0^(attempt - 1))

# Sleep duration before the next attempt: honor Retry-After when supplied,
# otherwise fall back to jittered backoff.
_retry_wait(c::HTTPClient, attempt::Integer, retry_after::Union{Nothing,Real}) =
    c.retry_sleep(retry_after === nothing ? _retry_delay(attempt) : retry_after)

# Drop entries whose value is `nothing`; convert remaining values to strings.
function _clean_params(params)::Vector{Pair{String,String}}
    params === nothing && return Pair{String,String}[]
    out = Pair{String,String}[]
    iter = if params isa AbstractVector{<:Pair}
        params
    elseif params isa AbstractDict
        params
    else
        pairs(params)  # NamedTuple
    end
    for (k, v) in iter
        v === nothing && continue
        if v isa AbstractVector || v isa Tuple
            isempty(v) && continue
            push!(out, String(string(k)) => join(string.(v), ","))
        elseif v isa Bool
            push!(out, String(string(k)) => (v ? "true" : "false"))
        else
            push!(out, String(string(k)) => String(string(v)))
        end
    end
    return out
end

function _request_id(resp)::String
    for (k, v) in resp.headers
        lowercase(String(k)) == "request-id" && return String(v)
    end
    return ""
end

"""
    request(c, method, path; query=nothing, body=nothing, accept="application/json")

Perform an HTTP request against the Databento gateway. Maps 4xx → `BentoClientError`
and 5xx → `BentoServerError`. Returns the underlying `HTTP.Messages.Response`.

Transient failures — HTTP `429` (rate limit) and `5xx`, plus connection/timeout
errors — are retried up to `c.max_retries` times with full-jitter exponential
backoff. A `Retry-After` response header, when present, overrides the backoff.
Once the retry budget is exhausted the final response is mapped to its error as
usual, so a persistently rate-limited request still surfaces `BentoClientError(429)`.

Use `body` as a Dict / NamedTuple for form-encoded POST bodies, or as a `String` /
`Vector{UInt8}` for a pre-encoded body.
"""
function request(c::HTTPClient, method::Symbol, path::AbstractString;
                 query = nothing,
                 body  = nothing,
                 accept::AbstractString = "application/json")
    url = string(c.base_url, path)
    qpairs = _clean_params(query)

    headers = [
        "Authorization" => basic_auth_header(c.api_key),
        "Accept"        => String(accept),
        "User-Agent"    => c.user_agent,
    ]

    body_bytes = if body === nothing
        UInt8[]
    elseif body isa AbstractVector{UInt8}
        body
    elseif body isa AbstractString
        Vector{UInt8}(String(body))
    elseif body isa AbstractDict || body isa NamedTuple || body isa AbstractVector{<:Pair}
        push!(headers, "Content-Type" => "application/x-www-form-urlencoded")
        Vector{UInt8}(URIs.escapeuri(_clean_params(body)))
    else
        Vector{UInt8}(string(body))
    end

    method_str = String(uppercase(string(method)))
    attempt = 0
    while true
        attempt += 1
        resp = try
            c.dispatcher(method_str, url, headers, body_bytes;
                         query              = qpairs,
                         status_exception   = false,
                         readtimeout        = c.timeout,
                         connect_timeout    = c.connect_timeout,
                         retry              = false,
                         decompress         = false)
        catch e
            # Transient transport failure: back off and retry until the budget
            # is spent, then let the original exception propagate.
            if _is_retryable_exception(e) && attempt <= c.max_retries
                _retry_wait(c, attempt, nothing)
                continue
            end
            rethrow()
        end

        # Retry transient statuses while budget remains; on the final attempt
        # fall through to the error mapping below.
        if _is_retryable_status(resp.status) && attempt <= c.max_retries
            _retry_wait(c, attempt, _retry_after_seconds(resp.headers))
            continue
        end

        if 400 <= resp.status < 500
            throw(http_error_from_response(BentoClientError, resp.status,
                                           String(copy(resp.body)), _request_id(resp)))
        elseif resp.status >= 500
            throw(http_error_from_response(BentoServerError, resp.status,
                                           String(copy(resp.body)), _request_id(resp)))
        end
        return resp
    end
end

# Convenience wrappers for the common cases.

"""
    get_json(c, path; query=nothing)

Issue a GET request and parse the JSON response body via JSON3.
"""
function get_json(c::HTTPClient, path::AbstractString; query = nothing)
    resp = request(c, :GET, path; query = query, accept = "application/json")
    return JSON3.read(resp.body)
end

"""
    post_json(c, path; body=nothing)

Issue a POST request with a form-encoded body and parse the JSON response.
"""
function post_json(c::HTTPClient, path::AbstractString; body = nothing)
    resp = request(c, :POST, path; body = body, accept = "application/json")
    return JSON3.read(resp.body)
end

"""
    get_bytes(c, path; query=nothing, accept="application/octet-stream")

Issue a GET request and return raw response bytes. The Databento `timeseries.get_range`
endpoint serves zstd-compressed DBN bytes through this path.
"""
function get_bytes(c::HTTPClient, path::AbstractString;
                   query = nothing,
                   accept::AbstractString = "application/octet-stream")::Vector{UInt8}
    resp = request(c, :GET, path; query = query, accept = accept)
    return Vector{UInt8}(resp.body)
end

# Pull the request-id out of a header collection (case-insensitive).
function _request_id_from_headers(headers)::String
    for (k, v) in headers
        lowercase(String(k)) == "request-id" && return String(v)
    end
    return ""
end

"""
    open_stream(f, c, path; query=nothing, accept="application/octet-stream")

Open a streaming GET against `path` and call `f(io)` with a readable `IO` that
yields the response body bytes as they arrive off the wire. `f` reads `io`
synchronously on the calling task, so the connection provides natural
backpressure — bytes are pulled from the socket only as fast as `f` consumes
them, and the payload is never buffered ahead in memory. The connection is
torn down on every exit path, including `f` returning early (e.g. an iterator
`break`) or throwing, so no socket or task is leaked.

Status-checking happens before `f` is invoked; errors raise
`BentoClientError`/`BentoServerError` like the eager `request` path. The
pre-body phase (connect + read response head) is retried on transient failures
per `c.max_retries`; once bytes have been handed to `f` the call is not
retried (the consumer may already have observed partial output).

Returns whatever `f` returns.
"""
function open_stream(f, c::HTTPClient, path::AbstractString;
                     query = nothing,
                     accept::AbstractString = "application/octet-stream")
    url = string(c.base_url, path)
    qpairs = _clean_params(query)
    headers = [
        "Authorization" => basic_auth_header(c.api_key),
        "Accept"        => String(accept),
        "User-Agent"    => c.user_agent,
    ]

    attempt = 0
    while true
        attempt += 1
        result_ref   = Ref{Any}(nothing)
        err_ref      = Ref{Any}(nothing)
        retry_ref    = Ref(false)
        retry_after  = Ref{Union{Nothing,Float64}}(nothing)

        try
            c.stream_opener(c, "GET", url, headers, qpairs) do status, hdrs, io
                if status >= 400
                    body = read(io)  # drain the error body for a structured message
                    if _is_retryable_status(status) && attempt <= c.max_retries
                        retry_ref[]   = true
                        retry_after[] = _retry_after_seconds(hdrs)
                        return nothing
                    end
                    T = status < 500 ? BentoClientError : BentoServerError
                    err_ref[] = http_error_from_response(T, status, String(body),
                                                         _request_id_from_headers(hdrs))
                    return nothing
                end
                result_ref[] = f(io)
                return nothing
            end
        catch e
            # Transport failure before/while reading the head — safe to retry.
            if _is_retryable_exception(e) && attempt <= c.max_retries
                _retry_wait(c, attempt, nothing)
                continue
            end
            rethrow()
        end

        if retry_ref[]
            _retry_wait(c, attempt, retry_after[])
            continue
        end
        err_ref[] === nothing || throw(err_ref[])
        return result_ref[]
    end
end
