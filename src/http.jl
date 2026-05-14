const USER_AGENT = "DatabentoAPI.jl/0.1.0"
const DEFAULT_TIMEOUT = 100
const DEFAULT_CONNECT_TIMEOUT = 30

"""
    basic_auth_header(api_key)

Build the value of the `Authorization` header for HTTP basic auth where the API key
is the username and the password is empty (Databento's convention).
"""
basic_auth_header(api_key::AbstractString) = "Basic " * base64encode(string(api_key, ":"))

# Default HTTP dispatcher. Tests substitute their own to mock responses.
_default_http_request(method, url, headers, body; kwargs...) =
    HTTP.request(method, url, headers, body; kwargs...)

mutable struct HTTPClient
    api_key::String
    base_url::String
    timeout::Int
    connect_timeout::Int
    user_agent::String
    dispatcher::Function
end

HTTPClient(api_key::AbstractString, base_url::AbstractString;
           timeout::Integer = DEFAULT_TIMEOUT,
           connect_timeout::Integer = DEFAULT_CONNECT_TIMEOUT,
           user_agent::AbstractString = USER_AGENT,
           dispatcher::Function = _default_http_request) =
    HTTPClient(String(api_key), String(base_url), Int(timeout), Int(connect_timeout),
               String(user_agent), dispatcher)

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

    resp = c.dispatcher(String(uppercase(string(method))), url, headers, body_bytes;
                        query              = qpairs,
                        status_exception   = false,
                        readtimeout        = c.timeout,
                        connect_timeout    = c.connect_timeout,
                        retry              = false,
                        decompress         = false)

    if 400 <= resp.status < 500
        throw(http_error_from_response(BentoClientError, resp.status,
                                       String(copy(resp.body)), _request_id(resp)))
    elseif resp.status >= 500
        throw(http_error_from_response(BentoServerError, resp.status,
                                       String(copy(resp.body)), _request_id(resp)))
    end
    return resp
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
