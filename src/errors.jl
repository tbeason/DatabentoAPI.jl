"""
    BentoError <: Exception

Abstract root of every error type this package raises. Catch this if you want
to handle any DatabentoAPI failure (auth, HTTP, etc.) without enumerating the
concrete types.
"""
abstract type BentoError <: Exception end

"""
    BentoAuthError(msg) <: BentoError

Raised when the API key cannot be resolved or the gateway rejects
authentication. Sources include the CRAM handshake on the Live TCP API and
HTTP `401 Unauthorized` responses on the Historical API.
"""
struct BentoAuthError <: BentoError
    msg::String
end

Base.showerror(io::IO, e::BentoAuthError) = print(io, "BentoAuthError: ", e.msg)

"""
    BentoHttpError <: BentoError

Abstract supertype for HTTP-layer failures on the Historical API. Subtypes
[`BentoClientError`](@ref) (4xx) and [`BentoServerError`](@ref) (5xx) carry
the parsed response payload.
"""
abstract type BentoHttpError <: BentoError end

"""
    BentoClientError(status, case, message, docs_url, request_id, body) <: BentoHttpError

Raised when an HTTP request to the Historical API returns a 4xx status
(bad request, missing parameters, dataset/symbol not found, etc.). The
fields mirror Databento's standard error envelope; `request_id` is useful
for support tickets.
"""
struct BentoClientError <: BentoHttpError
    status::Int
    case::String
    message::String
    docs_url::String
    request_id::String
    body::String
end

"""
    BentoServerError(status, case, message, docs_url, request_id, body) <: BentoHttpError

Raised when an HTTP request to the Historical API returns a 5xx status
(gateway timeout, internal error, etc.). Same field layout as
[`BentoClientError`](@ref); retrying after backoff is usually appropriate.
"""
struct BentoServerError <: BentoHttpError
    status::Int
    case::String
    message::String
    docs_url::String
    request_id::String
    body::String
end

function _parse_http_error_body(body::AbstractString)
    case = ""
    message = String(body)
    docs_url = ""
    request_id = ""
    if !isempty(body)
        try
            j = JSON3.read(body)
            if j isa JSON3.Object
                detail = get(j, :detail, j)
                if detail isa JSON3.Object
                    case = String(get(detail, :case, ""))
                    message = String(get(detail, :message, message))
                    docs_url = String(get(detail, :docs_url, ""))
                    request_id = String(get(detail, :request_id, ""))
                else
                    case = String(get(j, :case, ""))
                    message = String(get(j, :message, message))
                    docs_url = String(get(j, :docs_url, ""))
                    request_id = String(get(j, :request_id, ""))
                end
            end
        catch
            # non-JSON body — keep raw
        end
    end
    return case, message, docs_url, request_id
end

function http_error_from_response(::Type{T}, status::Integer, body::AbstractString,
                                  request_id::AbstractString = "") where {T<:BentoHttpError}
    case, msg, docs, rid = _parse_http_error_body(body)
    isempty(rid) && (rid = String(request_id))
    return T(Int(status), case, msg, docs, rid, String(body))
end

function Base.showerror(io::IO, e::T) where {T<:BentoHttpError}
    print(io, nameof(T), "(", e.status, ")")
    isempty(e.case) || print(io, " [", e.case, "]")
    print(io, ": ", e.message)
    isempty(e.request_id) || print(io, " (request_id=", e.request_id, ")")
    isempty(e.docs_url) || print(io, "\n  docs: ", e.docs_url)
end
