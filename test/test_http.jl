using Test
using Base64
using DatabentoAPI
using DatabentoAPI: HTTPClient, basic_auth_header, _clean_params, request, get_json, post_json
using DatabentoAPI: _is_retryable_status, _retry_after_seconds, _retry_delay, RETRY_CAP_S,
                    RETRY_AFTER_CAP_S, DEFAULT_MAX_RETRIES, DEFAULT_TIMEOUT
using HTTP

# No-op sleep so retry tests don't actually wait on the wall clock.
const _NOSLEEP = _ -> nothing

@testset "http" begin
    @testset "basic_auth_header" begin
        h = basic_auth_header("db-abc")
        @test startswith(h, "Basic ")
        decoded = String(base64decode(split(h, " ")[2]))
        @test decoded == "db-abc:"
    end

    @testset "_clean_params drops nothings, joins vectors, formats bools" begin
        p = _clean_params((a = 1, b = nothing, c = ["x", "y"], d = true, e = false))
        d = Dict(p)
        @test d["a"] == "1"
        @test !haskey(d, "b")
        @test d["c"] == "x,y"
        @test d["d"] == "true"
        @test d["e"] == "false"
    end

    @testset "_clean_params handles AbstractDict input" begin
        p = _clean_params(Dict("a" => 1, "b" => nothing))
        d = Dict(p)
        @test d["a"] == "1"
        @test !haskey(d, "b")
    end

    @testset "request maps 4xx → BentoClientError" begin
        captured = Ref{Any}(nothing)
        function mock(method, url, headers, body; kwargs...)
            captured[] = (; method, url, headers, body, kwargs)
            HTTP.Response(404, ["request-id" => "abc"];
                          body = """{"detail":{"case":"not_found","message":"nope"}}""")
        end
        c = HTTPClient("k", "https://example.test"; dispatcher = mock)
        @test_throws BentoClientError request(c, :GET, "/v0/foo")
        @test captured[].method == "GET"
        @test captured[].url == "https://example.test/v0/foo"
        # Authorization header present
        @test any(p -> first(p) == "Authorization", captured[].headers)
    end

    @testset "request maps 5xx → BentoServerError (no retry)" begin
        function mock(method, url, headers, body; kwargs...)
            HTTP.Response(503; body = "boom")
        end
        c = HTTPClient("k", "https://example.test"; dispatcher = mock, max_retries = 0)
        @test_throws BentoServerError request(c, :POST, "/v0/foo"; body = (a = 1,))
    end

    @testset "retry helpers" begin
        @test _is_retryable_status(429)
        @test _is_retryable_status(500)
        @test _is_retryable_status(503)
        @test !_is_retryable_status(501)   # Not Implemented is permanent
        @test !_is_retryable_status(404)
        @test !_is_retryable_status(200)

        # Retry-After: integer delta-seconds honored (capped), HTTP-date → nothing.
        @test _retry_after_seconds(["Retry-After" => "7"]) == 7.0
        @test _retry_after_seconds(["retry-after" => "2.5"]) == 2.5
        @test _retry_after_seconds(["Retry-After" => string(RETRY_AFTER_CAP_S + 100)]) == RETRY_AFTER_CAP_S
        @test _retry_after_seconds(["Retry-After" => "Wed, 21 Oct 2026 07:28:00 GMT"]) === nothing
        @test _retry_after_seconds(["X-Other" => "5"]) === nothing

        # Backoff stays within the jitter envelope for every attempt.
        for attempt in 1:8
            d = _retry_delay(attempt)
            @test 0.0 <= d <= RETRY_CAP_S
        end
    end

    @testset "request retries transient status then succeeds" begin
        calls = Ref(0)
        function mock(method, url, headers, body; kwargs...)
            calls[] += 1
            calls[] < 3 ? HTTP.Response(503; body = "down") :
                          HTTP.Response(200; body = """{"ok":true}""")
        end
        c = HTTPClient("k", "https://example.test";
                       dispatcher = mock, retry_sleep = _NOSLEEP)
        resp = request(c, :GET, "/v0/foo")
        @test resp.status == 200
        @test calls[] == 3   # two 503s retried, third call succeeds
    end

    @testset "request honors Retry-After on 429" begin
        slept = Float64[]
        calls = Ref(0)
        function mock(method, url, headers, body; kwargs...)
            calls[] += 1
            calls[] == 1 ? HTTP.Response(429, ["Retry-After" => "4"]; body = "slow down") :
                           HTTP.Response(200; body = "{}")
        end
        c = HTTPClient("k", "https://example.test";
                       dispatcher = mock, retry_sleep = d -> push!(slept, d))
        resp = request(c, :GET, "/v0/foo")
        @test resp.status == 200
        @test slept == [4.0]   # Retry-After overrode the jittered backoff
    end

    @testset "request exhausts retry budget then maps the final status" begin
        calls = Ref(0)
        function mock(method, url, headers, body; kwargs...)
            calls[] += 1
            HTTP.Response(503; body = "still down")
        end
        c = HTTPClient("k", "https://example.test";
                       dispatcher = mock, retry_sleep = _NOSLEEP, max_retries = 2)
        @test_throws BentoServerError request(c, :GET, "/v0/foo")
        @test calls[] == 3   # initial attempt + 2 retries
    end

    @testset "request retries transient transport exception" begin
        calls = Ref(0)
        function mock(method, url, headers, body; kwargs...)
            calls[] += 1
            calls[] < 2 && throw(HTTP.ConnectError(url, ErrorException("refused")))
            HTTP.Response(200; body = "{}")
        end
        c = HTTPClient("k", "https://example.test";
                       dispatcher = mock, retry_sleep = _NOSLEEP)
        resp = request(c, :GET, "/v0/foo")
        @test resp.status == 200
        @test calls[] == 2
    end

    @testset "read timeout → BentoTimeoutError, no retry" begin
        calls = Ref(0)
        function mock(method, url, headers, body; kwargs...)
            calls[] += 1
            throw(HTTP.TimeoutError("read", 100))
        end
        c = HTTPClient("k", "https://example.test";
                       dispatcher = mock, retry_sleep = _NOSLEEP, timeout = 100)
        @test_throws BentoTimeoutError request(c, :GET, "/v0/foo")
        @test calls[] == 1   # deterministic failure: no retry budget burned

        # The mapped error carries the configured timeout and an actionable hint.
        err = try
            request(c, :GET, "/v0/foo")
        catch e
            e
        end
        @test err isa BentoTimeoutError
        @test err.timeout_s == 100
        msg = sprint(showerror, err)
        @test occursin("100s", msg)
        @test occursin("Historical(timeout = ...)", msg)
    end

    @testset "connect errors remain retryable after timeout exclusion" begin
        calls = Ref(0)
        function mock(method, url, headers, body; kwargs...)
            calls[] += 1
            calls[] < 3 && throw(HTTP.ConnectError(url, ErrorException("refused")))
            HTTP.Response(200; body = "{}")
        end
        c = HTTPClient("k", "https://example.test";
                       dispatcher = mock, retry_sleep = _NOSLEEP)
        resp = request(c, :GET, "/v0/foo")
        @test resp.status == 200
        @test calls[] == 3
    end

    @testset "connection-phase timeouts (connect/tls_handshake) remain retryable" begin
        # HTTP 2.x reports connection-phase deadline failures as TimeoutError
        # tagged with the phase. The connect_timeout budget covers both the
        # dial ("connect") and the TLS handshake ("tls_handshake"); both are
        # transient and must be retried, not mapped to BentoTimeoutError.
        for op in ("connect", "tls_handshake")
            calls = Ref(0)
            function mock(method, url, headers, body; kwargs...)
                calls[] += 1
                calls[] < 3 && throw(HTTP.TimeoutError(op, 30))
                HTTP.Response(200; body = "{}")
            end
            c = HTTPClient("k", "https://example.test";
                           dispatcher = mock, retry_sleep = _NOSLEEP)
            resp = request(c, :GET, "/v0/foo")
            @test resp.status == 200
            @test calls[] == 3
        end
    end

    @testset "DEFAULT_TIMEOUT raised for long-range queries" begin
        @test DEFAULT_TIMEOUT == 600
    end

    @testset "request does not retry a non-transient exception" begin
        calls = Ref(0)
        function mock(method, url, headers, body; kwargs...)
            calls[] += 1
            throw(ArgumentError("boom"))
        end
        c = HTTPClient("k", "https://example.test";
                       dispatcher = mock, retry_sleep = _NOSLEEP)
        @test_throws ArgumentError request(c, :GET, "/v0/foo")
        @test calls[] == 1   # propagated immediately, not retried
    end

    @testset "request returns response on 2xx" begin
        function mock(method, url, headers, body; kwargs...)
            HTTP.Response(200; body = """{"ok":true}""")
        end
        c = HTTPClient("k", "https://example.test"; dispatcher = mock)
        resp = request(c, :GET, "/v0/foo")
        @test resp.status == 200
    end

    @testset "get_json parses JSON body" begin
        function mock(method, url, headers, body; kwargs...)
            HTTP.Response(200; body = """[{"name":"x"},{"name":"y"}]""")
        end
        c = HTTPClient("k", "https://example.test"; dispatcher = mock)
        result = get_json(c, "/v0/list")
        @test length(result) == 2
        @test result[1]["name"] == "x"
    end

    @testset "post_json sends form-encoded body" begin
        captured_body = Ref{String}("")
        captured_headers = Ref{Any}(nothing)
        function mock(method, url, headers, body; kwargs...)
            captured_body[] = String(copy(body))
            captured_headers[] = headers
            HTTP.Response(200; body = """{"id":"abc"}""")
        end
        c = HTTPClient("k", "https://example.test"; dispatcher = mock)
        result = post_json(c, "/v0/job"; body = (dataset = "X", limit = 5))
        @test result["id"] == "abc"
        @test occursin("dataset=X", captured_body[])
        @test occursin("limit=5", captured_body[])
        @test any(p -> first(p) == "Content-Type" && occursin("urlencoded", last(p)),
                  captured_headers[])
    end
end
