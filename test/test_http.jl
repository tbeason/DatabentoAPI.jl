using Test
using Base64
using DatabentoAPI
using DatabentoAPI: HTTPClient, basic_auth_header, _clean_params, request, get_json, post_json
using HTTP

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

    @testset "request maps 5xx → BentoServerError" begin
        function mock(method, url, headers, body; kwargs...)
            HTTP.Response(503; body = "boom")
        end
        c = HTTPClient("k", "https://example.test"; dispatcher = mock)
        @test_throws BentoServerError request(c, :POST, "/v0/foo"; body = (a = 1,))
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
