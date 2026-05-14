using Test
using DatabentoAPI
using DatabentoAPI: http_error_from_response

@testset "errors" begin
    @testset "JSON body parses fields" begin
        body = """{"detail":{"case":"bad_request","message":"missing dataset","docs_url":"https://databento.com/docs"}}"""
        e = http_error_from_response(BentoClientError, 400, body, "req-123")
        @test e isa BentoClientError
        @test e.status == 400
        @test e.case == "bad_request"
        @test e.message == "missing dataset"
        @test occursin("docs", e.docs_url)
        @test e.request_id == "req-123"
    end

    @testset "non-JSON body kept raw" begin
        body = "Internal Server Error"
        e = http_error_from_response(BentoServerError, 503, body)
        @test e isa BentoServerError
        @test e.message == "Internal Server Error"
        @test e.case == ""
    end

    @testset "showerror format" begin
        e = BentoClientError(401, "auth_error", "bad key", "https://docs", "req-9", "")
        s = sprint(showerror, e)
        @test occursin("BentoClientError(401)", s)
        @test occursin("[auth_error]", s)
        @test occursin("bad key", s)
        @test occursin("req-9", s)
        @test occursin("https://docs", s)
    end

    @testset "BentoAuthError" begin
        e = BentoAuthError("key missing")
        @test sprint(showerror, e) == "BentoAuthError: key missing"
        @test e isa BentoError
    end

    @testset "type hierarchy" begin
        @test BentoClientError <: BentoHttpError
        @test BentoServerError <: BentoHttpError
        @test BentoHttpError <: BentoError
        @test BentoAuthError <: BentoError
    end
end
