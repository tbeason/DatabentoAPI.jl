using Test
using SHA
using DatabentoAPI: cram_response

@testset "cram" begin
    @testset "matches reference SHA256-pipe-key + bucket id" begin
        challenge = "abcdef0123456789"
        api_key   = "db-1234567890abcdef12345"   # 23 chars, last 5 = "12345"
        expected_digest = bytes2hex(SHA.sha256("$(challenge)|$(api_key)"))
        expected = "$(expected_digest)-12345"
        @test cram_response(challenge, api_key) == expected
    end

    @testset "bucket id is exactly the last 5 chars" begin
        @test endswith(cram_response("c", "abcdefghij"), "-fghij")
        @test endswith(cram_response("c", "abcdefghijklmnop"), "-lmnop")
    end

    @testset "rejects too-short keys" begin
        @test_throws Exception cram_response("c", "abc")
    end
end
