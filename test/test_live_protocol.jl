using Test
using DatabentoAPI: build_text_frame, read_text_frame

@testset "live protocol text framing" begin
    @testset "build_text_frame omits nothing values" begin
        s = build_text_frame(a = 1, b = nothing, c = "x")
        @test s == "a=1|c=x\n"
    end

    @testset "build_text_frame ends with newline" begin
        s = build_text_frame(only = "v")
        @test endswith(s, "\n")
        @test count(==('\n'), s) == 1
    end

    @testset "round-trip via IOBuffer" begin
        io = IOBuffer()
        write(io, build_text_frame(schema = "trades", symbols = "AAPL,MSFT", id = 1))
        seekstart(io)
        d = read_text_frame(io)
        @test d["schema"] == "trades"
        @test d["symbols"] == "AAPL,MSFT"
        @test d["id"] == "1"
    end

    @testset "frame with no value yields empty string" begin
        io = IOBuffer("greeting=\n")
        d = read_text_frame(io)
        @test d["greeting"] == ""
    end
end
