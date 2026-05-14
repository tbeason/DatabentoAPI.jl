using Test
using DatabentoAPI
using HTTP

@testset "historical symbology" begin
    captured = Ref{String}("")
    function mock(method, url, headers, body; kwargs...)
        @test method == "POST"
        @test occursin("symbology.resolve", url)
        captured[] = String(copy(body))
        HTTP.Response(200; body = """{"result":{"AAPL":[]},"symbols":["AAPL"]}""")
    end
    c = Historical("test-key"; gateway = "https://hist.test", dispatcher = mock)
    result = resolve(c;
        dataset    = "XNAS.ITCH",
        symbols    = ["AAPL", "MSFT"],
        stype_in   = SType.RAW_SYMBOL,
        stype_out  = SType.INSTRUMENT_ID,
        start_date = "2024-01-02")
    @test "AAPL" in result["symbols"]
    @test occursin("symbols=AAPL%2CMSFT", captured[]) || occursin("symbols=AAPL,MSFT", captured[])
    @test occursin("stype_in=raw_symbol", captured[])
end
