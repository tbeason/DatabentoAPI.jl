using Test
using DatabentoAPI
using DatabentoAPI: HTTPClient
using HTTP

# Build a Historical client that uses an inline mock dispatcher.
function _mock_historical(handler::Function)
    return Historical("test-key";
                      gateway = "https://hist.test",
                      dispatcher = handler)
end

@testset "historical metadata" begin
    @testset "list_publishers" begin
        c = _mock_historical((m, u, h, b; kw...) -> begin
            @test occursin("metadata.list_publishers", u)
            HTTP.Response(200; body = """[{"publisher_id":1,"dataset":"XNAS.ITCH"}]""")
        end)
        result = list_publishers(c)
        @test result[1]["publisher_id"] == 1
    end

    @testset "list_datasets" begin
        c = _mock_historical((m, u, h, b; kw...) -> begin
            HTTP.Response(200; body = """["XNAS.ITCH","GLBX.MDP3"]""")
        end)
        result = list_datasets(c)
        @test "XNAS.ITCH" in result
    end

    @testset "list_schemas requires dataset" begin
        c = _mock_historical((m, u, h, b; kw...) -> begin
            qpairs = get(kw, :query, [])
            @test any(p -> first(p) == "dataset" && last(p) == "XNAS.ITCH", qpairs)
            HTTP.Response(200; body = """["mbo","trades","mbp-1"]""")
        end)
        result = list_schemas(c; dataset = "XNAS.ITCH")
        @test "trades" in result
    end

    @testset "get_cost converts schema enum to wire string" begin
        c = _mock_historical((m, u, h, b; kw...) -> begin
            qpairs = get(kw, :query, [])
            d = Dict(qpairs)
            @test d["schema"] == "mbp-1"
            @test d["stype_in"] == "raw_symbol"
            @test haskey(d, "end")
            @test !haskey(d, "end_")
            HTTP.Response(200; body = """{"cost":1.23}""")
        end)
        result = get_cost(c;
            dataset = "XNAS.ITCH",
            symbols = ["AAPL"],
            schema = Schema.MBP_1,
            start_dt = "2024-01-02T14:30:00",
            end_dt   = "2024-01-02T14:31:00")
        @test result["cost"] == 1.23
    end
end
