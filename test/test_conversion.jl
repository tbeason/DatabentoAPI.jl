using Test
using DatabentoAPI
using DBN
using DataFrames

function _tiny_store()
    metadata = DBN.Metadata(
        DBN.DBN_VERSION, "XNAS.ITCH", DBN.Schema.TRADES,
        Int64(0), nothing, nothing, nothing,
        DBN.SType.INSTRUMENT_ID, false,
        ["AAPL"], String[], String[],
        Tuple{String,String,Int64,Int64}[],
    )
    hd = DBN.RecordHeader(
        UInt8(0), DBN.RType.MBP_0_MSG,
        UInt16(1), UInt32(101), Int64(1_700_000_000_000_000_000),
    )
    rec = DBN.TradeMsg(hd, Int64(150_000_000_000), UInt32(100),
                       DBN.Action.TRADE, DBN.Side.ASK,
                       UInt8(0), UInt8(1),
                       Int64(1_700_000_000_000_000_000), Int32(0), UInt32(1))
    return DBNStore(metadata, DBN.DBNRecord[rec])
end

@testset "conversion" begin
    @testset "to_dataframe returns a DataFrame" begin
        s = _tiny_store()
        df = to_dataframe(s)
        @test DataFrames.nrow(df) == 1
    end

    @testset "to_file round-trips through DBN.read_dbn" begin
        s = _tiny_store()
        mktempdir() do dir
            out = joinpath(dir, "out.dbn")
            to_file(s, out)
            @test isfile(out)
            recs = DBN.read_dbn(out)
            @test length(recs) == 1
        end
    end
end
