using Test
using DatabentoAPI
using DatabentoBinaryEncoding
import DatabentoBinaryEncoding as DBN
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

    @testset "to_dataframe symbol join" begin
        # _tiny_store's record: instrument_id=101, ts_event = 2023-11-14 (UTC),
        # so YYYYMMDD = 20231114. Add a mapping covering that date.
        s = _tiny_store()
        md = DBN.Metadata(
            s.metadata.version, s.metadata.dataset, s.metadata.schema,
            s.metadata.start_ts, s.metadata.end_ts, s.metadata.limit,
            s.metadata.stype_in, s.metadata.stype_out, s.metadata.ts_out,
            s.metadata.symbols, s.metadata.partial, s.metadata.not_found,
            [("AAPL", "101", 20231101, 20231201)],
        )
        store = DBNStore(md, s.records)

        # off by default: no :symbol column, same frame as before
        @test !hasproperty(to_dataframe(store), :symbol)

        # on: joins the raw symbol
        df = to_dataframe(store; symbols = true)
        @test df.symbol == ["AAPL"]

        # re-exported helpers are usable directly (e.g. for foreach_record)
        smap = symbol_map(md)
        @test symbol_for(smap, 101, store.records[1].hd.ts_event) == "AAPL"
        @test symbol_for(smap, 999, store.records[1].hd.ts_event) === nothing
    end
end
