# Live symbol resolution: the running instrument_id → symbol map maintained by
# the reader, plus symbol_for / symbol_map / add_symbol_mapping_callback.
#
# These exercise the map directly via the reader hook `_record_symbol!` (no TCP
# needed) — the reader-loop integration is covered by the live reader tests.

@testset "Live symbol resolution" begin
    _smm(iid, insym, outsym) = DBN.SymbolMappingMsg(
        DBN.RecordHeader(UInt8(0), DBN.RType.SYMBOL_MAPPING_MSG, UInt16(1), UInt32(iid), Int64(0)),
        DBN.SType.PARENT, insym, DBN.SType.INSTRUMENT_ID, outsym, Int64(0), Int64(0))

    _trade(iid) = DBN.TradeMsg(
        DBN.RecordHeader(UInt8(0), DBN.RType.MBP_0_MSG, UInt16(1), UInt32(iid), Int64(0)),
        Int64(1), UInt32(1), DBN.Action.TRADE, DBN.Side.BID, 0x00, 0x00, Int64(0), Int32(0), UInt32(1))

    newclient() = Live("db-1234567890abcdef12345"; dataset = "GLBX.MDP3")

    @testset "empty map" begin
        c = newclient()
        @test symbol_for(c, 42) === nothing
        @test isempty(symbol_map(c))
    end

    @testset "record + resolve (in/out)" begin
        c = newclient()
        DatabentoAPI._record_symbol!(c, _smm(42, "ES.FUT", "ESH4"))
        DatabentoAPI._record_symbol!(c, _smm(99, "AAPL", "12345"))
        # non-mapping records are a no-op
        DatabentoAPI._record_symbol!(c, _trade(7))

        @test symbol_for(c, 42) == "ES.FUT"            # :in default
        @test symbol_for(c, 42; which = :out) == "ESH4"
        @test symbol_for(c, 99) == "AAPL"
        @test symbol_for(c, 7) === nothing             # data record didn't register
        @test symbol_for(c, 123) === nothing           # unknown id

        # record overload resolves via hd.instrument_id
        @test symbol_for(c, _trade(42)) == "ES.FUT"

        @test symbol_map(c) == Dict(UInt32(42) => "ES.FUT", UInt32(99) => "AAPL")
        @test symbol_map(c; which = :out) == Dict(UInt32(42) => "ESH4", UInt32(99) => "12345")

        @test_throws ArgumentError symbol_for(c, 42; which = :bogus)
    end

    @testset "remap overwrites (e.g. continuous roll)" begin
        c = newclient()
        DatabentoAPI._record_symbol!(c, _smm(42, "ES.c.0", "ESH4"))
        DatabentoAPI._record_symbol!(c, _smm(42, "ES.c.0", "ESM4"))   # rolled
        @test symbol_for(c, 42; which = :out) == "ESM4"
        @test length(symbol_map(c)) == 1
    end

    @testset "snapshot is a copy" begin
        c = newclient()
        DatabentoAPI._record_symbol!(c, _smm(1, "A", "1"))
        snap = symbol_map(c)
        DatabentoAPI._record_symbol!(c, _smm(2, "B", "2"))
        @test snap == Dict(UInt32(1) => "A")           # unaffected by later mapping
        @test length(symbol_map(c)) == 2
    end

    @testset "callbacks" begin
        c = newclient()
        seen = Tuple{UInt32,String}[]
        add_symbol_mapping_callback(c, (iid, msg) -> push!(seen, (iid, msg.stype_in_symbol)))
        # map is already updated when the callback fires
        add_symbol_mapping_callback(c, (iid, _) -> push!(seen, (iid, something(symbol_for(c, iid), "?"))))

        DatabentoAPI._record_symbol!(c, _smm(42, "ES.FUT", "ESH4"))
        @test (UInt32(42), "ES.FUT") in seen
        @test count(==((UInt32(42), "ES.FUT")), seen) == 2   # both callbacks saw it

        @test_throws ArgumentError add_symbol_mapping_callback(c, 123)
    end

    @testset "a raising callback is swallowed" begin
        c = newclient()
        add_symbol_mapping_callback(c, (_, _) -> error("boom"))
        ok = Ref(false)
        add_symbol_mapping_callback(c, (_, _) -> ok[] = true)
        # should not throw, and the second callback still runs
        @test (DatabentoAPI._record_symbol!(c, _smm(1, "A", "1")); true)
        @test ok[]
        @test symbol_for(c, 1) == "A"
    end
end
