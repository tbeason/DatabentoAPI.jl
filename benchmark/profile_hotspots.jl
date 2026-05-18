module ProfileHotspots

include("fixtures.jl"); using .Fixtures

using Profile
using DBN
using CodecZstd
using TranscodingStreams
using Printf

const HERE        = @__DIR__
const RESULTS_DIR = joinpath(HERE, "results")
mkpath(RESULTS_DIR)

# Run `f()` repeatedly for at least `seconds` while collecting Profile samples.
# Writes the formatted flat profile (top 30) to results/{label}.profile.txt and
# returns (elapsed_ns, samples, alloc_bytes, gc_fraction).
function profile_run(label::AbstractString, f; seconds::Real = 5.0)
    Profile.clear()
    Profile.init(n = 10_000_000, delay = 0.001)
    GC.gc()
    g0 = Base.gc_num()
    t0 = time_ns()
    Profile.@profile begin
        deadline = t0 + UInt64(seconds * 1e9)
        while time_ns() < deadline
            f()
        end
    end
    t1 = time_ns()
    g1 = Base.gc_num()
    elapsed_ns = t1 - t0
    gc_ns = g1.total_time - g0.total_time
    allocd = g1.allocd - g0.allocd
    gcf = elapsed_ns > 0 ? gc_ns / elapsed_ns : 0.0

    outpath = joinpath(RESULTS_DIR, label * ".profile.txt")
    open(outpath, "w") do io
        println(io, "# ", label)
        println(io, "# elapsed: ", elapsed_ns * 1e-9, " s")
        println(io, "# alloc:   ", allocd / (1024 * 1024), " MB")
        println(io, "# gc:      ", gcf * 100, " %")
        println(io)
        Profile.print(IOContext(io, :displaysize => (300, 200));
                      format = :flat,
                      sortedby = :count,
                      mincount = 20,
                      maxdepth = 25)
    end
    println("  wrote ", outpath)
    return elapsed_ns, allocd, gcf
end

function _read_dbn(file)
    DBN.read_dbn(file)
    nothing
end

function _read_dbn_typed(file)
    DBN.read_dbn_typed(file, DBN.TradeMsg)
    nothing
end

function _foreach_typed(file)
    n = Ref(0)
    DBN.foreach_record(file, DBN.TradeMsg) do _rec
        n[] += 1
    end
    nothing
end

function _foreach_generic(file)
    # DBN.foreach_record requires a concrete record type; drop down to the
    # underlying decoder loop to count records generically.
    bytes = read(file)
    io = IOBuffer(bytes)
    src = if endswith(file, ".zst")
        TranscodingStream(ZstdDecompressor(), io)
    else
        io
    end
    decoder = DBN.DBNDecoder(src)
    DBN.read_header!(decoder)
    n = 0
    while true
        rec = DBN.read_record(decoder)
        rec === nothing && break
        n += 1
    end
    nothing
end

function run(; tier::Symbol = :medium, seconds::Real = 5.0)
    println("\n=== profile_hotspots (tier=$tier, seconds=$seconds) ===")
    f_plain = Fixtures.trades_file(tier; zst = false)
    f_zst   = Fixtures.trades_file(tier; zst = true)

    # Warmup
    _read_dbn(f_plain)
    _foreach_typed(f_plain)

    profile_run("read_dbn_plain",   () -> _read_dbn(f_plain);     seconds = seconds)
    profile_run("read_dbn_zst",     () -> _read_dbn(f_zst);       seconds = seconds)
    profile_run("read_dbn_typed",   () -> _read_dbn_typed(f_zst); seconds = seconds)
    profile_run("foreach_typed",    () -> _foreach_typed(f_zst);  seconds = seconds)
    profile_run("foreach_generic",  () -> _foreach_generic(f_zst); seconds = seconds)
end

end # module ProfileHotspots

if abspath(PROGRAM_FILE) == @__FILE__
    ProfileHotspots.run()
end
