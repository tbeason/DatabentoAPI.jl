module BenchCommon

using BenchmarkTools
using Printf

const HERE        = @__DIR__
const RESULTS_DIR = joinpath(HERE, "results")

mkpath(RESULTS_DIR)

# Row appended to per-suite CSVs.
Base.@kwdef struct Row
    suite::String
    path::String
    size::String
    n_records::Int
    samples::Int
    min_s::Float64
    median_s::Float64
    alloc_bytes::Int
    gc_fraction::Float64
    records_per_sec::Float64
    mb_per_sec::Float64
end

"""
    write_row(row::Row)

Append `row` to `results/{suite}.csv`. Creates the file (with header) on first
write of the session.
"""
function write_row(r::Row)
    file = joinpath(RESULTS_DIR, r.suite * ".csv")
    new_file = !isfile(file)
    open(file, "a") do io
        if new_file
            println(io, "suite,path,size,n_records,samples,min_s,median_s,alloc_bytes,gc_fraction,records_per_sec,mb_per_sec")
        end
        Printf.@printf(io, "%s,%s,%s,%d,%d,%.6f,%.6f,%d,%.4f,%.2f,%.2f\n",
            r.suite, r.path, r.size, r.n_records, r.samples,
            r.min_s, r.median_s, r.alloc_bytes, r.gc_fraction,
            r.records_per_sec, r.mb_per_sec)
    end
    return file
end

"""
    print_row(r::Row)

One-line console summary.
"""
function print_row(r::Row)
    Printf.@printf("  %-32s %-7s  min=%7.2f ms  alloc=%6.2f MB  gc=%5.1f%%  %8.2f M rec/s  %7.1f MB/s\n",
        r.path, r.size,
        r.min_s * 1e3,
        r.alloc_bytes / (1024 * 1024),
        r.gc_fraction * 100,
        r.records_per_sec / 1e6,
        r.mb_per_sec)
end

"""
    bench_trial(name, f; samples=5, seconds=10) -> (min_s, median_s, alloc_bytes, gc_fraction)

Run `f()` under BenchmarkTools with budgeted samples + a hard time cap. Returns
min/median wall-clock seconds, allocated bytes (from minimum trial), and the GC
fraction (gctime/walltime from the minimum trial).
"""
function bench_trial(name::AbstractString, f; samples::Int = 5, seconds::Real = 10.0)
    GC.gc()
    f()  # warmup (drops compile cost)
    b = BenchmarkTools.@benchmarkable $f()
    BenchmarkTools.tune!(b)
    trial = BenchmarkTools.run(b; samples = samples, seconds = Float64(seconds), evals = 1, gctrial = true)
    mn = minimum(trial)
    md = median(trial)
    gcfrac = mn.time > 0 ? mn.gctime / mn.time : 0.0
    return mn.time * 1e-9, md.time * 1e-9, Int(mn.memory), gcfrac
end

"""
    bench_to_row(; suite, path, size, n_records, bytes_in=0, f, samples=5, seconds=10) -> Row

Time `f()`, build a Row, append to CSV, print summary. Returns the Row.
"""
function bench_to_row(; suite::AbstractString,
                       path::AbstractString,
                       size::AbstractString,
                       n_records::Integer,
                       bytes_in::Integer = 0,
                       f,
                       samples::Int = 5,
                       seconds::Real = 10.0)
    min_s, med_s, alloc, gcf = bench_trial(path, f; samples = samples, seconds = seconds)
    rps = n_records > 0 && min_s > 0 ? n_records / min_s : 0.0
    mbps = bytes_in > 0 && min_s > 0 ? (bytes_in / (1024 * 1024)) / min_s : 0.0
    row = Row(
        suite = String(suite), path = String(path), size = String(size),
        n_records = Int(n_records), samples = samples,
        min_s = min_s, median_s = med_s, alloc_bytes = alloc,
        gc_fraction = gcf,
        records_per_sec = rps, mb_per_sec = mbps,
    )
    write_row(row)
    print_row(row)
    return row
end

"""
    manual_time(f) -> (elapsed_s, alloc_bytes, gc_fraction)

For paths that don't fit BenchmarkTools' assumptions (mock TCP servers, file I/O
with side effects). Returns wall time, allocated bytes, and GC fraction for a
single invocation, measured via `Base.gc_num()` deltas.
"""
function manual_time(f)
    GC.gc()
    g0 = Base.gc_num()
    t0 = time_ns()
    f()
    t1 = time_ns()
    g1 = Base.gc_num()
    elapsed_ns = t1 - t0
    gc_ns = (g1.total_time - g0.total_time)
    allocd = g1.allocd - g0.allocd
    gcf = elapsed_ns > 0 ? gc_ns / elapsed_ns : 0.0
    return elapsed_ns * 1e-9, Int(allocd), gcf
end

"""
    manual_to_row(; suite, path, size, n_records, bytes_in=0, f, samples=3) -> Row

Run `f()` `samples` times via `manual_time`; report the minimum.
"""
function manual_to_row(; suite::AbstractString,
                        path::AbstractString,
                        size::AbstractString,
                        n_records::Integer,
                        bytes_in::Integer = 0,
                        f,
                        samples::Int = 3)
    f()  # warmup
    best_t = Inf; best_alloc = 0; best_gc = 0.0; med_t = 0.0
    times = Float64[]
    for _ in 1:samples
        t, a, g = manual_time(f)
        push!(times, t)
        if t < best_t
            best_t = t; best_alloc = a; best_gc = g
        end
    end
    sort!(times)
    med_t = times[cld(length(times), 2)]
    rps = n_records > 0 && best_t > 0 ? n_records / best_t : 0.0
    mbps = bytes_in > 0 && best_t > 0 ? (bytes_in / (1024 * 1024)) / best_t : 0.0
    row = Row(
        suite = String(suite), path = String(path), size = String(size),
        n_records = Int(n_records), samples = samples,
        min_s = best_t, median_s = med_t, alloc_bytes = best_alloc,
        gc_fraction = best_gc,
        records_per_sec = rps, mb_per_sec = mbps,
    )
    write_row(row)
    print_row(row)
    return row
end

end # module BenchCommon
