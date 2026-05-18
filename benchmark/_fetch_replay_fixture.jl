# Helper to fetch a real OPRA CMBP-1 archive via `submit_job` + `batch_download`
# for use as a replay-bench fixture. Costs nothing under a standard OPRA
# subscription (CMBP-1 quotes data within the last 12 mo is included).
#
# Usage (defaults work):
#   julia --project=benchmark benchmark/_fetch_replay_fixture.jl
# Or override the window:
#   julia --project=benchmark benchmark/_fetch_replay_fixture.jl \
#       --symbols SPY.OPT \
#       --start   2026-05-13T13:30:00 \
#       --end     2026-05-13T13:31:00
#
# Cached at benchmark/data/replay_*.dbn.zst. Re-running with the same args
# returns the existing path without re-downloading.

using Pkg
Pkg.activate(@__DIR__)

using DatabentoAPI
using JSON3
using Dates

function _parse_args(args)
    out = Dict{String,String}(
        "symbols" => "SPY.OPT",
        "start"   => "2026-05-13T13:30:00",
        "end"     => "2026-05-13T13:31:00",
    )
    i = 1
    while i <= length(args)
        a = args[i]
        if startswith(a, "--")
            k = a[3:end]
            i + 1 <= length(args) || error("missing value for $a")
            out[k] = args[i+1]; i += 2
        else
            i += 1
        end
    end
    return out
end

function _data_dir()
    d = joinpath(@__DIR__, "data")
    mkpath(d); return d
end

function _cache_path(opts)
    safe_syms = replace(opts["symbols"], r"[^A-Za-z0-9._-]" => "_")
    safe_start = replace(opts["start"], ':' => '_', 'T' => '_')
    safe_end   = replace(opts["end"], ':' => '_', 'T' => '_')
    name = "replay_$(safe_syms)_$(safe_start)__$(safe_end).dbn.zst"
    return joinpath(_data_dir(), name)
end

function wait_for_job(c::Historical, job_id::AbstractString;
                     poll_seconds::Real = 10.0,
                     deadline_seconds::Real = 1800.0)
    t0 = time()
    while true
        jobs = DatabentoAPI.list_jobs(c)
        for j in jobs
            if String(j["id"]) == job_id
                s = String(j["state"])
                rc = get(j, "record_count", nothing)
                as = get(j, "actual_size", nothing)
                println("  state=", s, "  record_count=", rc, "  actual_size=", as)
                if s == "done"
                    return j
                elseif s in ("expired", "failed")
                    error("Batch job $job_id ended in state '$s'")
                end
            end
        end
        time() - t0 > deadline_seconds &&
            error("Job $job_id did not complete within $(deadline_seconds)s")
        sleep(poll_seconds)
    end
end

function main(args = ARGS)
    opts = _parse_args(args)
    cache = _cache_path(opts)
    if isfile(cache) && filesize(cache) > 0
        println("Cached: ", cache, " (", round(filesize(cache) / (1024*1024), digits = 1), " MB)")
        return cache
    end

    c = Historical()

    println("Submitting batch job:")
    println("  dataset = OPRA.PILLAR")
    println("  schema  = cmbp-1")
    println("  symbols = ", opts["symbols"], " (parent)")
    println("  window  = ", opts["start"], " — ", opts["end"])
    job = DatabentoAPI.submit_job(c;
        dataset = "OPRA.PILLAR",
        schema = Schema.CMBP_1,
        symbols = split(opts["symbols"], ','),
        stype_in = SType.PARENT,
        start = opts["start"],
        end_  = opts["end"],
        encoding = "dbn", compression = "zstd")
    job_id = String(job["id"])
    println("  job_id  = ", job_id, "   cost_usd = ", job["cost_usd"])

    println("Polling…")
    final = wait_for_job(c, job_id)

    println("Downloading…")
    files = DatabentoAPI.batch_download(c; job_id = job_id, output_dir = _data_dir())
    isempty(files) && error("No files returned by batch_download")

    # Concatenate / rename to a stable cache path. Typical single-day, single-
    # parent jobs return one file; if it's already a .dbn.zst we just rename.
    if length(files) == 1 && endswith(lowercase(files[1]), ".dbn.zst")
        src = files[1]
        if src != cache
            mv(src, cache; force = true)
        end
    else
        # Multiple files (shouldn't happen for our small windows). Take the first
        # .dbn.zst and warn.
        idx = findfirst(f -> endswith(lowercase(f), ".dbn.zst"), files)
        idx === nothing && error("No .dbn.zst in $files")
        if files[idx] != cache
            mv(files[idx], cache; force = true)
        end
        @warn "Job produced multiple files; using only the first .dbn.zst" all_files=files used=files[idx]
    end

    println("Cached at: ", cache, " (", round(filesize(cache) / (1024*1024), digits = 1), " MB)")
    return cache
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
