module BenchLiveStress

# Live-network stress bench against the real Databento OPRA Live gateway.
# Subscribes to a high-volume parent set on CMBP-1 with
# `slow_reader_behavior = WARN` so the gateway emits a code-7
# `SkippedRecordsAfterSlowReading` ErrorMsg every time it drops records
# because the reader fell behind. Counts those + tracks sustained rate,
# peak channel depth, GC fraction.
#
# NOTE: hits the real live gateway. Requires a valid API key in
# ~/.databento/config.toml or $DATABENTO_API_KEY. Counts toward your
# subscription.

using DatabentoAPI
using DBN
using Printf

const DEFAULT_PARENTS = [
    "SPX.OPT", "SPXW.OPT", "SPY.OPT", "QQQ.OPT", "VIX.OPT",
    "NDX.OPT", "NDXP.OPT", "NVDA.OPT", "TSLA.OPT", "MSTR.OPT",
]

function run(; duration_s::Real = 60.0,
              warmup_s::Real = 2.0,
              channel_size::Integer = 65_536,
              parents = DEFAULT_PARENTS,
              schema::Schema.T = Schema.CMBP_1,
              wire_zstd::Bool = true,
              typed::Bool = false)
    println("live stress: dataset=OPRA.PILLAR schema=", schema,
            "  parents=", length(parents), "  duration=", duration_s, "s",
            typed ? "  mode=TYPED" : "  mode=untyped")

    client = DatabentoAPI.Live(;
        dataset = "OPRA.PILLAR",
        compression = wire_zstd ? Compression.ZSTD : Compression.NONE,
        slow_reader_behavior = SlowReaderBehavior.WARN,
        channel_size = Int(channel_size),
        typed = typed)
    DatabentoAPI.connect!(client)
    println("  connected  session_id=", client.session_id)

    sub_ret = DatabentoAPI.subscribe!(client;
        schema = schema, symbols = parents, stype_in = SType.PARENT)
    DatabentoAPI.start!(client)
    println("  started")

    # In typed mode, drain the schema's typed data channel and the control
    # channel separately. In untyped mode, the single Union channel
    # carries both.
    data_ch = typed ? sub_ret : client.channel
    ctrl_consumer::Union{Nothing,Task} = nothing
    n_control_typed = Ref(0)
    n_smap_typed = Ref(0)
    n_system_typed = Ref(0)
    n_error_typed = Ref(0)
    err_samples_typed = String[]
    if typed
        ctrl = DatabentoAPI.control_channel(client)
        ctrl_consumer = @async begin
            try
                for rec in ctrl
                    n_control_typed[] += 1
                    if rec isa DBN.SymbolMappingMsg
                        n_smap_typed[] += 1
                    elseif rec isa DBN.SystemMsg
                        n_system_typed[] += 1
                    elseif rec isa DBN.ErrorMsg
                        n_error_typed[] += 1
                        length(err_samples_typed) < 5 && push!(err_samples_typed, String(rec.err))
                    end
                end
            catch e
                e isa InvalidStateException || rethrow()
            end
        end
    end

    # Wait for first record on the data channel.
    first_rec = take!(data_ch)
    println("  first record:  ", typeof(first_rec))

    # Warmup drain (excluded from rate calculation).
    n_warmup = 1
    warmup_until = time() + warmup_s
    while time() < warmup_until
        try
            _ = take!(data_ch); n_warmup += 1
        catch e
            e isa InvalidStateException && break
            rethrow()
        end
    end
    println("  warmup:  ", n_warmup, " records in ", warmup_s, " s  (",
            round(n_warmup / warmup_s / 1000, digits = 1), " k/s)")

    # ---- main measurement window ----
    n = 0
    n_data = 0
    n_symbol_mapping = 0
    n_system = 0
    n_error = 0
    n_skipped_err = 0   # code-7 SkippedRecordsAfterSlowReading
    skipped_records_total = Int64(0)
    err_samples = String[]   # first few errors verbatim
    max_depth = 0
    depth_samples = 0
    depth_sample_every = 4096
    log_every_s = 10.0
    last_log_t = time()
    last_log_n = 0

    GC.gc()
    g0 = Base.gc_num()
    t0 = time_ns()
    deadline = time() + duration_s

    try
        while time() < deadline
            rec = try
                take!(data_ch)
            catch e
                e isa InvalidStateException && break
                rethrow()
            end
            n += 1
            # Under typed mode, data_ch only carries data records (control
            # goes through ctrl_consumer above); under untyped mode we
            # classify here.
            if typed
                n_data += 1
            elseif rec isa DBN.ErrorMsg
                n_error += 1
                txt = String(rec.err)
                if hasproperty(rec, :code) && UInt8(rec.code) == 0x07
                    n_skipped_err += 1
                elseif occursin("skipped", lowercase(txt)) ||
                       occursin("slow", lowercase(txt))
                    n_skipped_err += 1
                end
                length(err_samples) < 5 && push!(err_samples, txt)
            elseif rec isa DBN.SystemMsg
                n_system += 1
            elseif rec isa DBN.SymbolMappingMsg
                n_symbol_mapping += 1
            else
                n_data += 1
            end

            if (n & (depth_sample_every - 1)) == 0
                d = Base.n_avail(data_ch)
                d > max_depth && (max_depth = d)
                depth_samples += 1
            end

            now = time()
            if now - last_log_t >= log_every_s
                el = now - last_log_t
                rate = (n - last_log_n) / el
                @printf("  +%4.0fs   recent=%6.0f k/s   max_depth=%d   err_skipped=%d\n",
                        now - (deadline - duration_s),
                        rate / 1000, max_depth, n_skipped_err)
                last_log_t = now; last_log_n = n
            end
        end
    finally
        elapsed = (time_ns() - t0) * 1e-9
        g1 = Base.gc_num()
        gc_frac = elapsed > 0 ? (g1.total_time - g0.total_time) * 1e-9 / elapsed : 0.0
        alloc_bytes = g1.allocd - g0.allocd

        try; DatabentoAPI.stop!(client); catch; end
        try; close(client); catch; end
        # Drain control consumer (typed mode).
        if ctrl_consumer !== nothing
            try; wait(ctrl_consumer); catch; end
            # Merge typed control counts back in for the summary.
            n_symbol_mapping = n_smap_typed[]
            n_system        = n_system_typed[]
            n_error         = n_error_typed[]
            for s in err_samples_typed
                length(err_samples) < 5 && push!(err_samples, s)
            end
        end

        println()
        println("=== live stress summary ===")
        @printf("  symbols           %d parents (%s)\n",
                length(parents), join(parents, ","))
        @printf("  measurement       %.2f s\n", elapsed)
        @printf("  total records     %d  (%.0f k/s avg)\n",
                n, n / elapsed / 1000)
        @printf("    data            %d  (%.0f k/s)\n", n_data, n_data / elapsed / 1000)
        @printf("    symbol_mapping  %d\n", n_symbol_mapping)
        @printf("    system          %d\n", n_system)
        @printf("    error           %d  (of which %d look like SkippedRecords)\n",
                n_error, n_skipped_err)
        if !isempty(err_samples)
            println("  first error msgs:")
            for s in err_samples
                println("    - ", s)
            end
        end
        @printf("  channel           cap=%d  peak=%d (%.1f%%)\n",
                channel_size, max_depth, 100 * max_depth / channel_size)
        @printf("  GC                fraction=%.1f%%  alloc=%.1f MB (%.0f bytes/rec)\n",
                gc_frac * 100, alloc_bytes / 1024^2, alloc_bytes / max(n, 1))

        if n_skipped_err > 0
            @warn "Reader fell behind the gateway during this window — consumer can't keep up at the current arrival rate"
        elseif max_depth < channel_size ÷ 4
            @info "Reader kept up easily; channel never approached capacity"
        else
            @info "Reader kept up but channel saw bursts; would tighten under heavier load"
        end

        return (; n, n_data, n_skipped_err, max_depth, gc_frac, alloc_bytes,
                  duration = elapsed, rate_per_s = n / elapsed)
    end
end

end # module BenchLiveStress

if abspath(PROGRAM_FILE) == @__FILE__
    BenchLiveStress.run()
end
