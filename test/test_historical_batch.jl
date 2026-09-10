using Test
using DatabentoAPI
using HTTP

@testset "historical batch" begin
    @testset "submit_job posts form body and parses response" begin
        captured = Ref{String}("")
        function mock(method, url, headers, body; kwargs...)
            @test method == "POST"
            @test occursin("batch.submit_job", url)
            captured[] = String(copy(body))
            HTTP.Response(200; body = """{"id":"job-123","state":"received"}""")
        end
        c = Historical("test-key"; gateway = "https://hist.test", dispatcher = mock)
        result = submit_job(c;
            dataset = "XNAS.ITCH",
            symbols = ["AAPL"],
            schema  = Schema.TRADES,
            start_dt = "2024-01-02",
            end_dt   = "2024-01-03")
        @test result["id"] == "job-123"
        # Body uses "end" not "end_"
        @test occursin("end=", captured[])
        @test !occursin("end_=", captured[])
        @test occursin("schema=trades", captured[])
    end

    @testset "list_jobs joins states vector" begin
        function mock(method, url, headers, body; kwargs...)
            qpairs = get(kwargs, :query, [])
            d = Dict(qpairs)
            @test d["states"] == "queued,processing"
            HTTP.Response(200; body = "[]")
        end
        c = Historical("test-key"; gateway = "https://hist.test", dispatcher = mock)
        list_jobs(c; states = [JobState.QUEUED, JobState.PROCESSING])
    end

    @testset "list_files returns parsed array" begin
        function mock(method, url, headers, body; kwargs...)
            HTTP.Response(200; body = """[{"filename":"foo.dbn.zst","size":1024}]""")
        end
        c = Historical("test-key"; gateway = "https://hist.test", dispatcher = mock)
        result = list_files(c; job_id = "j-1")
        @test result[1]["filename"] == "foo.dbn.zst"
    end

    @testset "download writes files to output_dir" begin
        using SHA
        # Synthesize two file bodies with sha256 hashes; list_files returns urls
        # pointing at api.databento.com, and batch_download fetches those URLs
        # directly (rather than going through /v0/batch.download).
        body_a = Vector{UInt8}("alpha-content")
        body_b = Vector{UInt8}("beta-content-longer")
        hash_a = "sha256:" * bytes2hex(sha256(body_a))
        hash_b = "sha256:" * bytes2hex(sha256(body_b))
        url_a  = "https://api.databento.com/v0/batch/download/U/j-1/a.bin"
        url_b  = "https://api.databento.com/v0/batch/download/U/j-1/b.bin"
        files_json = """
        [{"filename":"a.bin","size":$(length(body_a)),"hash":"$(hash_a)",
          "urls":{"https":"$(url_a)"}},
         {"filename":"b.bin","size":$(length(body_b)),"hash":"$(hash_b)",
          "urls":{"https":"$(url_b)"}}]
        """
        files_called = Ref(false)
        gets_called  = Ref(0)
        function mock(method, url, headers, body; kwargs...)
            if occursin("batch.list_files", url)
                files_called[] = true
                HTTP.Response(200; body = files_json)
            elseif url == url_a
                gets_called[] += 1
                HTTP.Response(200; body = body_a)
            elseif url == url_b
                gets_called[] += 1
                HTTP.Response(200; body = body_b)
            else
                HTTP.Response(500; body = "unexpected url $url")
            end
        end
        c = Historical("test-key"; gateway = "https://hist.test", dispatcher = mock)
        mktempdir() do dir
            paths = batch_download(c; job_id = "j-1", output_dir = dir)
            @test files_called[]
            @test gets_called[] == 2
            @test length(paths) == 2
            @test all(isfile, paths)
            @test read(joinpath(dir, "a.bin")) == body_a
            @test read(joinpath(dir, "b.bin")) == body_b
        end
        # Idempotent re-download: same size → skip
        mktempdir() do dir
            batch_download(c; job_id = "j-1", output_dir = dir)
            gets_called[] = 0
            batch_download(c; job_id = "j-1", output_dir = dir)
            @test gets_called[] == 0
        end
    end

    @testset "list_jobs short parameter (Aug 2026 batch API change)" begin
        seen = Ref(Dict{String,String}())
        function mock(method, url, headers, body; kwargs...)
            @test method == "GET"
            @test occursin("batch.list_jobs", url)
            seen[] = Dict{String,String}(get(kwargs, :query, Pair{String,String}[]))
            HTTP.Response(200; body = """[{"id":"GLBX-20220901-5DEFXVTMSM","state":"done","ts_received":"2022-09-01T00:00:00Z"}]""")
        end
        c = Historical("test-key"; gateway = "https://hist.test", dispatcher = mock)

        # Default: parameter omitted so the server default applies through the
        # phased rollout (full response now, condensed later).
        list_jobs(c)
        @test !haskey(seen[], "short")

        jobs = list_jobs(c; short = true)
        @test seen[]["short"] == "true"
        @test jobs[1]["id"] == "GLBX-20220901-5DEFXVTMSM"
        @test jobs[1]["state"] == "done"
        @test jobs[1]["ts_received"] == "2022-09-01T00:00:00Z"

        list_jobs(c; short = false, states = JobState.DONE)
        @test seen[]["short"] == "false"
        @test seen[]["states"] == "done"
    end

    @testset "get_job_details queries by job_id" begin
        function mock(method, url, headers, body; kwargs...)
            @test method == "GET"
            @test occursin("batch.get_job_details", url)
            d = Dict{String,String}(get(kwargs, :query, Pair{String,String}[]))
            @test d["job_id"] == "GLBX-20220901-5DEFXVTMSM"
            HTTP.Response(200; body = """{"id":"GLBX-20220901-5DEFXVTMSM","state":"done",
                "dataset":"GLBX.MDP3","schema":"mbo","symbols":"ESM2","stype_in":"raw_symbol",
                "record_count":12345,"billed_size":98765,"cost_usd":0.5,"progress":100,
                "ts_received":"2022-09-01T00:00:00Z","ts_expiration":"2022-10-01T00:00:00Z"}""")
        end
        c = Historical("test-key"; gateway = "https://hist.test", dispatcher = mock)
        job = get_job_details(c; job_id = "GLBX-20220901-5DEFXVTMSM")
        @test job["id"] == "GLBX-20220901-5DEFXVTMSM"
        @test job["state"] == "done"
        @test job["dataset"] == "GLBX.MDP3"
        @test job["record_count"] == 12345
        @test job["cost_usd"] == 0.5
    end
end
