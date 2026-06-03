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
end
