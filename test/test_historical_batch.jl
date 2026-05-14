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
            start   = "2024-01-02",
            end_    = "2024-01-03")
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
        files_called = Ref(false)
        function mock(method, url, headers, body; kwargs...)
            if occursin("batch.list_files", url)
                files_called[] = true
                HTTP.Response(200;
                    body = """[{"filename":"a.bin","size":3},{"filename":"b.bin","size":3}]""")
            elseif occursin("batch.download", url)
                qpairs = get(kwargs, :query, [])
                fname = Dict(qpairs)["filename"]
                HTTP.Response(200; body = Vector{UInt8}(fname * "!"))
            else
                HTTP.Response(500; body = "unexpected url $url")
            end
        end
        c = Historical("test-key"; gateway = "https://hist.test", dispatcher = mock)
        mktempdir() do dir
            paths = batch_download(c; job_id = "j-1", output_dir = dir)
            @test files_called[]
            @test length(paths) == 2
            @test all(isfile, paths)
            @test read(joinpath(dir, "a.bin"), String) == "a.bin!"
        end
    end
end
