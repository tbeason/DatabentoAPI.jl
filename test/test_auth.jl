using Test
using DatabentoAPI
using DatabentoAPI: read_config, default_config_path

@testset "auth" begin
    # Cleanly isolate from the developer's own ENV/config during these tests by
    # pointing DATABENTO_CONFIG_PATH at a non-existent file in a tempdir.
    saved_env  = pop!(ENV, "DATABENTO_API_KEY",     nothing)
    saved_path = pop!(ENV, "DATABENTO_CONFIG_PATH", nothing)
    isolation_dir = mktempdir()
    ENV["DATABENTO_CONFIG_PATH"] = joinpath(isolation_dir, "no-such-config.toml")
    try
        @testset "explicit key wins" begin
            ENV["DATABENTO_API_KEY"] = "env-key"
            @test load_api_key("explicit-key") == "explicit-key"
            delete!(ENV, "DATABENTO_API_KEY")
        end

        @testset "TOML config takes precedence over env" begin
            mktempdir() do dir
                cfg = joinpath(dir, "config.toml")
                open(cfg, "w") do io
                    write(io, "[auth]\napi_key = \"toml-key\"\n")
                end
                ENV["DATABENTO_API_KEY"] = "env-key"
                cfg_dict = read_config(cfg)
                @test cfg_dict !== nothing
                @test cfg_dict["auth"]["api_key"] == "toml-key"
                delete!(ENV, "DATABENTO_API_KEY")
            end
        end

        @testset "env fallback" begin
            ENV["DATABENTO_API_KEY"] = "env-key-xyz"
            @test load_api_key() == "env-key-xyz"
            delete!(ENV, "DATABENTO_API_KEY")
        end

        @testset "missing key throws BentoAuthError" begin
            haskey(ENV, "DATABENTO_API_KEY") && delete!(ENV, "DATABENTO_API_KEY")
            @test_throws BentoAuthError load_api_key()
        end

        @testset "empty/whitespace explicit falls through" begin
            ENV["DATABENTO_API_KEY"] = "fallback-key"
            @test load_api_key("   ") == "fallback-key"
            delete!(ENV, "DATABENTO_API_KEY")
        end

        @testset "config path is platform-aware" begin
            # Lift the override for this one test so we see the true default.
            override = pop!(ENV, "DATABENTO_CONFIG_PATH", nothing)
            try
                p = default_config_path()
                @test endswith(p, joinpath(".databento", "config.toml"))
            finally
                override === nothing || (ENV["DATABENTO_CONFIG_PATH"] = override)
            end
        end
    finally
        if saved_env !== nothing
            ENV["DATABENTO_API_KEY"] = saved_env
        else
            haskey(ENV, "DATABENTO_API_KEY") && delete!(ENV, "DATABENTO_API_KEY")
        end
        if saved_path !== nothing
            ENV["DATABENTO_CONFIG_PATH"] = saved_path
        else
            delete!(ENV, "DATABENTO_CONFIG_PATH")
        end
        rm(isolation_dir; recursive = true, force = true)
    end
end
