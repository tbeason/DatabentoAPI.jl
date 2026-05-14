"""
    default_config_path()

Cross-platform path to the Databento config file: `~/.databento/config.toml`.
The environment variable `DATABENTO_CONFIG_PATH` overrides this — useful for tests
and for users with non-standard config locations.
"""
function default_config_path()
    override = get(ENV, "DATABENTO_CONFIG_PATH", "")
    isempty(override) || return String(override)
    return joinpath(homedir(), ".databento", "config.toml")
end

"""
    read_config(path = default_config_path())

Read the Databento TOML config. Returns `nothing` if the file does not exist.
On Unix, emits `@warn` if the file is group- or world-readable.
"""
function read_config(path::AbstractString = default_config_path())
    isfile(path) || return nothing
    if Sys.isunix()
        try
            mode = stat(path).mode & 0o777
            if (mode & 0o077) != 0
                @warn "Databento config file is readable by other users; consider `chmod 600`" path mode=string(mode; base=8)
            end
        catch
        end
    end
    return TOML.parsefile(path)
end

"""
    load_api_key([explicit])

Resolve the Databento API key. Order of precedence:

1. `explicit` argument if non-`nothing` and non-empty.
2. `~/.databento/config.toml` `[auth] api_key = "..."`.
3. Environment variable `DATABENTO_API_KEY`.

Throws `BentoAuthError` if no key is found.
"""
function load_api_key(explicit::Union{Nothing,AbstractString} = nothing)::String
    if explicit !== nothing
        s = String(explicit)
        isempty(strip(s)) || return s
    end
    cfg = read_config()
    if cfg !== nothing
        auth = get(cfg, "auth", nothing)
        if auth isa AbstractDict
            k = get(auth, "api_key", nothing)
            if k isa AbstractString && !isempty(strip(k))
                return String(k)
            end
        end
    end
    env_key = get(ENV, "DATABENTO_API_KEY", "")
    isempty(strip(env_key)) || return env_key
    throw(BentoAuthError(
        "No Databento API key found. Either pass `key=` explicitly, " *
        "set ENV[\"DATABENTO_API_KEY\"], or create $(default_config_path()) " *
        "with:\n\n[auth]\napi_key = \"db-...\"\n"))
end
