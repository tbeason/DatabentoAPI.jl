# Authentication

```@meta
CurrentModule = DatabentoAPI
```

DatabentoAPI.jl resolves your API key from three sources, in order:

1. **Explicit keyword argument** to [`Historical`](@ref) or [`Live`](@ref):
   ```julia
   Historical("db-AAAAAAAA…")
   Live("db-AAAAAAAA…"; dataset = "GLBX.MDP3")
   ```
2. **Config file** at [`default_config_path()`](@ref) (`~/.databento/config.toml`):
   ```toml
   api_key = "db-AAAAAAAA…"
   ```
3. **Environment variable** `DATABENTO_API_KEY`:
   ```powershell
   $env:DATABENTO_API_KEY = "db-AAAAAAAA…"
   ```
   ```bash
   export DATABENTO_API_KEY=db-AAAAAAAA…
   ```

The first non-empty source wins. If none yield a key, both client constructors
throw [`BentoAuthError`](@ref).

## Inspecting / overriding the lookup

[`load_api_key`](@ref) is exported so you can pre-check what the package would
resolve, or feed your own value to it:

```julia
using DatabentoAPI

# What would the constructor use?
key = load_api_key()
# Or pass an explicit override (skips file + env var):
key = load_api_key("db-override")
```

[`default_config_path`](@ref) returns the platform-appropriate config location
(`~/.databento/config.toml` on Linux/macOS, `%USERPROFILE%\.databento\config.toml`
on Windows). Override the path with the `DATABENTO_CONFIG` env var if you want
to keep the file elsewhere.

## When auth fails

On the Historical API, the client surfaces a [`BentoAuthError`](@ref) immediately
on the first request. On the Live API, [`connect!`](@ref) performs a CRAM-MD5
handshake and raises [`BentoAuthError`](@ref) if the gateway rejects the
challenge response. The error's `msg` field includes whatever detail the
gateway sent back — usually "invalid api_key" or "key not authorized for dataset".
