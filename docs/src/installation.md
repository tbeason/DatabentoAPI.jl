# Installation

DatabentoAPI.jl requires **Julia 1.12 or newer**.

## From the General registry

```julia
using Pkg
Pkg.add("DatabentoAPI")
```

This installs [`DatabentoBinaryEncoding`](https://github.com/tbeason/DatabentoBinaryEncoding.jl)
as a transitive dependency. You only need to `using DatabentoAPI` — the
sister package's enums and helpers are re-exported.

## Development checkout

If you want to hack on the package:

```julia
using Pkg
Pkg.develop(url = "https://github.com/tbeason/DatabentoAPI.jl")
# Optionally pair with a local DatabentoBinaryEncoding.jl checkout:
Pkg.develop(path = "/path/to/DatabentoBinaryEncoding.jl")
Pkg.instantiate()
Pkg.test("DatabentoAPI")
```

The offline test suite (1500+ tests, ~30s) uses in-process mock TCP gateways
and HTTP servers — no API key required. Live-network smoke tests are gated
behind `DATABENTO_LIVE_TESTS=1`; see [Troubleshooting](@ref) for details.

## API key

You'll need a Databento API key to actually fetch data. Sign up at
[databento.com](https://databento.com); the dashboard shows your key. See
[Authentication](guide/authentication.md) for how the package locates it.
