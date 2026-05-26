using Documenter
using DatabentoAPI

makedocs(
    sitename = "DatabentoAPI.jl",
    format = Documenter.HTML(
        prettyurls = get(ENV, "CI", nothing) == "true",
        canonical  = "https://tbeason.github.io/DatabentoAPI.jl",
        assets     = String[],
    ),
    modules = [DatabentoAPI],
    pages = [
        "Home"          => "index.md",
        "Installation"  => "installation.md",
        "Quick Start"   => "quickstart.md",
        "User Guide"    => [
            "Authentication"    => "guide/authentication.md",
            "Historical Data"   => "guide/historical.md",
            "Live Streaming"    => "guide/live.md",
            "Capture to File"   => "guide/capture.md",
            "Format Conversion" => "guide/conversion.md",
        ],
        "API Reference" => [
            "Authentication" => "api/authentication.md",
            "Historical"     => "api/historical.md",
            "Live"           => "api/live.md",
            "Capture"        => "api/capture.md",
            "Conversion"     => "api/conversion.md",
            "Errors"         => "api/errors.md",
            "Enums"          => "api/enums.md",
        ],
        "Performance"     => "performance.md",
        "Troubleshooting" => "troubleshooting.md",
    ],
    checkdocs = :none,
)

deploydocs(
    repo         = "github.com/tbeason/DatabentoAPI.jl.git",
    devbranch    = "main",
    push_preview = true,
)
