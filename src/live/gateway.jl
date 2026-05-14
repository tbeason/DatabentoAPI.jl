const DEFAULT_LIVE_PORT = 13000

"""
    gateway_for_dataset(dataset)

Return the TCP hostname for the Databento Live gateway serving `dataset`.
Mirrors the Python client: lower-case the dataset and replace `.` with `-`, then
append `.lsg.databento.com` (e.g. `"GLBX.MDP3"` → `"glbx-mdp3.lsg.databento.com"`).
"""
function gateway_for_dataset(dataset::AbstractString)::String
    subdomain = replace(lowercase(String(dataset)), '.' => '-')
    return string(subdomain, ".lsg.databento.com")
end
