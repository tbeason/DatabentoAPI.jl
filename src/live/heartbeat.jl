# Heartbeat handling.
#
# Databento gateways send heartbeats to clients as `SystemMsg` records with
# `is_heartbeat()` true (per databento-python). Our reader simply forwards them as
# regular records — consumers can dispatch on `DBN.SystemMsg` to ignore or log them.
#
# The client-supplied `heartbeat_interval` is forwarded to the gateway in the
# `AuthenticationRequest` (see `connect!`); no separate keepalive task is required
# in v0.1.

"""
    is_heartbeat(rec) -> Bool

Return `true` if `rec` is a gateway heartbeat (`SystemMsg` with code 23 / heartbeat).
For other record types, returns `false`.
"""
function is_heartbeat(rec)
    rec isa DBN.SystemMsg || return false
    code_field = try; rec.code catch; nothing end
    code_field === nothing && return false
    return String(code_field) == "23" || code_field == 23 || lowercase(String(code_field)) == "heartbeat"
end
