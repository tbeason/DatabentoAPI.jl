# Live symbol resolution.
#
# Live data records carry only the opaque numeric `hd.instrument_id`. The gateway
# announces the human-readable symbol for each id out-of-band via
# `SymbolMappingMsg` records (a burst at subscription start, then occasional
# updates — e.g. a continuous/parent symbol rolling to a new contract). This
# layer keeps a running `instrument_id → SymbolMappingMsg` map on the `Live`
# client so consumers can resolve a record's symbol without manually tracking
# those control records.
#
# This is the live counterpart to the historical/DBN side, where the mapping is
# read once from `Metadata.mappings`. The difference: the historical map is
# interval-keyed (an id is reused across a date range, so `symbol_for` there
# takes a timestamp), whereas the live map is *current state* — it reflects the
# latest mapping seen, so the live `symbol_for` needs no timestamp.

# Called by both reader loops on every control record; updates the map (and
# fires callbacks) only for SymbolMappingMsg. Invoked before the typed reader's
# control-channel drop logic, so the map is captured even if control_channel is
# full and the record itself is dropped.
@inline function _record_symbol!(c::Live, rec)
    rec isa DBN.SymbolMappingMsg || return nothing
    iid = rec.hd.instrument_id
    @lock c.symbols_lock c.symbols_by_id[iid] = rec
    _fire_symbol_callbacks(c, iid, rec)
    return nothing
end

# Snapshot callbacks under the lock, then fire outside it so a slow callback
# can't block the reader (mirrors _fire_reconnect_callbacks).
function _fire_symbol_callbacks(c::Live, iid::UInt32, rec::DBN.SymbolMappingMsg)
    isempty(c.symbol_callbacks) && return nothing
    cbs = @lock c.symbols_lock copy(c.symbol_callbacks)
    for cb in cbs
        try
            cb(iid, rec)
        catch e
            @error "symbol-mapping callback raised" dataset = c.dataset exception = (e, catch_backtrace())
        end
    end
    return nothing
end

# Pull the requested symbol string out of a SymbolMappingMsg.
@inline function _symbol_field(msg::DBN.SymbolMappingMsg, which::Symbol)
    which === :in && return msg.stype_in_symbol
    which === :out && return msg.stype_out_symbol
    throw(ArgumentError("`which` must be :in or :out, got $(repr(which))"))
end

"""
    symbol_for(client::Live, instrument_id; which=:in) -> Union{String,Nothing}
    symbol_for(client::Live, record;        which=:in) -> Union{String,Nothing}

Resolve the human-readable symbol the gateway last mapped to `instrument_id`
(or to `record.hd.instrument_id`), or `nothing` if no `SymbolMappingMsg` has been
seen for that id yet. The map is maintained automatically by the reader as
mappings stream in, so this works in both typed and untyped mode and without
draining `control_channel`.

`which` selects which side of the mapping to return:
- `:in` (default) — the input symbol you subscribed with (e.g. `"AAPL"` for a
  raw-symbol subscription, or the parent/continuous symbol such as `"ES.FUT"`).
- `:out` — the resolved output symbol (`stype_out_symbol`).

See also [`symbol_map`](@ref) for a full snapshot and
[`add_symbol_mapping_callback`](@ref) for push-style updates.
"""
function symbol_for(c::Live, instrument_id::Integer; which::Symbol = :in)
    msg = @lock c.symbols_lock get(c.symbols_by_id, UInt32(instrument_id), nothing)
    return msg === nothing ? nothing : _symbol_field(msg, which)
end

symbol_for(c::Live, rec; which::Symbol = :in) =
    symbol_for(c, rec.hd.instrument_id; which = which)

"""
    symbol_map(client::Live; which=:in) -> Dict{UInt32,String}

Return a thread-safe snapshot of the current `instrument_id → symbol` map for a
live `client`, built from the `SymbolMappingMsg` records seen so far. `which`
(`:in` default, or `:out`) selects which side of each mapping to use — see
[`symbol_for`](@ref).

The returned `Dict` is a copy; mutating it does not affect the client, and the
client may add entries after the snapshot is taken.
"""
function symbol_map(c::Live; which::Symbol = :in)
    out = Dict{UInt32,String}()
    @lock c.symbols_lock begin
        sizehint!(out, length(c.symbols_by_id))
        for (iid, msg) in c.symbols_by_id
            out[iid] = _symbol_field(msg, which)
        end
    end
    return out
end

"""
    add_symbol_mapping_callback(client::Live, cb)

Register `cb(instrument_id::UInt32, msg::SymbolMappingMsg)` to be invoked each
time the gateway maps (or remaps) an instrument id — useful for reacting to
continuous/parent symbol rolls or persisting the mapping. Callbacks fire from
the reader task; they are snapshotted under a lock and run outside it, and an
exception in a callback is logged and swallowed (it won't disrupt the reader or
other callbacks). The map itself is updated before callbacks fire, so
[`symbol_for`](@ref) already reflects the new mapping inside the callback.
"""
function add_symbol_mapping_callback(c::Live, cb)
    cb isa Function || cb isa Base.Callable || throw(ArgumentError(
        "symbol-mapping callback must be callable, got $(typeof(cb))"))
    @lock c.symbols_lock push!(c.symbol_callbacks, cb)
    return nothing
end
