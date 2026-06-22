# Live — API Reference

```@meta
CurrentModule = DatabentoAPI
```

## Client

```@docs
Live
```

## Lifecycle

```@docs
connect!
subscribe!
subscribe_callback
start!
stop!
```

## Channels (typed mode)

```@docs
channel
control_channel
```

## Convenience

```@docs
live_session
```

## Reconnect

```@docs
add_reconnect_callback
```

## Symbol resolution

Live records carry only the numeric `instrument_id`; the reader maintains a
running `instrument_id → symbol` map from the gateway's `SymbolMappingMsg`
records (in both typed and untyped mode, without needing to drain
`control_channel`).

```@docs
symbol_for(::Live, ::Integer)
symbol_map(::Live)
add_symbol_mapping_callback
```
