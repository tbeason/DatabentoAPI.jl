# Helpers for spinning up an in-process TCP server that role-plays the Databento Live
# gateway. Tests pass a `script(client_socket, ctx)` function that drives both sides
# of the conversation.

using Sockets

"""
    with_mock_gateway(script::Function) -> result

Bind a server to `127.0.0.1` on a free port, accept one connection, and call
`script(server_side_socket)` for the test to drive the protocol. Yields the port to
the caller via the returned port number on the wrapping function. Use the helper
`spawn_mock_gateway(script)` instead for the common pattern.
"""
function spawn_mock_gateway(script::Function)
    server = Sockets.listen(Sockets.IPv4("127.0.0.1"), 0)
    port = Sockets.getsockname(server)[2]
    accept_task = @async begin
        try
            sock = Sockets.accept(server)
            try
                script(sock)
            finally
                isopen(sock) && Sockets.close(sock)
            end
        finally
            isopen(server) && Sockets.close(server)
        end
    end
    return (; port = Int(port), server, accept_task)
end

"""
    spawn_mock_gateway_sequence(scripts::Vector{<:Function})

Spawn a server that accepts one connection per script in turn (in order). Each
script runs against its own freshly-accepted socket. Useful for testing the
client's reconnect behaviour.
"""
function spawn_mock_gateway_sequence(scripts::Vector)
    server = Sockets.listen(Sockets.IPv4("127.0.0.1"), 0)
    port = Sockets.getsockname(server)[2]
    accept_task = @async begin
        try
            for script in scripts
                sock = Sockets.accept(server)
                try
                    script(sock)
                catch
                end
                try; isopen(sock) && Sockets.close(sock); catch; end
            end
        finally
            isopen(server) && Sockets.close(server)
        end
    end
    return (; port = Int(port), server, accept_task)
end
