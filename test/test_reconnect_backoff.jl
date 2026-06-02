using Test
using Sockets
using DatabentoAPI
using DatabentoAPI: _reconnect_delay, _RECONNECT_BASE_S, _RECONNECT_CAP_S
using DatabentoBinaryEncoding
import DatabentoBinaryEncoding as DBN

const _BACKOFF_TEST_KEY = "db-0123456789abcdef0123456789abcdef"

@testset "reconnect — _reconnect_delay full-jitter bounds" begin
    # attempt=1 → upper = base
    for _ in 1:200
        d = _reconnect_delay(1)
        @test 0 ≤ d < _RECONNECT_BASE_S
    end
    # attempt=2 → upper = 2*base
    for _ in 1:200
        d = _reconnect_delay(2)
        @test 0 ≤ d < 2 * _RECONNECT_BASE_S
    end
    # attempt=3 → upper = 4*base
    for _ in 1:200
        d = _reconnect_delay(3)
        @test 0 ≤ d < 4 * _RECONNECT_BASE_S
    end
    # Cap kicks in once base * 2^(n-1) > cap.
    # With base=1, cap=60: 2^(n-1) > 60 ⇔ n ≥ 7
    for n in 7:20
        for _ in 1:50
            d = _reconnect_delay(n)
            @test 0 ≤ d < _RECONNECT_CAP_S
        end
    end
    # attempt < 1 → 0
    @test _reconnect_delay(0) == 0.0
    @test _reconnect_delay(-3) == 0.0

    # Custom base/cap kwargs
    d_custom = _reconnect_delay(1; base = 5.0, cap = 100.0)
    @test 0 ≤ d_custom < 5.0
    d_capped = _reconnect_delay(20; base = 1.0, cap = 2.0)
    @test 0 ≤ d_capped < 2.0
end

@testset "reconnect — jitter actually varies between calls" begin
    # 50 draws at attempt=5 should produce many distinct values; if they're
    # all identical the RNG isn't being consulted (regression guard).
    draws = [_reconnect_delay(5) for _ in 1:50]
    @test length(unique(draws)) > 10
end

@testset "reconnect — immediate phase returns exactly 0.0" begin
    # First `immediate` attempts must short-circuit to 0.0 so transient TCP
    # blips recover without the 1s+ backoff floor.
    for n in 1:3
        @test _reconnect_delay(n; immediate = 3) === 0.0
    end
    # attempt = immediate + 1 enters backoff, drawn from [0, base).
    for _ in 1:50
        d = _reconnect_delay(4; immediate = 3)
        @test 0 ≤ d < _RECONNECT_BASE_S
    end
    # attempt = immediate + 2 → [0, 2*base).
    for _ in 1:50
        d = _reconnect_delay(5; immediate = 3)
        @test 0 ≤ d < 2 * _RECONNECT_BASE_S
    end
    # immediate = 0 (default) leaves prior behaviour unchanged: attempt=1
    # samples [0, base) just like the no-kwarg form.
    for _ in 1:50
        d = _reconnect_delay(1; immediate = 0)
        @test 0 ≤ d < _RECONNECT_BASE_S
    end
    # Large immediate suppresses backoff for any attempt within its window.
    for n in 1:10
        @test _reconnect_delay(n; immediate = 100) === 0.0
    end
end

@testset "reconnect — max_reconnect_attempts caps connection attempts" begin
    # Mock gateway that immediately closes each socket. The client will fail
    # during the LSG-version read, raise, and fall through to the reconnect
    # path. With max_reconnect_attempts=2 we expect exactly 3 accepts
    # (1 initial + 2 reconnects), then the loop exits.
    accept_count = Threads.Atomic{Int}(0)
    # Pre-stage more scripts than we expect to use; extras simply never get
    # accepted and the server gets closed in finally.
    scripts = [function(sock)
        Threads.atomic_add!(accept_count, 1)
        # Close immediately — client's first read (LSG version frame) gets EOF.
        return
    end for _ in 1:8]
    mock = spawn_mock_gateway_sequence(scripts)

    t0 = time()
    mktempdir() do dir
        DatabentoAPI.stream_to_file(
            schema = DBN.Schema.TRADES, symbols = ["AAPL"],
            dataset = "TEST.MOCK", stype_in = DBN.SType.RAW_SYMBOL,
            base_dir = dir, compress = false,
            duration_s = 30,                  # generous; the cap should trip first
            reconnect = true,
            max_reconnect_attempts = 2,
            key = _BACKOFF_TEST_KEY,
            gateway = "127.0.0.1", port = mock.port,
            wire_compression = DBN.Compression.NONE,
            heartbeat_log_interval_s = 5.0,
        )
    end
    elapsed = time() - t0

    # 1 initial + 2 reconnects = 3 accepts.
    @test accept_count[] == 3
    # Worst-case delay budget: attempt-1 (≤1s) + attempt-2 (≤2s) = ≤3s plus
    # a few seconds of handshake / cleanup slack. Anything in single digits
    # means the cap engaged; without the cap this would run for duration_s=30.
    @test elapsed < 15.0

    # Server cleanup
    try; close(mock.server); catch; end
end

@testset "reconnect — duration_s cuts off mid-backoff sleep" begin
    # Regression guard for Codex P1: with full-jitter backoff a single retry
    # can sleep up to 60s. The reconnect loop must remain responsive to the
    # deadline so `duration_s` is honored even if the deadline is crossed
    # while a backoff sleep is in flight.
    #
    # We force the loop into the high-backoff regime by setting a small base
    # and a large cap via the existing `_reconnect_delay` defaults, then check
    # that the call returns close to `duration_s` regardless of how many
    # attempts have stacked up.
    accept_count = Threads.Atomic{Int}(0)
    scripts = [function(sock)
        Threads.atomic_add!(accept_count, 1)
        return
    end for _ in 1:50]
    mock = spawn_mock_gateway_sequence(scripts)

    duration = 1.5
    t0 = time()
    mktempdir() do dir
        DatabentoAPI.stream_to_file(
            schema = DBN.Schema.TRADES, symbols = ["AAPL"],
            dataset = "TEST.MOCK", stype_in = DBN.SType.RAW_SYMBOL,
            base_dir = dir, compress = false,
            duration_s = duration,
            reconnect = true,
            max_reconnect_attempts = nothing,   # let cap-by-deadline drive exit
            key = _BACKOFF_TEST_KEY,
            gateway = "127.0.0.1", port = mock.port,
            wire_compression = DBN.Compression.NONE,
            heartbeat_log_interval_s = 5.0,
        )
    end
    elapsed = time() - t0

    # The call must return promptly after the deadline: `_wait_for_reconnect`
    # slices its sleep so the loop never sits through a full backoff window
    # (up to `_RECONNECT_CAP_S` = 60s) when the deadline lands mid-sleep. We
    # assert a generous ceiling rather than a tight `duration + ε` — on shared
    # macOS / Windows CI runners, per-attempt socket churn and scheduler stalls
    # add several seconds of wall-clock unrelated to backoff responsiveness
    # (the old `duration + 3.0` bound flaked here, observed 5.0s). Anything in
    # the low-double-digits proves the in-flight sleep was cut short; a
    # regression that ignored the deadline would run for tens of seconds.
    @test elapsed < 15.0
    try; close(mock.server); catch; end
end

@testset "reconnect — reconnect=false bypasses cap entirely" begin
    # When reconnect is disabled, the first accepted connection is the only one
    # regardless of max_reconnect_attempts. Verifies kwargs don't interact
    # unexpectedly.
    accept_count = Threads.Atomic{Int}(0)
    scripts = [function(sock)
        Threads.atomic_add!(accept_count, 1)
        return
    end for _ in 1:5]
    mock = spawn_mock_gateway_sequence(scripts)

    mktempdir() do dir
        DatabentoAPI.stream_to_file(
            schema = DBN.Schema.TRADES, symbols = ["AAPL"],
            dataset = "TEST.MOCK", stype_in = DBN.SType.RAW_SYMBOL,
            base_dir = dir, compress = false,
            duration_s = 30,
            reconnect = false,
            max_reconnect_attempts = 10,      # ignored when reconnect=false
            key = _BACKOFF_TEST_KEY,
            gateway = "127.0.0.1", port = mock.port,
            wire_compression = DBN.Compression.NONE,
            heartbeat_log_interval_s = 5.0,
        )
    end

    @test accept_count[] == 1
    try; close(mock.server); catch; end
end
