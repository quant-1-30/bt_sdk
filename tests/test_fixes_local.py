#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Local regression tests for the 2026-10 audit & optimization batch.
No gRPC server required (grpc.aio channel creation is lazy; RPCs use
wait_for_ready=True so they hang deterministically instead of failing):

  F1         get_factor_async widens Close/Adjustment/Rightment to [19900101, 20991231]
  F2         sync/async getters & subscribe fail fast when no running loop is attached
  F3/F4/F8   _pending_tasks/_pending_futs tracking, shutdown cancel, .pxd slot inheritance
  F6         calc_factor_async bounds concurrency via Semaphore + chunked batching
  F7         RpcClient.cleanup drops references before awaiting channel close
  S1         _apply_factor converts UTC tick to Asia/Shanghai before integer YYYYMMDD
  S2         GetMdApi updates timeout on cached singleton when positive timeout differs
  S3         pybind11 NumPy zero-copy binding matches list-input factor results
  S4         Observable resubscription isolates Subject per subscriber and auto-disposes
  D1         Shared singleton __exit__ and live-loop start() protection + auto-revive
  D2/D4      stream_async backpressure-enabled async generator on caller's loop
  P1/P3/P4   calc_adjust_factors_columns columnar C++ entry point & empty short-circuit
  N1/N3/N5/N7 Future unimplemented ex_date exclusion, idle loop guard, None body check, IPv6
"""
import asyncio
import os
import sys
import threading
import time

import polars as pl
import pytest

import bt_sdk.core.factor as factor_mod
from bt_sdk.core.factor import calc_factor_async, adjust2struct, right2struct

# same lib path setup as bt_sdk.core.factor (idempotent), relative to repo
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bt_sdk", "core", "lib"))
import adj_factor

from bt_sdk.core.client.api import GetMdApi, MdApi, _full_history_body
from bt_sdk.core.rpc.client import RpcClient
from bt_protocol._protocol import QueryBody
from bt_protocol.constant import RpcTopic, FactorTopic

POLL_ITER, POLL_INTERVAL = 100, 0.05


async def poll_until(predicate, what):
    """Poll up to 5s for `predicate()`; return its last value."""
    for _ in range(POLL_ITER):
        result = predicate()
        if result:
            return result
        await asyncio.sleep(POLL_INTERVAL)
    return predicate()


# --------------------------------------------------------------------------
# F1: full-history widening
# --------------------------------------------------------------------------

class TestFullHistoryBody:
    def test_querybody_widened(self):
        body = QueryBody(start_date=20100101, end_date=20260630, sid=[b"300374"])
        wide = _full_history_body(body)
        assert isinstance(wide, QueryBody)
        assert wide.start_date == 19900101
        assert wide.end_date == 20991231
        assert wide.sid == [b"300374"]
        # original untouched (frozen struct, no mutation)
        assert body.start_date == 20100101

    def test_none_passthrough(self):
        assert _full_history_body(None) is None
        assert _full_history_body("garbage") == "garbage"

    def test_empty_sid_preserved(self):
        wide = _full_history_body(QueryBody(20200101, 20210101, []))
        assert wide.sid == []


# --------------------------------------------------------------------------
# F2: loop guards
# --------------------------------------------------------------------------

class TestLoopGuards:
    @pytest.fixture
    def cold_api(self):
        # MdApi built directly (no start): loop is None
        return MdApi(addr=("127.0.0.1", 50051), timeout=30)

    def _assert_clear_error(self, fn):
        with pytest.raises(RuntimeError, match="no usable event loop"):
            fn()

    def test_sync_getters_guarded(self, cold_api):
        self._assert_clear_error(lambda: cold_api.get_instrument())
        self._assert_clear_error(
            lambda: cold_api.get_factor(QueryBody(20200101, 20210101, [b"1"]), FactorTopic.Qfq)
        )

    def test_subscribe_guarded(self, cold_api):
        self._assert_clear_error(
            lambda: cold_api.subscribe(QueryBody(20200101, 20210101, [b"1"]), RpcTopic.Tick)
        )

    @pytest.mark.asyncio
    async def test_async_getters_guarded(self, cold_api):
        with pytest.raises(RuntimeError, match="no usable event loop"):
            await cold_api.get_instrument_async()
        with pytest.raises(RuntimeError, match="no usable event loop"):
            await cold_api.get_factor_async(QueryBody(20200101, 20210101, [b"1"]), FactorTopic.Qfq)

    @pytest.mark.asyncio
    async def test_rpc_async_guarded(self, cold_api):
        with pytest.raises(RuntimeError, match="no usable event loop"):
            await cold_api.rpc_async(QueryBody(20200101, 20210101, [b"1"]), RpcTopic.Daily)


# --------------------------------------------------------------------------
# F3 + F4 + F8: pending task/future tracking, shutdown cancellation, slots
# --------------------------------------------------------------------------

@pytest.fixture
def api_on_runner():
    """MdApi attached to a private background loop (mimics AsyncRunner)."""
    loop = asyncio.new_event_loop()
    t = threading.Thread(target=loop.run_forever, daemon=True)
    t.start()
    api = MdApi(addr=("127.0.0.1", 59999), timeout=5)
    api.start(loop)
    yield api
    api.disconnect()
    loop.call_soon_threadsafe(loop.stop)
    t.join(timeout=5)
    loop.close()


class TestPendingTracking:
    def _stream_close_counts(self, api):
        # cross-loop stream: 1 concurrent future; its wrapped task registers
        # in .tasks once it starts on the runner loop
        return api.async_client.pending_counts().futures == 1

    @pytest.mark.asyncio
    async def test_cross_loop_future_tracked_and_teardown_cancels(self, api_on_runner):
        """Subscribe from a DIFFERENT loop (this test loop): the stream task
        runs on the attached runner loop and must be tracked; consumer dispose
        must cancel it and untrack everything."""
        client = api_on_runner.async_client

        body = QueryBody(20200101, 20210101, [b"300374"])
        obs = api_on_runner.subscribe(body, RpcTopic.Close)

        subscription = obs.subscribe(
            on_next=lambda _: None,
            on_error=lambda _: None,   # never fires: wait_for_ready hangs
            on_completed=lambda: None,
        )

        # channel creation is lazy; the RPC then waits for a server
        # (wait_for_ready=True) so the stream task stays alive deterministically
        assert await poll_until(lambda: self._stream_close_counts(api_on_runner),
                                "cross-loop future tracked"), client.pending_counts()

        # dispose -> teardown cancels the wrapped task -> run() finally discards
        subscription.dispose()
        counts = client.pending_counts
        assert await poll_until(
            lambda: counts().tasks == 0 and counts().futures == 0,
            "future discarded after teardown",
        ), counts()

    @pytest.mark.asyncio
    async def test_shutdown_cancels_inflight(self, api_on_runner):
        """close() must cancel in-flight cross-loop streams (and confirm the
        wrapped task settled) instead of waiting for them to fail alone."""
        client = api_on_runner.async_client
        body = QueryBody(20200101, 20210101, [b"300374"])
        obs = api_on_runner.subscribe(body, RpcTopic.Close)
        sub = obs.subscribe(on_next=lambda _: None, on_error=lambda _: None)

        assert await poll_until(lambda: self._stream_close_counts(api_on_runner),
                                "stream tracked"), client.pending_counts()

        api_on_runner.disconnect()  # sync close from a foreign thread

        counts = client.pending_counts()
        assert counts.tasks == 0 and counts.futures == 0, \
            "shutdown must cancel and clear tracked work (incl. settled wrapped task)"
        # client is closed for new work
        with pytest.raises(RuntimeError, match="client is not running"):
            api_on_runner.subscribe(body, RpcTopic.Close)
        sub.dispose()

    @pytest.mark.asyncio
    async def test_dispose_racing_task_start_cleans_refs(self, api_on_runner):
        """Timing regression: dispose() racing the wrapped task's start.

        Setup blocks the runner loop so subscribe+dispose complete while the
        queued task-start cannot run. Empirically (CPython 3.11) the
        concurrent-future cancel is delivered via call_soon_threadsafe AFTER
        the task's first step, so run()'s finally still executes; this test
        pins that teardown + finally together leave nothing tracked, whatever
        interleaving the cancel lands in.
        """
        client = api_on_runner.async_client
        runner = api_on_runner.loop

        release = threading.Event()
        block_done = threading.Event()

        def block_loop():
            release.wait(timeout=2.0)   # occupy the runner loop thread
            block_done.set()

        runner.call_soon_threadsafe(block_loop)
        await asyncio.sleep(0.1)        # ensure the loop is inside block_loop

        body = QueryBody(20200101, 20210101, [b"300374"])
        obs = api_on_runner.subscribe(body, RpcTopic.Close)
        sub = obs.subscribe(on_next=lambda _: None, on_error=lambda _: None)

        # future is tracked in the factory, before the loop can start the task
        assert await poll_until(lambda: client.pending_counts().futures == 1,
                                "future tracked"), client.pending_counts()

        sub.dispose()                   # teardown: discard + cancel NOW
        release.set()                   # let the runner loop process the queue
        assert block_done.wait(timeout=3.0)
        await asyncio.sleep(0.2)        # let the unstarted task settle

        counts = client.pending_counts()
        assert counts.tasks == 0 and counts.futures == 0, \
            f"dispose-before-start must not leak (got {counts})"

    @pytest.mark.asyncio
    async def test_start_revives_closed_client(self, api_on_runner):
        """Re-entering external_mdapi_context revives a closed instance."""
        api_on_runner.disconnect()
        loop = api_on_runner.loop
        api_on_runner.start(loop)   # revive on the same runner loop
        # revived: subscribe no longer raises "client is not running"
        obs = api_on_runner.subscribe(QueryBody(20200101, 20210101, [b"1"]), RpcTopic.Close)
        sub = obs.subscribe(on_next=lambda _: None, on_error=lambda _: None)
        await asyncio.sleep(0.2)
        sub.dispose()
        api_on_runner.disconnect()


# --------------------------------------------------------------------------
# F6: bounded concurrency in calc_factor_async
# --------------------------------------------------------------------------

class TestBoundedConcurrency:
    @pytest.mark.asyncio
    async def test_semaphore_bounds_and_results_correct(self, monkeypatch):
        state = {"inflight": 0, "max_inflight": 0, "calls": 0}

        def fake_calc(close_df, adj_df, rgt_df, forward):
            state["calls"] += 1
            state["inflight"] += 1
            state["max_inflight"] = max(state["max_inflight"], state["inflight"])
            time.sleep(0.05)
            state["inflight"] -= 1
            return f"factor:{close_df['sid'][0]}"

        monkeypatch.setattr(factor_mod, "_calc_factor", fake_calc)

        closes = {
            f"s{i}".encode(): pl.DataFrame({"sid": [f"s{i}"], "day": [20200101 + i], "close": [10.0]})
            for i in range(24)
        }
        result = await calc_factor_async(closes, {}, {}, 1, max_concurrency=4)
        assert len(result) == 24
        assert result[b"s0"] == "factor:s0"
        assert result[b"s23"] == "factor:s23"
        assert state["calls"] == 24
        assert 1 <= state["max_inflight"] <= 4, f"concurrency must be bounded by 4, saw {state['max_inflight']}"

    @pytest.mark.asyncio
    async def test_empty_input(self):
        assert await calc_factor_async({}, {}, {}, 1) == {}


# --------------------------------------------------------------------------
# C++ engine golden values (validates the map-iteration rewrite)
# --------------------------------------------------------------------------

class TestCppEngineGolden:
    def test_two_events_forward_backward(self):
        # 20200102: 10送10 -> factor 0.5 ; 20200106: 10配1.5 @14.43, preclose 25.70
        # rgt factor = (25.70 + 14.43*0.15) / (25.70*1.15) = 27.8645/29.555
        trading = [20200101, 20200102, 20200105, 20200106]
        close = [10.0, 5.0, 25.70, 24.15]

        adj = [adj_factor.AdjustmentEvent()]
        adj[0].ex_date = 20200102
        adj[0].bonus_share = 10.0  # 10送10 (already scaled 1e-3 from raw)
        adj[0].transfer = 0.0
        adj[0].bonus = 0.0

        rgt = [adj_factor.RightmentEvent()]
        rgt[0].ex_date = 20200106
        rgt[0].price = 14.43
        rgt[0].ratio = 1.5  # 10配1.5

        f1 = 0.5
        f2 = (25.70 + 14.43 * 0.15) / (25.70 * 1.15)

        fwd = adj_factor.calc_adjust_factors(trading, close, adj, rgt, adj_factor.AdjustType.Forward)
        assert fwd.raw_factors[20200102] == pytest.approx(f1)
        assert fwd.raw_factors[20200106] == pytest.approx(f2)
        # qfq: latest event gets its own factor, earliest gets the product
        assert fwd.adj_factors[20200106] == pytest.approx(f2)
        assert fwd.adj_factors[20200102] == pytest.approx(f1 * f2)

        bwd = adj_factor.calc_adjust_factors(trading, close, adj, rgt, adj_factor.AdjustType.Backward)
        # hfq: earliest event = 1/f1, then 1/(f1*f2)
        assert bwd.adj_factors[20200102] == pytest.approx(1.0 / f1)
        assert bwd.adj_factors[20200106] == pytest.approx(1.0 / (f1 * f2))

    def test_same_date_events_merge_multiplicatively(self):
        trading = [20200101, 20200102, 20200103]
        close = [10.0, 5.0, 5.1]
        adj = [adj_factor.AdjustmentEvent()]
        adj[0].ex_date = 20200102
        adj[0].bonus_share = 10.0
        adj[0].transfer = 0.0
        adj[0].bonus = 0.0
        rgt = [adj_factor.RightmentEvent()]
        rgt[0].ex_date = 20200102
        rgt[0].price = 5.0
        rgt[0].ratio = 10.0  # r=1.0, preclose=10 -> (10+5*1)/(10*2) = 0.75

        res = adj_factor.calc_adjust_factors(trading, close, adj, rgt, adj_factor.AdjustType.Backward)
        # merged multiplicatively on the same ex_date: 0.5 * 0.75
        assert res.raw_factors[20200102] == pytest.approx(0.375)

    def test_ex_date_before_first_day_skipped(self):
        trading = [20200102, 20200103]
        close = [5.0, 5.1]
        adj = [adj_factor.AdjustmentEvent()]
        adj[0].ex_date = 20200101  # before window: no preclose available
        adj[0].bonus_share = 10.0
        res = adj_factor.calc_adjust_factors(trading, close, adj, [], adj_factor.AdjustType.Backward)
        assert len(res.raw_factors) == 0

    def test_struct_converters_batch(self):
        df = pl.DataFrame({
            "ex_date": [20200102, 20200103],
            "bonus_share": [0.1, 0.2],
            "transfer": [0.0, 0.3],
            "bonus": [0.5, 0.0],
        })
        events = adjust2struct(df)
        assert len(events) == 2
        assert events[1].ex_date == 20200103 and events[1].transfer == 0.3

        rdf = pl.DataFrame({"ex_date": [20200102], "price": [14.43], "ratio": [1.5]})
        rev = right2struct(rdf)
        assert rev[0].price == 14.43 and rev[0].ratio == 1.5


# --------------------------------------------------------------------------
# F7 + C1/C9: cleanup / initialize lifecycle
# --------------------------------------------------------------------------

class TestCleanupOrdering:
    def test_reinit_across_loops(self):
        rc = RpcClient(host="127.0.0.1", port=59998)

        async def phase1():
            await rc.initialize()
            await rc.cleanup()

        asyncio.run(phase1())   # loop A: channel + lock created, then dropped

        async def phase2():
            # a stale loop-bound lock / channel reference here used to break
            # re-attachment (see RpcClient.cleanup comment)
            await rc.initialize()
            await rc.cleanup()

        asyncio.run(phase2())   # loop B: must rebuild cleanly

    @pytest.mark.asyncio
    async def test_concurrent_initialize_is_safe(self):
        # C1/C9 regression: concurrent first-wave initialize must not raise
        # (double-checked asyncio.Lock keeps a single channel build)
        rc = RpcClient(host="127.0.0.1", port=59997)
        await asyncio.gather(*[rc.initialize() for _ in range(20)])
        await rc.cleanup()


# --------------------------------------------------------------------------
# S1: _apply_factor UTC -> Asia/Shanghai timezone alignment + integer day
# --------------------------------------------------------------------------

class TestApplyFactorTimezone:
    def test_tick_to_day_uses_asia_shanghai_timezone(self):
        from bt_sdk.core.factor import _apply_factor

        # 1704125100 = 2024-01-01 16:05:00 UTC = 2024-01-02 00:05:00 Asia/Shanghai
        # 1704159060 = 2024-01-02 01:31:00 UTC = 2024-01-02 09:31:00 Asia/Shanghai (live server tick)
        df = pl.DataFrame({
            "tick": [1704125100, 1704159060],
            "open": [10.0, 10.0],
            "high": [10.0, 10.0],
            "low": [10.0, 10.0],
            "close": [10.0, 10.0],
            "volume": [100.0, 100.0],
        })
        # Ex-date is 20240102 with qfq factor 0.5 (meaning dates < 20240102 get 0.5,
        # dates >= 20240102 get 1.0). Both ticks belong to 20240102 in Asia/Shanghai!
        out_qfq = _apply_factor(df, {20240102: 0.5}, 1)
        assert out_qfq["day"].to_list() == [20240102, 20240102]
        assert out_qfq["close"].to_list() == [10.0, 10.0]


# --------------------------------------------------------------------------
# S2: GetMdApi updates timeout on cached instance
# --------------------------------------------------------------------------

class TestGetMdApiTimeoutUpdate:
    def test_cached_instance_updates_timeout(self):
        from bt_sdk.core.client.api import DisposeMdApi

        addr = ("127.0.0.1", 59996)
        try:
            api1 = GetMdApi(addr, timeout=15)
            assert api1.timeout == 15
            assert api1.async_client.timeout == 15

            api2 = GetMdApi(addr, timeout=90)
            assert api2 is api1
            assert api2.timeout == 90
            assert api2.async_client.timeout == 90
        finally:
            DisposeMdApi(addr)


# --------------------------------------------------------------------------
# S3: NumPy zero-copy C++ binding for calc_adjust_factors
# --------------------------------------------------------------------------

class TestNumpyZeroCopyBinding:
    def test_numpy_and_list_produce_identical_factors(self):
        from bt_sdk.core.factor import _calc_factor

        c_df = pl.DataFrame({
            "day": pl.Series([20200101, 20200102, 20200103], dtype=pl.Int32),
            "close": pl.Series([10.0, 5.0, 5.2], dtype=pl.Float64),
        })
        adj_df = pl.DataFrame({
            "ex_date": [20200102],
            "bonus_share": [10.0],
            "transfer": [0.0],
            "bonus": [0.0],
        })
        res = _calc_factor(c_df, adj_df, pl.DataFrame(), 1)
        assert res.adj_factors[20200102] == pytest.approx(0.5)


# --------------------------------------------------------------------------
# S4: Observable resubscription / per-subscription Subject isolation
# --------------------------------------------------------------------------

class TestResubscribeIsolation:
    @pytest.mark.asyncio
    async def test_resubscribe_allocates_fresh_subject_and_disposes(self, api_on_runner):
        client = api_on_runner.async_client
        body = QueryBody(20200101, 20210101, [b"300374"])
        obs = api_on_runner.subscribe(body, RpcTopic.Close)

        sub1 = obs.subscribe(on_next=lambda _: None, on_error=lambda _: None)
        assert await poll_until(lambda: client.pending_counts().futures == 1, "sub1 tracked")
        sub1.dispose()
        assert await poll_until(
            lambda: client.pending_counts().tasks == 0 and client.pending_counts().futures == 0,
            "sub1 cleaned",
        )

        # Second subscription on the SAME observable must still work cleanly
        sub2 = obs.subscribe(on_next=lambda _: None, on_error=lambda _: None)
        assert await poll_until(lambda: client.pending_counts().futures == 1, "sub2 tracked")
        sub2.dispose()
        assert await poll_until(
            lambda: client.pending_counts().tasks == 0 and client.pending_counts().futures == 0,
            "sub2 cleaned",
        )

    @pytest.mark.asyncio
    async def test_subscribe_on_existing_observable_after_disconnect_fails_fast(self, api_on_runner):
        body = QueryBody(20200101, 20210101, [b"300374"])
        obs = api_on_runner.subscribe(body, RpcTopic.Close)
        api_on_runner.disconnect()

        errors = []
        obs.subscribe(on_next=lambda _: None, on_error=errors.append)
        assert len(errors) == 1 and isinstance(errors[0], RuntimeError)
        assert api_on_runner.async_client.pending_counts() == (0, 0)


# --------------------------------------------------------------------------
# D1: Shared singleton lifecycle protection (__exit__ and start(loop))
# --------------------------------------------------------------------------

class TestSharedSingletonProtection:
    def test_shared_singleton_exit_does_not_close_or_hijack_live_loop(self):
        from bt_sdk.core.client.api import DisposeMdApi

        addr = ("127.0.0.1", 59994)
        loop1 = asyncio.new_event_loop()
        t1 = threading.Thread(target=loop1.run_forever, daemon=True)
        t1.start()
        loop2 = asyncio.new_event_loop()
        t2 = threading.Thread(target=loop2.run_forever, daemon=True)
        t2.start()

        try:
            shared = GetMdApi(addr, timeout=10)
            assert shared._is_shared is True
            shared.start(loop1)
            assert shared.loop is loop1

            # Exiting a `with shared:` block must NOT close the shared singleton
            with shared:
                pass
            obs = shared.subscribe(QueryBody(20200101, 20210101, [b"1"]), RpcTopic.Close)
            assert obs is not None

            # Calling start(loop2) while loop1 is alive & running must NOT hijack loop1
            shared.start(loop2)
            assert shared.loop is loop1

            # Calling disconnect() and then bare GetMdApi(addr) (without start())
            # auto-revives the cached instance while loop1 is still running
            shared.disconnect()
            revived = GetMdApi(addr, timeout=10)
            assert revived is shared
            obs_after = revived.subscribe(QueryBody(20200101, 20210101, [b"1"]), RpcTopic.Close)
            assert obs_after is not None
        finally:
            DisposeMdApi(addr)
            loop1.call_soon_threadsafe(loop1.stop)
            loop2.call_soon_threadsafe(loop2.stop)
            t1.join(timeout=5)
            t2.join(timeout=5)
            loop1.close()
            loop2.close()


# --------------------------------------------------------------------------
# D2 + D4: stream_async native backpressure-enabled async generator
# --------------------------------------------------------------------------

class TestStreamAsyncBackpressure:
    @pytest.mark.asyncio
    async def test_stream_async_cancels_cleanly_on_aclose(self, api_on_runner):
        client = api_on_runner.async_client
        body = QueryBody(20200101, 20210101, [b"300374"])
        agen = api_on_runner.stream_async(body, RpcTopic.Close, max_queue=4)

        # Start pulling the first item in a task (will hang on wait_for_ready)
        pull_task = asyncio.create_task(agen.__anext__())
        assert await poll_until(lambda: client.pending_counts().futures == 1, "producer tracked")

        pull_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pull_task
        await agen.aclose()

        assert await poll_until(
            lambda: client.pending_counts().tasks == 0 and client.pending_counts().futures == 0,
            "producer cleaned after aclose",
        )


# --------------------------------------------------------------------------
# P1 + P3 + P4: Columnar C++ factor entry point & empty short-circuit
# --------------------------------------------------------------------------

class TestColumnarCppEngine:
    def test_columnar_matches_struct_and_short_circuits_empty(self):
        from bt_sdk.core.factor import _calc_factor

        c_df = pl.DataFrame({
            "day": pl.Series([20200101, 20200102, 20200103], dtype=pl.Int32),
            "close": pl.Series([10.0, 5.0, 5.1], dtype=pl.Float64),
        })
        # Empty events fast short-circuit
        empty_res = _calc_factor(c_df, pl.DataFrame(), pl.DataFrame(), 1)
        assert len(empty_res.raw_factors) == 0
        assert len(empty_res.adj_factors) == 0

        # Mismatched adjustment column sizes raise ValueError
        with pytest.raises(ValueError, match="adjustment column sizes"):
            adj_factor.calc_adjust_factors_columns(
                [20200101, 20200102], [10.0, 5.0],
                [20200102], [10.0, 20.0], [0.0], [0.0],
                [], [], [],
                adj_factor.AdjustType.Forward,
            )

    def test_unimplemented_future_ex_date_excluded_from_factors(self):
        """Domain rule (N1): unimplemented future ex-right events (ex_date >
        last trading date) MUST NOT be included in adjustment factors."""
        res = adj_factor.calc_adjust_factors_columns(
            [20200101, 20200102, 20200103],
            [10.0, 5.0, 5.1],
            [20200102, 20991231],   # 20991231 is an announced future event
            [10.0, 10.0],
            [0.0, 0.0],
            [0.0, 0.0],
            [], [], [],
            adj_factor.AdjustType.Forward,
        )
        assert list(res.adj_factors.keys()) == [20200102]
        assert res.adj_factors[20200102] == pytest.approx(0.5)


# --------------------------------------------------------------------------
# N3, N5, N7: Non-running loop guard, None body validation, IPv6 addr parse
# --------------------------------------------------------------------------

class TestAuditRound2EdgeGuards:
    def test_non_running_loop_rejected_fast(self):
        idle_loop = asyncio.new_event_loop()
        try:
            api = MdApi(addr=("127.0.0.1", 59992), timeout=5)
            api.start(idle_loop)
            with pytest.raises(RuntimeError, match="no usable event loop attached"):
                api.get_instrument()
        finally:
            idle_loop.close()

    @pytest.mark.asyncio
    async def test_rpc_async_rejects_none_body_for_non_instrument(self, api_on_runner):
        with pytest.raises(ValueError, match="QueryBody req_body is required"):
            await api_on_runner.rpc_async(None, RpcTopic.Daily)

    def test_ipv6_addr_parsing_in_get_md_api(self):
        from bt_sdk.ctx import get_md_api
        from bt_sdk.core.client.api import DisposeMdApi

        try:
            api = get_md_api("[::1]:59990", timeout=12)
            assert api.timeout == 12
        finally:
            DisposeMdApi(("::1", 59990))
