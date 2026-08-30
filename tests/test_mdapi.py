#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import pytest
import pytest_asyncio
import queue
import asyncio
import threading
import reactivex
import pyarrow as pa
import pyarrow.compute as pc
import polars as pl

from reactivex import operators as ops
from bt_sdk.ctx import external_mdapi_context

from bt_protocol._protocol import QueryBody
from bt_protocol.constant import RpcTopic, FactorTopic


class TestMdApi:

    @pytest.fixture
    def chan(self):
        return list()
    
    @pytest.fixture
    def session(self):
        return 20241008

    @pytest.fixture
    def md_api_ctx(self):
        return external_mdapi_context()
    
    @pytest.fixture
    def event_type(self):
        return RpcTopic.Adjustment # RpcTopic.Rightment

    @pytest.fixture
    def query(self):
        start_date = 20100101
        # start_date = 1721926400
        end_date = 20260630
        # end_date = 1734972800
        sid = [b'300374'] # [b'399001'] # 399006 399001
        return QueryBody(start_date, end_date, sid)

    @pytest.mark.asyncio
    async def test_getInstrument(self, md_api_ctx):
        with md_api_ctx as md_api:
            assets = await md_api.get_instrument_async()
            print(f"Instrument Results: {assets}")

    @pytest.mark.asyncio
    async def test_tick_subscirbe(self, md_api_ctx, query):
        with md_api_ctx as md_api:

            chan = []
            observable = md_api.subscribe(query, RpcTopic.Tick)

            # RxPy subscribe nonblocking 
            loop = asyncio.get_running_loop()
            finished_future = loop.create_future()

            observable.pipe(
                # ops.sample(0.1),  # 100ms abandon reset 
                # ops.buffer_with_time_or_count(timespan=1.0, count=500), # up to 500 / 1 second to list
                # ops.throttle_first(0.05), # on receive / 50ms not receive
                # ops.publish_replay(1), # cache 1 record 
                # ops.ref_count()
                ops.map(lambda data: data["data"]),
                ops.share()
            ).subscribe( 
                on_next=chan.append,
                on_error=lambda e: loop.call_soon_threadsafe(finished_future.set_exception, e),
                on_completed=lambda: loop.call_soon_threadsafe(finished_future.set_result, True)
            )
            await finished_future
            table = pa.concat_tables(chan) if chan else []
            df = pl.from_arrow(table) if table else pl.DataFrame()
            print(f"Subscribe Tick Results: {df}")

    @pytest.mark.asyncio
    async def test_daily_subscirbe(self, md_api_ctx, query):
        with md_api_ctx as md_api:

            chan = []
            observable = md_api.subscribe(query, RpcTopic.Daily)

            # RxPy subscribe nonblocking 
            loop = asyncio.get_running_loop()
            finished_future = loop.create_future()

            observable.pipe(
                # ops.sample(0.1),  # 100ms abandon reset 
                # ops.buffer_with_time_or_count(timespan=1.0, count=500), # up to 500 / 1 second to list
                # ops.throttle_first(0.05), # on receive / 50ms not receive
                # ops.publish_replay(1), # cache 1 record 
                # ops.ref_count()
                ops.map(lambda data: data["data"]),
                ops.share()
            ).subscribe( 
                on_next=chan.append,
                on_error=lambda e: loop.call_soon_threadsafe(finished_future.set_exception, e),
                on_completed=lambda: loop.call_soon_threadsafe(finished_future.set_result, True)
            )
            await finished_future
            table = pa.concat_tables(chan) if chan else []
            df = pl.from_arrow(table) if table else pl.DataFrame()
            print(f"Subscribe Daily Results: {df}")
    
    @pytest.mark.asyncio
    async def test_close_subscirbe(self, md_api_ctx, query):
        with md_api_ctx as md_api:

            chan = []
            observable = md_api.subscribe(query, RpcTopic.Close)

            # RxPy subscribe nonblocking 
            loop = asyncio.get_running_loop()
            finished_future = loop.create_future()

            observable.pipe(
                ops.map(lambda data: data["data"]),
                ops.share()
            ).subscribe( 
                on_next=chan.append,
                on_error=lambda e: loop.call_soon_threadsafe(finished_future.set_exception, e),
                on_completed=lambda: loop.call_soon_threadsafe(finished_future.set_result, True)
            )
        
            await finished_future
            table = pa.concat_tables(chan) if chan else []
            df = pl.from_arrow(table) if table else pl.DataFrame()
            print(f"Subscribe Close Results: {df}")

    @pytest.mark.asyncio
    async def test_adj_subscirbe(self, md_api_ctx, query):
        with md_api_ctx as md_api:

            chan = []
            observable = md_api.subscribe(query, RpcTopic.Adjustment)

            # RxPy subscribe nonblocking 
            loop = asyncio.get_running_loop()
            finished_future = loop.create_future()

            observable.pipe(
                ops.map(lambda data: data["data"]),
                ops.share()
            ).subscribe( 
                on_next=chan.append,
                on_error=lambda e: loop.call_soon_threadsafe(finished_future.set_exception, e),
                on_completed=lambda: loop.call_soon_threadsafe(finished_future.set_result, True)
            )
        
            await finished_future
            table = pa.concat_tables(chan) if chan else []
            df = pl.from_arrow(table) if table else pl.DataFrame()
            print(f"Subscribe Adjustment Results: {df}")

    @pytest.mark.asyncio
    async def test_rgt_subscirbe(self, md_api_ctx, query):
        with md_api_ctx as md_api:
        
            chan = []
            observable = md_api.subscribe(query, RpcTopic.Rightment)

            # RxPy subscribe nonblocking 
            loop = asyncio.get_running_loop()
            finished_future = loop.create_future()

            observable.pipe(
                ops.map(lambda data: data["data"]),
                ops.share()
            ).subscribe( 
                on_next=chan.append,
                on_error=lambda e: loop.call_soon_threadsafe(finished_future.set_exception, e),
                on_completed=lambda: loop.call_soon_threadsafe(finished_future.set_result, True)
            )
        
            await finished_future
            table = pa.concat_tables(chan) if chan else []
            df = pl.from_arrow(table) if table else pl.DataFrame()
            print(f"Subscribe Rightment Results: {df}")

    @pytest.mark.asyncio
    async def test_rpc_async(self, md_api_ctx, query):
        with md_api_ctx as md_api:

            data = await md_api.rpc_async(query, RpcTopic.Adjustment)
            print(f"Direct Run Async Adjustment Results: {data}")

    @pytest.mark.asyncio
    async def test_factor(self, md_api_ctx, query):
        with md_api_ctx as md_api:
            data = await md_api.get_factor_async(query, FactorTopic.Qfq)
            # print("adj_factors: ", data[b'000001'].raw_factors, '\n', "rgt_factors: ", data[b'000001'].adj_factors)
            print(f"Factor Results: {data}")
