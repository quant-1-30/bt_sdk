#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest
import pytest_asyncio
import queue
import asyncio
import threading
import reactivex
from reactivex import operators as ops
import pyarrow as pa
import pyarrow.compute as pc

from bt_sdk.core.protocol import QueryBody
from bt_sdk.core.client import GetMdApi, RpcTopic


@pytest.fixture(scope="session") 
def event_loop():
    """
        session 级别的 event loop pytest-asyncio 默认的 function 级别 loop。
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def md_api():
    api = GetMdApi(("127.0.0.1", 50051))

    loop = asyncio.get_running_loop()  
    api.start(loop)

    yield api

    api.disconnect()


class TestMdApi:
    
    @pytest.fixture
    def benchmark(self):
        start_date = 20000101
        end_date = 20250424
        sid = [b'000001'] # 000001 000680 399006 399001
        return QueryBody(start_date, end_date, sid)

    @pytest.fixture
    def session(self):
        return 20241008
    
    @pytest.fixture
    def event_type(self):
        return RpcTopic.Adjustment # RpcTopic.Rightment

    @pytest.fixture
    def query(self):
        start_date = 20000101
        end_date = 20260424
        sid = [b'300308']
        return QueryBody(start_date, end_date, sid)
    
    # @pytest.mark.asyncio
    # async def test_getCalendar(self, md_api):
    #     results = await md_api.get_calendar_async()
    #     print(f"Calendar Results: {results}")

    # @pytest.mark.asyncio
    # async def test_getInstrument(self, md_api):
    #     results = await md_api.get_instrument_async()
    #     print(f"Instrument Results: {results}")

    # @pytest.mark.asyncio
    # async def test_getBenchmark(self, md_api, benchmark):
    #     results = await md_api.get_benchmark_async(benchmark)
    #     print(f"Benchmark Results: {results}")
    
    # @pytest.mark.asyncio
    # async def test_event(self, md_api, query):
    #     results = await md_api.get_event_async(6, query)
    #     print(f"Subscribe Adj Results: {results}")

    # @pytest.mark.asyncio
    # async def test_close(self, md_api, query):
    #     results = await md_api.get_close_async(query)
    #     print(f"Subscribe Close Results: {results}")

    # @pytest.mark.asyncio
    # async def test_subscirbe(self, md_api, query):
    #     results = await md_api.get_subscribe_async(query)
    #     print(f"Subscribe Results: {results}")
    
    @pytest.mark.asyncio
    async def test_factor(self, md_api, query):
        datas = await md_api.get_factor_async(query)
        data = datas[b"300308"]
        print("test_get_factors: ", data.raw_factors, data.adj_factors)
        assert data is not None
