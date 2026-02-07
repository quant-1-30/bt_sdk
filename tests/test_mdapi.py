#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest
import queue
import asyncio
import reactivex
from reactivex import operators as ops
import pyarrow as pa
import pyarrow.compute as pc

from bt_sdk.core.protocol import QueryBody
from bt_sdk.core.client import MdApi, RpcTopic


class TestMdApi:
    
    @pytest.fixture
    def md_api(self):
        return MdApi(addr=("127.0.0.1", 50051)) # MdApi(addr=("192.168.2.100", 9000))
    
    @pytest.fixture
    def benchmark(self):
        return b'000001'

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
        sid = [b'000001']
        return QueryBody(start_date, end_date, sid)
    
    # def test_getCalendar(self, md_api):
    #     with md_api as client:
    #         results = client.get_calendar() 
    #     print(f"Calendar Results: {results}")

    # def test_getInstrument(self, md_api):
    #     with md_api as client:
    #         results = client.get_instrument()
    #         print(f"Instrument Results: {results}")

    # def test_getBenchmark(self, md_api, benchmark):
    #     with md_api as client:
    #         results = client.get_benchmark(benchmark)
    #         print(f"Benchmark Results: {results}")
    
    # def test_adj_event(self, md_api, query):
    #     with md_api as client:
    #         results = client.get_event(5, query)
    #         print(f"Subscribe Adj Results: {results}")

    # def test_close(self, md_api, query):
    #     with md_api as client:
    #         results = client.get_close(query)
    #         print(f"Subscribe Close Results: {results}")

    def test_subscirbe(self, md_api, query):
        with md_api as client:
            results = client.get_subscribe(query)
            print(f"Subscribe Results: {results}")
    
    # def test_factor(self, md_api, query):
    #     with md_api as client:
    #         data = md_api.get_factor(query)
    #         print("test_get_factors: ", data.raw_factors, data.adj_factors)
    #         assert data is not None
