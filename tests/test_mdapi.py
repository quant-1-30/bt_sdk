#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest
import queue
import asyncio
import reactivex
from reactivex import operators as ops
import pyarrow as pa
import pyarrow.compute as pc

from bt_sdk.core.client import MdApi


class TestMdApi:
    
    @pytest.fixture
    def md_api(self):
        return MdApi(addr=("127.0.0.1", 9000))
        # return MdApi(addr=("192.168.2.100", 9000))
    
    @pytest.fixture
    def benchmark(self):
        # 000001 000680 399006 399001
        return b'000001'

    @pytest.fixture
    def session(self):
        return 20241008
    
    @pytest.fixture
    def event_type(self):
        # return "adjustment"
        return "rightment"

    @pytest.fixture
    def query(self):
        start_date = 20100101
        end_date = 20250424
        sid = [b'002750']
        return {"start_date": start_date ,"end_date": end_date, "sid": sid}
    
    # def test_getCalendar(self, md_api):
    #     with md_api as client:
    #         observable = client.get_calendar() 
    #         q = queue.Queue()
    #         results = []
            
    #         observable.subscribe( # nonblocking 
    #             on_next=q.put,
    #             on_error=lambda e: q.put(e),
    #             on_completed=lambda: q.put(StopIteration) # 使用特殊标记表示结束
    #         )
            
    #         while True:
    #             item = q.get() # blocking
    #             if item is StopIteration:
    #                 break
    #             if isinstance(item, Exception):
    #                 raise item
    #             results.append(item)
                
    #         print(f"Calendar Results: {results}")

    # def test_get_calendar_async(self, md_api):
    #     with md_api as client:
    #         obs = client.get_calendar()
    #         results = []

    #         async def collect():
    #             async for item in obs: # v4
    #                 results.append(item)
            
    #         future = asyncio.run_coroutine_threadsafe(collect(), client.loop)
    #         future.result(timeout=10) 
             
    #         assert len(results) > 0

    # def test_getInstrument(self, md_api):
    #     with md_api as client:
    #         observable = client.get_instrument()

    #         q = queue.Queue()
    #         results = []
            
    #         observable.subscribe( # nonblocking 
    #             on_next=q.put,
    #             on_error=lambda e: q.put(e),
    #             on_completed=lambda: q.put(StopIteration) # 使用特殊标记表示结束
    #         )
            
    #         while True:
    #             item = q.get() # blocking
    #             if item is StopIteration:
    #                 break
    #             if isinstance(item, Exception):
    #                 raise item
    #             results.append(item)
                
    #         print(f"Instrument Results: {results}")

    # def test_getBenchmark(self, md_api, benchmark):
    #     with md_api as client:
    #         observable = client.get_benchmark(benchmark)

    #         q = queue.Queue()
    #         results = []
            
    #         observable.subscribe( # nonblocking 
    #             on_next=q.put,
    #             on_error=lambda e: q.put(e),
    #             on_completed=lambda: q.put(StopIteration) # 使用特殊标记表示结束
    #         )
            
    #         while True:
    #             item = q.get() # blocking
    #             if item is StopIteration:
    #                 break
    #             if isinstance(item, Exception):
    #                 raise item
    #             results.append(item)
                
    #         print(f"Benchmark Results: {results}")
     
    # def test_adjust_event(self, md_api, query):
    #     with md_api as client:
    #         observable = client.get_event("adjustment", query)

    #         q = queue.Queue()
    #         results = []
            
    #         observable.subscribe( # nonblocking 
    #             on_next=q.put,
    #             on_error=lambda e: q.put(e),
    #             on_completed=lambda: q.put(StopIteration) # 使用特殊标记表示结束
    #         )
            
    #         while True:
    #             item = q.get() # blocking
    #             if item is StopIteration:
    #                 break
    #             if isinstance(item, Exception):
    #                 raise item
    #             results.append(item)
                
    #         print(f"Adjustment Results: {results}")
    
    # def test_right_event(self, md_api, query):
    #     with md_api as client:
    #         observable = client.get_event("rightment", query)

    #         q = queue.Queue()
    #         results = []
            
    #         observable.subscribe( # nonblocking 
    #             on_next=q.put,
    #             on_error=lambda e: q.put(e),
    #             on_completed=lambda: q.put(StopIteration) # 使用特殊标记表示结束
    #         )
            
    #         while True:
    #             item = q.get() # blocking
    #             if item is StopIteration:
    #                 break
    #             if isinstance(item, Exception):
    #                 raise item
    #             results.append(item)
                
    #         print(f"Right Results: {results}")
    
    # def test_get_close(self, md_api, query):
    #     with md_api as client:
    #         observable = client.get_close(query)

    #         q = queue.Queue()
    #         results = []
            
    #         observable.subscribe( # nonblocking 
    #             on_next=q.put,
    #             on_error=lambda e: q.put(e),
    #             on_completed=lambda: q.put(StopIteration) # 使用特殊标记表示结束
    #         )
            
    #         while True:
    #             item = q.get() # blocking
    #             if item is StopIteration:
    #                 break
    #             if isinstance(item, Exception):
    #                 raise item
    #             results.append(item)
                
    #         print(f"Close Results: {results}")

    # def test_subscirbe(self, md_api, query):
    #     with md_api as client:
    #         observable = client.subscribe(query)

    #         q = queue.Queue()
    #         results = []
            
    #         observable.subscribe( # nonblocking 
    #             on_next=q.put,
    #             on_error=lambda e: q.put(e),
    #             on_completed=lambda: q.put(StopIteration) # 使用特殊标记表示结束
    #         )
            
    #         while True:
    #             item = q.get() # blocking
    #             if item is StopIteration:
    #                 break
    #             if isinstance(item, Exception):
    #                 raise item
    #             results.append(item)
                
    #         print(f"Subscribe Results: {results}")
    
    def test_factor(self, md_api, query):
        with md_api as client:
            data = client.get_factor(query)
            print("test_get_factors: ", data.raw_factors, data.adj_factors)
            assert data is not None
