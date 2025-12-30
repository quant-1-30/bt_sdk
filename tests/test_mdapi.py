#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest

from bt_sdk.core.client import MdApi


def get_data(q):
    data = []
    while True:
        msg = q.get()
        print("get_data :", msg)
        if msg == "eof":
            break
        data.append(msg)
    return data


class TestMdApi:
    
    @pytest.fixture
    def md_api(self):
        return MdApi(addr=("localhost", 9000))
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
        start_date = 20000420
        end_date = 20250425
        sid = [b'002750']
        return {"start_date": start_date ,"end_date": end_date, "sid": sid}
    
    def test_getCalendar(self, md_api):
        observable = md_api.get_calendar()
        observable.subscribe(
            on_next = lambda i: print("Received Calendar {0}".format(i)),
            on_error = lambda e: print("Error Occurred: {0}".format(e)),
            on_completed = lambda: print("Done!"),
        )
        assert 1 is not None

    # def test_getInstrument(self, md_api):
    #     observable = md_api.get_instrument()
    #     observable.subscribe(
    #         on_next = lambda i: print("Received Instrument {0}".format(i)),
    #         on_error = lambda e: print("Error Occurred: {0}".format(e)),
    #         on_completed = lambda: print("Done!"),
    #     )
    #     assert 1 is not None

    # def test_getBenchmark(self, md_api, benchmark):
    #     observable = md_api.get_benchmark(benchmark)
    #     assert 1 is not None
     
    # def test_adjust_event(self, md_api, query):
    #     observable = md_api.get_event("adjustment", query)
    #     assert 1 is not None
    
    # def test_right_event(self, md_api, query):
    #     observable = md_api.get_event("rightment", query)
    #     assert 1 is not None
    
    # def test_get_close(self, md_api, query):
    #     observable = md_api.get_close(query)
    #     assert 1 is not None

    # def test_subscirbe(self, md_api, query):
    #     observable = md_api.subscribe(query)
    #     assert 1 is not None
    
    # def test_factor(self, md_api, query):
    #     data = md_api.factor(query)
    #     print("test_get_factors: ", data.raw_factors, data.adj_factors)
    #     assert data is not None
