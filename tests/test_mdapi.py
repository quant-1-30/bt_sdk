#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest

from bt_sdk.core.client import MdApi
from bt_sdk.core.model import *


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
    def session(self):
        return 20241008
    
    @pytest.fixture
    def event_type(self):
        # return "adjustment"
        return "rightment"

    @pytest.fixture
    def query(self):
        start_date = "20240108"
        end_date = "20240308"
        sid = ['603676']
        return Query(start_date = start_date ,end_date = end_date, sid = sid)
    
    # def test_getCalendar(self, md_api):
    #     data = md_api.get_calendar()
    #     print("test_getCalendar: ", data)
    #     assert data is not None

    # def test_getInstrument(self, md_api):
    #     data = md_api.get_instrument()
    #     print("test_getInstrument: ", data)
    #     assert data is not None
    
    # def test_adjust_event(self, md_api, query):
    #     data = md_api.get_event("adjustment", query)
    #     print("test_adjEvent: ", data)
    #     assert data is not None
    
    # def test_right_event(self, md_api, query):
    #     data = md_api.get_event("rightment", query)
    #     print("test_rgtEvent: ", data)
    #     assert data is not None
    
    # def test_subscirbe(self, md_api, query):
    #     res = []
    #     _iter = md_api.subscribe(query)
    #     while True:
    #         try:
    #             data = next(_iter)
    #             res.append(data)
    #         except StopIteration:
    #             break
    #     print("test_req: ", res)
    #     assert res is not None
    
    def test_getBenchmark(self, md_api):
        data = md_api.get_benchmark()
        print("test_getBenchmark: ", data)
        import pdb; pdb.set_trace()
        assert data is not None
    
    # def test_get_close(self, md_api, query):
    #     data = md_api.get_close(query)
    #     print("test_getClose: ", data)
    #     assert data is not None

    # def test_factor(self, md_api, query):
    #     data = md_api.factor(query)
    #     print("test_get_factors: ", data.raw_factors, data.adj_factors)
    #     assert data is not None
