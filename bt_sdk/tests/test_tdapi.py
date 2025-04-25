#! /usr/bin/env python3
# -*- coding: utf-8 -*- 

import pytest
from core.lib.tdapi import *
from core.model import *


class TestTdApi:

    def get_data(self, q):
        data = []
        while True:                
            item = q.get()
            if item == "eof":
                break
            data.append(item)
        return data

    @pytest.fixture
    def td_api(self):
        return TdApi(addr=("127.0.0.1", 8888), client_id="hx")
    
    @pytest.fixture
    def ordermsg(self):
        return OrderMsg(topic="default", 
                         msg=OrderMeta(
                             client_id="hx", 
                             sid="603676", 
                             size=100,
                             sizer_cash=10000,
                             price=97,
                             pricelimit=102,
                             created_at=1728351060, 
                             exectype=0))
    
    @pytest.fixture 
    def reqmsg(self):
        return RequestMsg(topic="query", 
                          msg=ReqMeta(
                              sub_topic="order",
                              client_id="hx",
                              sids=["603676"], 
                              start_time=20241008, 
                              end_time=20241008))
    
    @pytest.fixture
    def timermsg(self):
        return TimerMsg(topic="timer", 
                        msg=TimerMeta(
                              sub_topic="sos",
                              eos=20241008)
                              )
    
    # def test_default(self, td_api, ordermsg):
    #     # body = {"user_id": "hx", "session": 0, "data": order.model_dump()} 
    #     # body = {"client_id": "hx", "data": order.model_dump()} 
    #     q = td_api.on_trade(ordermsg)
    #     data = self.get_data(q)
    #     print(data)
    #     assert data is not None

    def test_request(self, td_api, reqmsg):
        q = td_api.on_request(reqmsg)
        data = self.get_data(q)
        print(data)
        assert data is not None

    # def test_timer(self, td_api, timermsg):
    #     q = td_api.on_timer(timermsg)    
    #     data = self.get_data(q)
    #     print(data)
    #     assert data is not None
