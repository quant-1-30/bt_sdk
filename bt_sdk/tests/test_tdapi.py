#! /usr/bin/env python3
# -*- coding: utf-8 -*- 

import pytest
from core.lib.tdapi import *
from core.model import *
from core.constant import *

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
        return TdApi(addr=("127.0.0.1", 8888))
    
    @pytest.fixture
    def client_id(self):
        return "685c25e9-c25a-43d0-a3bf-d3f304a3fe28"
    
    @pytest.fixture
    def loginmsg(self):
        return LoginMsg(topic="login", 
                        msg=LoginMeta(
                              user_id="hx",
                              phone=13776668123,
                              auto_register=True)
                              )
    
    @pytest.fixture
    def ordermsg(self, client_id):
        return OrderMsg(topic="default", 
                         msg=OrderMeta(
                             client_id=client_id, 
                             sid="603676", 
                             price=97,
                             size=100,
                             sizer_cash=10000,
                             pricelimit=102,
                             created_at=1728351060, 
                             exec_type=ExecType.Open,
                            #  order_type = OrderType.Buy))
                             order_type = OrderType.Sell))
    
    @pytest.fixture 
    def reqmsg(self, client_id):
        return RequestMsg(topic="query", 
                          msg=ReqMeta(
                            #   sub_topic="order",
                            #   sub_topic="position",
                            #   sub_topic="account",
                              client_id=client_id,
                              sids=["603676"], 
                              start_time=20241008, 
                              end_time=20241008))
    
    @pytest.fixture
    def timermsg(self, client_id):
        return TimerMsg(topic="timer", 
                        msg=TimerMeta(
                              client_id=client_id,
                              timer="eos")
                              )
    
    # def test_login(self, td_api, loginmsg):
    #     q = td_api.on_login(loginmsg)
    #     data = self.get_data(q)
    #     print(data)
    #     assert data is not None
    
    def test_default(self, td_api, ordermsg):
        # body = {"user_id": "hx", "session": 0, "data": order.model_dump()} 
        # body = {"client_id": "hx", "data": order.model_dump()} 
        q = td_api.on_trade(ordermsg)
        data = self.get_data(q)
        print(data)
        assert data is not None

    # def test_request(self, td_api, reqmsg):
    #     q = td_api.on_request(reqmsg)
    #     data = self.get_data(q)
    #     print(data)
    #     assert data is not None

    # def test_timer(self, td_api, timermsg):
    #     q = td_api.on_timer(timermsg)    
    #     data = self.get_data(q)
    #     print(data)
    #     assert data is not None
