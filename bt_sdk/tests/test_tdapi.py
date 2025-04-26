#! /usr/bin/env python3
# -*- coding: utf-8 -*- 

import pytest
from core.cerebro.tdapi import *
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
        return "86e0cefc-d5dc-4994-b190-50531597ea61"
    
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
        return OrderMsg(topic="order", 
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
                              sub_topic="order",
                            #   sub_topic="position",
                            #   sub_topic="account",
                              client_id=client_id,
                              sids=["603676"], 
                              start_time=1728351060, 
                              end_time=1728351060))
    
    @pytest.fixture
    def timermsg(self, client_id):
        return TimerMsg(topic="timer", 
                        msg=TimerMeta(
                              client_id=client_id,
                              timer="eos",
                              body={'session': "20241008"})
                              )
    
    # def test_login(self, td_api, loginmsg):
    #     td_api.on_login(loginmsg)
    #     print(f"client_id : {td_api.client_id}")
    #     assert td_api.client_id is not None

    # def test_order(self, td_api, ordermsg):
    #     data = td_api.on_trade(ordermsg)
    #     print(data)
    #     assert data is not None

    # def test_request(self, td_api, reqmsg):
    #     data = td_api.on_request(reqmsg)
    #     print(data)
    #     assert data is not None

    def test_timer(self, td_api, timermsg):
        data = td_api.on_timer(timermsg)    
        print(data)
        assert data is not None
