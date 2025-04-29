#! /usr/bin/env python3
# -*- coding: utf-8 -*- 

import pytest
from bt_sdk.core.client import TdApi
from bt_sdk.core.model import *
from bt_sdk.core.constant import *

class TestTdApi:

    @pytest.fixture
    def td_api(self):
        return TdApi(addr=("127.0.0.1", 8888))
    
    @pytest.fixture
    def client_id(self):
        return "69b0322e-1313-4c2a-b9ef-967b8583b39d"
    
    @pytest.fixture
    def loginmeta(self):
        return LoginMeta(user_id="hx",
                         phone=13776668123,
                         auto_register=True)
    
    @pytest.fixture
    def query_topic(self):
        # topic = "query_order"
        # topic="query_position"
        topic="query_account"
        return topic
    
    @pytest.fixture
    def authmeta(self, client_id):
        return AuthMeta(client_id=client_id)
    
    @pytest.fixture
    def ordermeta(self):
        order_type = OrderType.Sell
        # order_type = OrderType.Buy
        return OrderMeta(sid="603676", 
                         price=97,
                         size=100,
                         sizer_cash=10000,
                         pricelimit=102,
                         created_at=1728351060, 
                         exec_type=ExecType.Open,
                         order_type = order_type)
    
    @pytest.fixture 
    def reqmeta(self):
        return ReqMeta(sid=["603676"], 
                       start_date=1728351060, 
                       end_date=1728351060)
    
    @pytest.fixture
    def timermeta(self):
        return TimerMeta(timer="eos", session = 20241008)
    
    # def test_login(self, td_api, loginmeta):
    #     td_api.on_login(loginmeta)
    #     print(f"client_id : {td_api.client_id}")
    #     assert td_api.client_id is not None

    # def test_order(self, td_api, ordermeta, authmeta):
    #     data = td_api.on_trade(ordermeta, authmeta)
    #     print(data)
    #     assert data is not None

    def test_timer(self, td_api, timermeta, authmeta):
        data = td_api.on_timer(timermeta, authmeta)    
        print(data)
        assert data is not None

    # def test_request(self, td_api, query_topic, reqmeta, authmeta):
    #     data = td_api.on_request(query_topic, reqmeta, authmeta)
    #     print(data)
    #     assert data is not None
