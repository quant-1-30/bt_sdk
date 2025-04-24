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
    def order(self):
        return OrderMeta(client_id="hx", 
                         sid="603676", 
                         size=100,
                         sizer_cash=10000,
                         price=97,
                         pricelimit=102,
                         created_at=1728351060, 
                         exectype=0)
    
    @pytest.fixture
    def req_order(self):
        return Req(sid=["603676"], start_date=20241008, end_date=20241008)
    
    # @pytest.fixture 
    # def req_position(self):
    #     return Req(sid=["603676"], start_date=20241008, end_date=20241008)
    
    # @pytest.fixture
    # def req_account(self):
    #     return Req(sid=["603676"], start_date=20241008, end_date=20241008)
    
    # @pytest.fixture
    # def req_sync(self):
    #     return 20241008
    
    def test_default(self, td_api, order):
        # body = {"user_id": "hx", "session": 0, "data": order.model_dump()} 
        # body = {"client_id": "hx", "data": order.model_dump()} 
        q = td_api.on_trade(order)
        data = self.get_data(q)
        print(data)
        assert data is not None

    # def test_reqOrder(self, td_api, req_order):
    #     q = td_api.reqOrder(req_order)
    #     data = self.get_data(q)
    #     print(data)
    #     assert data is not None

    # def test_reqPosition(self, td_api, req_position):
    #     q = td_api.reqPosition(req_position)
    #     data = self.get_data(q)
    #     print(data)
    #     assert data is not None

    # def test_reqAccount(self, td_api, req_account):
    #     q = td_api.reqAccount(req_account)
    #     data = self.get_data(q)
    #     print(data)
    #     assert data is not None

    # def test_sync(self, td_api, req_sync):
    #     q = td_api.sync(req_sync)    
    #     data = self.get_data(q)
    #     print(data)
    #     assert data is not None
