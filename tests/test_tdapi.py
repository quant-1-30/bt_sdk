#! /usr/bin/env python3
# -*- coding: utf-8 -*- 

import pytest
from bt_sdk.core.client import TdApi
from bt_sdk.core.model import *
from bt_sdk.core.constant import *


def get_data(q):
    data = []
    while True:
        msg = q.get()
        if msg == "eof":
            q.reset()
            break
        data.append(msg)
    return data


class TestTdApi:

    @pytest.fixture
    def client_id(self):
        return "efe4eaee-0406-46e3-a395-91dc4502c4a3"

    @pytest.fixture
    def td_api(self, client_id):
        api = TdApi(addr=("127.0.0.1", 8888), client_id=client_id)
        return api
    
    @pytest.fixture
    def ordermeta(self):
        order_type = OrderType.Sell
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
    
    def test_getAccount(self, td_api):
        q = td_api.getAccount()
        data = get_data(q)
        print("test_get_account: ", data)
        assert data is not None

    def test_getPosition(self, td_api):
        q = td_api.getPosition()
        data = get_data(q)
        print("test_get_position: ", data)
        assert data is not None

    def test_sub_Order(self, td_api, reqmeta):
        q = td_api.subscribe("order", reqmeta)
        data = get_data(q)
        print("test_reqOrder: ", data)
        assert data is not None

    def test_sub_Position(self, td_api, reqmeta):
        q = td_api.subscribe("position", reqmeta)
        data = get_data(q)
        print("test_reqPosition: ", data)
        assert data is not None

    def test_sub_Account(self, td_api, reqmeta):
        q = td_api.subscribe("account", reqmeta)
        data = get_data(q)
        print("test_reqAccount: ", data)
        assert data is not None
    
    def test_placeOrder(self, td_api, ordermeta):
        q = td_api.placeOrder(ordermeta)
        data = get_data(q)
        print("test_placeOrder: ", data)
        assert data is not None

    def test_onTimer(self, td_api):
        q = td_api.onTimer(1728351060)
        data = q.get()
        assert data is not None

