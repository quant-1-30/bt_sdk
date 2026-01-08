#! /usr/bin/env python3
# -*- coding: utf-8 -*- 

import pytest
import pytz
import uuid
from datetime import datetime
from bt_sdk.core.client import TdApi, SubTopic
from bt_sdk.core.protocol import RegisterBody, CashBody, OrderBody, QueryBody


def get_data(q):
    data = []
    while True:
        msg = q.get()
        if msg == "eof":
            break
        data.append(msg)
    return data


class TestTdApi:

    @pytest.fixture
    def client_id(self): # \x16 --> 2
        return uuid.UUID("e9f8cd38-e73c-453f-8a47-55beda640ae6").bytes
    
    @pytest.fixture
    def experiment_id(self): # bytes.fromhex()
        return b'\xacc\xfb\xa4\x8a\xacF\xca\xa6\xde\xa6$\xa7W7`'

    @pytest.fixture
    def td_api(self, client_id):
        api = TdApi(addr=("127.0.0.1", 8888), client_id=client_id, timeout=20)
        # api = TdApi(addr=("192.168.2.100", 8888), client_id=client_id, timeout=20)
        return api
    
    @pytest.fixture
    def register(self, client_id):
        strategy = "test_cython"
        extra_info= "300308"
        return RegisterBody(client_id=client_id, strategy=strategy, extra_info=extra_info)
    
    @pytest.fixture
    def cash(self):
        session = 19901210
        cash = 100000
        return CashBody(session=session, cash=cash)
      
    @pytest.fixture
    def order(self):
        created_str = "2025-04-23 9:31:00" # 原始数据没有设定pytz timezone default to UTC
        created_dt = datetime.strptime(created_str, '%Y-%m-%d %H:%M:%S')
        created_dt = pytz.UTC.localize(created_dt)
        return OrderBody(
                sid=b"300308", 
                pricelimit=2, # / 100
                sizer_ratio=80, #  /100
                created_dt=int(created_dt.timestamp()),
                order_type=0,
                exec_type=0,
                filler=b"likehood") # oco / occ / smooth / likehood
    
    @pytest.fixture(scope="function")
    def query(self):
        start_date = 1589817599
        end_date = 1589880660
        sid = [b'300308']
        return QueryBody(start_date=start_date, end_date=end_date, sid=sid)
    
    # def test_register(self, td_api, register):
    #     fut = td_api.register(register)
    #     resp = fut.result()
    #     print("resp ", resp)
    #     assert resp is not None

    # def test_set_cash(self, td_api, cash, experiment_id):
    #     fut = td_api.set_cash(experiment_id, cash)
    #     resp = fut.result()
    #     print("test_set_cash: ", resp)
    #     assert resp is not None
    
    # def test_submit(self, td_api, experiment_id, order):
    #     fut = td_api.submit(experiment_id, order)
    #     resp = fut.result()
    #     print("test_submit: ", resp)
    #     assert resp is not None

    # def test_getAccount(self, td_api, experiment_id):
    #     fut = td_api.getvalue(experiment_id, SubTopic.Account)
    #     resp = fut.result()
    #     print("test get_account: ", resp)
    #     assert resp is not None

    # def test_getPosition(self, td_api, experiment_id):
    #     fut = td_api.getvalue(experiment_id, SubTopic.Position)
    #     resp = fut.result()
    #     print("test get_position: ", resp)
    #     assert resp is not None

    # def test_subscirbe_order(self, td_api, experiment_id, query):
    #     fut = td_api.subscribe(experiment_id, SubTopic.Order, query)
    #     resp = fut.result()
    #     print("test_reqOrder: ", resp)
    #     assert resp is not None

    # def test_subscribe_position(self, td_api, experiment_id, query):
    #     fut = td_api.subscribe(experiment_id, SubTopic.Position, query)
    #     resp = fut.result()
    #     print("test_reqPosition: ", resp)
    #     assert resp is not None

    # def test_subscribe_account(self, td_api, experiment_id, query):
    #     fut = td_api.subscribe(experiment_id, SubTopic.Account, query)
    #     resp = fut.result()
    #     print("test_reqAccount: ", resp)
    #     assert resp is not None
    
    def test_on_dt_over(self, td_api, experiment_id, query):
        fut = td_api.on_dt_over(experiment_id, query)
        resp = fut.result()
        print("test_on_dt_over: ", resp)
        assert resp is not None
