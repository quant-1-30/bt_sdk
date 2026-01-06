#! /usr/bin/env python3
# -*- coding: utf-8 -*- 

import pytest
import uuid
from datetime import datetime
from bt_sdk.core.client import TdApi
from bt_sdk.core.model import *


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
    def patch_client_id(self): # \x16 --> 2
        return uuid.UUID("e9f8cd38-e73c-453f-8a47-55beda640ae6").bytes
    
    @pytest.fixture
    def patch_experiment_id(self): # bytes.fromhex()
        return uuid.UUID("502ed4db-371d-411f-8bee-5fb4d31860fb").bytes 

    @pytest.fixture
    def td_api(self, patch_client_id, patch_experiment_id):
        api = TdApi(addr=("127.0.0.1", 8888), client_id=patch_client_id, timeout=20)
        # api = TdApi(addr=("192.168.2.100", 8888), client_id=client_id, timeout=20)
        return api
    
    @pytest.fixture
    def experiment(self, patch_client_id):
        strategy = "test_cython"
        extra_info= "002750"
        return {"strategy": strategy, "extra_info": extra_info, "identity": b"jsklfjaslfjsalfjaslfj"}
    
    @pytest.fixture
    def cash(self):
        session = 19901210
        cash = 100000
        return {"session": session, "cash": cash}
      
    @pytest.fixture
    def order(self):
        created_str = "2025-04-23 9:30:00" # asia 8 after utc
        created_dt = datetime.strptime(created_str, '%Y-%m-%d %H:%M:%S')
        return {"sid": b"002750", 
                "pricelimit": 2, # / 100
                "sizer_ratio": 80, #  /100
                "created_dt": created_dt.timestamp(),
                "order_type": 0,
                "exec_type": 0,
                "filler": "likehood"} # oco / occ / smooth / likehood
    
    @pytest.fixture(scope="function")
    def query(self):
        start_date = 1744387199
        end_date = 1745400660
        sid = [b'002750']
        return {"start_date": start_date, "end_date": end_date, "sid": sid}
    
    # def test_register(self, td_api, experiment):
    #     fut = td_api.register(experiment)
    #     resp = fut.result()
    #     print("resp ", resp)
    #     assert resp is not None

    # def test_set_cash(self, td_api, cash, patch_experiment_id):
    #     fut = td_api.set_cash(patch_experiment_id, cash)
    #     resp = fut.result()
    #     print("test_set_cash: ", resp)
    #     assert resp is not None
    
    # def test_submit(self, td_api, patch_experiment_id, order):
    #     fut = td_api.submit(patch_experiment_id, order)
    #     resp = fut.result()
    #     print("test_submit: ", resp)
    #     assert resp is not None

    # def test_getAccount(self, td_api, patch_experiment_id):
    #     fut = td_api.getvalue(patch_experiment_id, "account")
    #     resp = fut.result()
    #     print("test get_account: ", resp)
    #     assert resp is not None

    # def test_getPosition(self, td_api, patch_experiment_id):
    #     fut = td_api.getvalue(patch_experiment_id, "position")
    #     resp = fut.result()
    #     print("test get_position: ", resp)
    #     assert resp is not None

    # def test_subscirbe_order(self, td_api, patch_experiment_id, query):
    #     query["req_type"] = "order"
    #     fut = td_api.subscribe(patch_experiment_id, query)
    #     resp = fut.result()
    #     print("test_reqOrder: ", resp)
    #     assert resp is not None

    # def test_subscribe_position(self, td_api, patch_experiment_id, query):
    #     query["req_type"] = "position"
    #     fut = td_api.subscribe(patch_experiment_id, query)
    #     resp = fut.result()
    #     print("test_reqPosition: ", resp)
    #     assert resp is not None

    # def test_subscribe_account(self, td_api, patch_experiment_id, query):
    #     query["req_type"] = "account"
    #     fut = td_api.subscribe(patch_experiment_id, query)
    #     resp = fut.result()
    #     print("test_reqAccount: ", resp)
    #     assert resp is not None
    
    # def test_on_dt_over(self, td_api, patch_experiment_id, query):
    #     fut = td_api.on_dt_over(patch_experiment_id, query)
    #     resp = fut.result()
    #     print("test_on_dt_over: ", resp)
    #     assert resp is not None
