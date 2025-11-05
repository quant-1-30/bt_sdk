#! /usr/bin/env python3
# -*- coding: utf-8 -*- 

import pytest
from datetime import datetime
from bt_sdk.core.client import TdApi
from bt_sdk.constant import *
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
    def patch_client_id(self):
        return "1001fe63-3d5d-42b3-89d5-d96218617219"
    
    @pytest.fixture
    def patch_experiment_id(self):
        return "1c3a9bd5-869f-4a26-8b08-dfcfaa48d63b"

    @pytest.fixture
    def td_api(self, patch_client_id, patch_experiment_id):
        api = TdApi(addr=("localhost", 8888), client_id=patch_client_id, timeout=20)
        # api = TdApi(addr=("192.168.2.100", 8888), client_id=client_id, timeout=20)
        api.experiment_id = patch_experiment_id
        return api
    
    @pytest.fixture
    def experiment(self, patch_client_id):
        strategy = "test"
        appendix='002750'
        return Experiment(client_id=patch_client_id, strategy=strategy, appendix=appendix)
    
    @pytest.fixture
    def cash(self):
        session = 19901210
        cash = 100000
        return Cash(session=session, cash=cash)
      
    @pytest.fixture
    def order(self):
        order_type = OrderType.Buy
        created_str = "2025-04-24 09:40:30"
        created_dt = datetime.strptime(created_str, '%Y-%m-%d %H:%M:%S')
        return Order(sid="002750", 
                     price=122,
                     size=1000,
                     sizer_ratio=0.5,
                     pricelimit=130,
                     created_at=created_dt.timestamp(),
                     # ExecType.Close / ExecType.Market 
                     exec_type=ExecType.Open, # oco
                     order_type = order_type)
    
    @pytest.fixture(scope="function")
    def query(self):
        start_date = "20250424"
        end_date = "20250424"
        # end_date = "20250815"
        start_time = datetime.strptime(start_date, '%Y%m%d')
        end_time = datetime.strptime(end_date, '%Y%m%d')
        sid = ['002750']
        return Query(start_date = start_time.timestamp(),
                     end_date = end_time.timestamp(),
                     sid = sid)
    
    # def test_register(self, td_api, experiment):
    #     resp = td_api.register(experiment)
    #     print("resp ", resp)
    #     assert resp is not None

    # def test_set_cash(self, td_api, cash, patch_experiment_id):
    #     data = td_api.set_cash(cash, patch_experiment_id)
    #     print("test_set_cash: ", data)
    #     assert data is not None

    def test_getvalue(self, td_api, patch_experiment_id):
        data = td_api.getvalue("account", "null")
        print("test_getvalue: ", data)
        assert data is not None

    # def test_submit(self, td_api, order, patch_experiment_id):
    #     data = td_api.submit(order, patch_experiment_id)
    #     print("test_submit: ", data)
    #     assert data is not None

    # def test_getAccount(self, td_api, patch_experiment_id):
    #     o = td_api.getvalue("account", patch_experiment_id)
    #     print("test get_account: ", o)
    #     assert o is not None

    # def test_getPosition(self, td_api, patch_experiment_id):
    #     o = td_api.getvalue("position", patch_experiment_id)
    #     print("test get_position: ", o)
    #     assert o is not None

    # def test_subscirbe_order(self, td_api, query, patch_experiment_id):
    #     with td_api.subscribe("order", query, patch_experiment_id) as q:
    #         data = get_data(q)
    #     print("test_reqOrder: ", data)
    #     assert data is not None

    # def test_subscribe_position(self, td_api, query, patch_experiment_id):
    #     with td_api.subscribe("position", query, patch_experiment_id) as q:
    #         data = get_data(q)
    #     print("test_reqPosition: ", data)
    #     assert data is not None

    # def test_subscribe_account(self, td_api, query, patch_experiment_id):
    #     with td_api.subscribe("account", query, patch_experiment_id) as q:
    #         data = get_data(q)
    #     print("test_reqAccount: ", data)
    #     assert data is not None
    
    # def test_on_dt_over(self, td_api, query, patch_experiment_id):
    #     status = td_api.on_dt_over(query, patch_experiment_id)
    #     print("test_on_dt_over: ", status)
    #     assert status is not None
