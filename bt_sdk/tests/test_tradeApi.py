import pytest
import numpy as np
import datetime
from core.tradeApi import *
from core.meta import LoginMeta, QuoteMeta, OrderMeta, EventMeta, RangeMeta


# reuse / dependency / setup / teardown

class TestQuoteApi:

    @pytest.fixture
    def trade_api(self):
        return TradeApi(addr="http://localhost:8100")
    
    @pytest.fixture
    def req_login(self):
        # return ReqEvent(rpc_type="calendar", meta=Meta(start_date=19900101))
        return LoginMeta(name="test", phone=1234567890, email="test@test.com")
    
    @pytest.fixture
    def req_auth(self):
        return AuthMeta(token="a8894326-edf0-48ff-8a15-ca82e2d8a74f", 
                        experiment_id="249635c0-79f2-44c3-81a0-e7211dbc8621")
                        # experiment_id="")
    
    @pytest.fixture
    def req_trade(self):
        return OrderMeta(sid="603676", order_type=4, direction=1, created_at="202410081405", price=100, amount=10000000)
    
    @pytest.fixture
    def req_event(self):
        # return EventMeta(event_type="adjustment", meta=QuoteMeta(sid=["603676"]))
        return EventMeta(event_type="rightment", meta=QuoteMeta(sid=["603676"]))
    
    @pytest.fixture
    def req_sync(self):
        return QuoteMeta(sid=["603676"], start_date=20241008, end_date=20241008)
    
    @pytest.fixture
    def req_account(self):
        return RangeMeta(start_dt=0, end_dt=20241008)
    
    # def test_onLogin(self, trade_api, req_login):
    #     data = trade_api.on_login(req_login)
    #     print(data)
    #     assert data is not None

    # def test_onDeploy(self, trade_api, req_auth):
    #     data = trade_api.on_deploy(req_auth)
    #     print(data)

    # def test_onDisplay(self, trade_api, req_auth):
    #     data = trade_api.on_display(req_auth)
    #     print(data)

    def test_onTrade(self, trade_api, req_auth, req_trade):
        data = trade_api.on_trade(req_auth, req_trade)
        print(data)

    # def test_onEvent(self, trade_api, req_auth, req_event):
    #     data = trade_api.on_event(req_auth, req_event)
    #     print(data)

    # def test_onSync(self, trade_api, req_auth, req_sync):
    #     data = trade_api.on_sync(req_auth, req_sync)
    #     print(data)

    # def test_onAccount(self, trade_api, req_auth, req_account):
    #     data = trade_api.on_account(req_auth, req_account)
    #     print(data)

    # def test_onMetrics(self, trade_api, req_auth, req_event):
    #     data = trade_api.on_metrics(req_auth, req_event)
    #     print(data)

