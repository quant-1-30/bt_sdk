import pytest
import numpy as np
import datetime
from core.quoteApi import *
from core.meta import QuoteMeta

# reuse / dependency / setup / teardown

class TestQuoteApi:
    
    @pytest.fixture
    def req_calendar(self):
        # return ReqEvent(rpc_type="calendar", meta=Meta(start_date=19900101))
        return QuoteMeta(start_date=19900101, end_date=30000202)
    
    @pytest.fixture
    def req_asset(self):
        return QuoteMeta(sid=[])
    
    @pytest.fixture
    def req_tick(self):
        return QuoteMeta(start_date=1728351060, end_date=1728351600, sid=["603676"])
    
    @pytest.fixture
    def req_event(self):
        return "adjustment", QuoteMeta(start_date=1728351060, end_date=1728351600, sid=["603676"])
    
    # def test_onSubCalendar(self, req_calendar):
    #     data = quote_api.onSubCalendar(req_calendar)
    #     print(data)
    #     assert data is not None

    # def test_onSubAsset(self, req_asset):
    #     data = quote_api.onSubAsset(req_asset)
    #     print(data)

    # def test_onSubTicks(self, req_tick):
    #     data = quote_api.onSubTicks(req_tick)
    #     print(data)

    # def test_onSubEvent(self, req_event):
    #     quote_api.onSubEvent(*req_event)

