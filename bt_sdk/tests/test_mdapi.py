#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest
from core.lib.mdapi import *
from core.model import *


class TestMdApi:

    def get_data(self, q):
        data = []
        while True:
            item = q.get()
            if item == "eof":
                break
            data.append(item)
        return data
    
    @pytest.fixture
    def md_api(self):
        return MdApi(addr=("127.0.0.1", 10000))
    
    @pytest.fixture
    def req_calendar(self):
        return Req(start_date=19900101, end_date=30000202)
    
    @pytest.fixture
    def req_contract(self):
        return Req(sid=[])
    
    @pytest.fixture
    def req_tick(self):
        return Req(start_date=1728351060, end_date=1728351600, sid=["603676"])
    
    @pytest.fixture
    def req_adjustment(self):
        return Req(start_date=1728351060, end_date=1728351600, sid=["603676"])
    
    @pytest.fixture
    def req_rightment(self):
        return Req(start_date=1728351060, end_date=1728351600, sid=["603676"])
    
    # # @pytest.mark.asyncio
    # # async def test_Calendar(self, md_api, req_cal):
    def test_Calendar(self, md_api, req_calendar):

        q = md_api.on_calendar(req_calendar)
        data = self.get_data(q)
        print("calendar", data)
        assert data is not None

    def test_Contract(self, md_api, req_contract):
        q = md_api.on_contract(req_contract)       
        data = self.get_data(q)
        print("contract", data)
        assert data is not None

    def test_RealTimeBar(self, md_api, req_tick):
        q = md_api.on_tick(req_tick)
        data = self.get_data(q)
        print("tick", data)
        assert data is not None

    def test_Adjustment(self, md_api, req_adjustment):
        q = md_api.on_adjustment(req_adjustment)
        data = self.get_data(q)
        print(data)
        assert data is not None

    def test_Rightment(self, md_api, req_rightment):
        q = md_api.on_rightment(req_rightment)
        data = self.get_data(q)
        print("rightment", data)
        assert data is not None
