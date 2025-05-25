#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest
from bt_sdk.core.client import MdApi
from bt_sdk.core.model import *


def get_data(q):
    data = []
    while True:
        msg = q.get()
        if msg == "eof":
            q.reset()
            break
        data.append(msg)
    return data

class TestMdApi:
    
    @pytest.fixture
    def client_id(self):
        return "efe4eaee-0406-46e3-a395-91dc4502c4a3"
    
    @pytest.fixture
    def md_api(self, client_id):
        return MdApi(addr=("127.0.0.1", 8888), client_id=client_id)
    
    @pytest.fixture
    def session(self):
        return 20241008

    @pytest.fixture
    def subMeta(self):
        return ReqMeta(
                      start_date = 1728351060,
                      end_date = 1728351060,
                      sid = ['603676'])
    
    @pytest.fixture
    def reqmeta(self):
        return ReqMeta(
                      start_date = 19900101,
                      end_date = 20241008,
                    #   sid = ['603676'])
                      sid =[]) 
    
    def test_connect(self, md_api):
        assert md_api.connected()

    def test_getCalendar(self, md_api):
        q = md_api.getCalendar()
        data = get_data(q)
        print("test_getCalendar: ", data)
        assert data is not None

    def test_getInstrument(self, md_api, session):
        q = md_api.getInstrument(session)
        data = get_data(q)
        print("test_getInstrument: ", data)
        assert data is not None

    def test_getEvent(self, md_api, session):
        q = md_api.getEvent(session)
        data = get_data(q)
        print("test_getEvent: ", data)
        assert data is not None

    def test_subscribe(self, md_api, subMeta):
        q = md_api.subscribe(subMeta)
        data = get_data(q)
        print("test_subscribe: ", data)
        assert data is not None
