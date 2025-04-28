#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest
from core.client.mdapi import *
from core.model import *


class TestMdApi:
    
    @pytest.fixture
    def md_api(self):
        return MdApi(addr=("127.0.0.1", 10000))
    
    @pytest.fixture
    def req_topic(self):
        #   topic="calendar", 
        #   topic="instrument", 
        #   topic="adjustment", 
        #   topic="rightment", 
        return "tick"
    
    @pytest.fixture
    def reqmeta(self):
        return ReqMeta(
                      # start_date=19900101, 
                      # end_date=20241008,
                      start_date = 1728351060,
                      end_date = 1728351060,
                      sid = ['603676'])

    def test_request(self, md_api, req):
        data = md_api.on_request(req)
        print("data: ", data)
        assert data is not None
