#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest
from core.cerebro.mdapi import *
from core.model import *


class TestMdApi:
    
    @pytest.fixture
    def md_api(self):
        return MdApi(addr=("127.0.0.1", 10000))
    
    @pytest.fixture
    def request(self):
        return RequestMsg(topic="calendar", 
                          msg=ReqMeta(
                              client_id="test",
                              sub_topic="calendar",
                            #   sub_topic="instrm",
                            #   sub_topic="tick",
                            #   sub_topic="adjustment",
                            #   sub_topic="rightment",
                              start_time=19900101, 
                              end_time=20241008,
                              sids = ['603676']))

    def test_request(self, md_api, request):
        q = md_api.on_request(request)
        data = self.get_data(q)
        print(data)
        assert data is not None
