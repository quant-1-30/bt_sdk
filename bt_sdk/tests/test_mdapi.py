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
    