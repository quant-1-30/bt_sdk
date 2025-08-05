# /usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Tuple
from bt_sdk.core.client.api import Api
from bt_sdk.core.model import ReqMeta, Request


class MdApi(Api):
    params = (("protocol", "udp"),)

    def __init__(self, addr: Tuple[str, int]=(), client_id: str=""):
        self.addr = addr
        self.client_id = client_id
        self._init()
    
    def get_calendar(self): 
        """
            request calendar
        """
        msg = Request(topic='calendar', msg={}, client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        cals = self.get_data(q)
        return cals
    
    def get_instrument(self):
        """
            request instruments
        """
        msg = Request(topic='instrument', msg={}, client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        instruments = self.get_data(q)
        return instruments  
    
    def subscribe(self, msg: ReqMeta):
        """
            request market data
        """
        msg = Request(topic='tick', msg=msg, client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        return q
    
    def cancel(self, q):
        super().cancel(q)
    

__all__ = ["MdApi"]
