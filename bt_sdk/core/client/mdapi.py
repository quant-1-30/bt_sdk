# /usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Tuple
from bt_sdk.core.client.api import Api
from bt_sdk.core.model import ReqMeta, RequestMsg


class MdApi(Api):
    params = (("protocol", "udp"),)

    def __init__(self, addr: Tuple[str, int]=(), client_id: str=""):
        self.addr = addr
        self.client_id = client_id
    
    def get_calendar(self): 
        """
            request calendar
        """
        msg = RequestMsg(topic='calendar', msg=ReqMeta(), client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        return q
    
    def get_instrument(self, msg=ReqMeta()):
        """
            request instruments
        """
        msg = RequestMsg(topic='instrument', msg=msg, client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        return q
    
    def reqMktData(self, msg: ReqMeta):
        """
            request market data
        """
        msg = RequestMsg(topic='tick', msg=msg, client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        return q
    
    def reqEvents(self, msg: ReqMeta):
        """
            request adjustment and right events  
        """
        msg = RequestMsg(topic='adjustment', msg=msg, client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        return q
    

__all__ = ["MdApi"]
