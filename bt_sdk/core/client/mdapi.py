# /usr/bin/env python3
# -*- coding: utf-8 -*-

from bt_sdk.core.client.api import Api
from bt_sdk.core.model import ReqMeta, Request


class MdApi(Api):
    params = (("protocol", "udp"),)

    def get_calendar(self): 
        """
            request calendar
        """
        msg = Request(topic='calendar', msg={})
        _c = self.getChan()
        self.async_client.run(msg.model_dump(), _c)
        cals = self.get_data(_c)
        return cals
    
    def get_instrument(self):
        """
            request instruments
        """
        msg = Request(topic='instrument', msg={})
        _c = self.getChan()
        self.async_client.run(msg.model_dump(), _c)
        instruments = self.get_data(_c)
        return instruments  
    
    def subscribe(self, msg: ReqMeta):
        """
            request market data
        """
        msg = Request(topic='tick', msg=msg)
        _c = self.getChan()
        self.async_client.run(msg.model_dump(), _c)
        return _c
    
    def cancel(self, q):
        super().cancel(q)
    

__all__ = ["MdApi"]
