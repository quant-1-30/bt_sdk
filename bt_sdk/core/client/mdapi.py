# /usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
from bt_sdk.core.client.api import Api
from bt_sdk.core.model import ReqMeta, Request
from bt_sdk.utils.dt_utility import num2date


class MdApi(Api):
    params = (("protocol", "udp"),)

    def get_calendar(self): 
        """
            request calendar
        """
        msg = Request(topic='calendar', msg=ReqMeta())
        _c = self.getChan()
        self.async_client.run(msg.model_dump(), _c)
        cals = self.get_data(_c)
        return cals
    
    def get_instrument(self):
        """
            request instruments
        """
        msg = Request(topic='instrument', msg=ReqMeta())
        _c = self.getChan()
        self.async_client.run(msg.model_dump(), _c)
        instruments = self.get_data(_c)
        return instruments 

    def subscribe(self, msg:ReqMeta):
        """
            request market data
        """
        rq = Request(topic='tick', msg=msg)
        _c = self.getChan()
        self.async_client.run(rq.model_dump(), _c)
        return _c
    
    def get_event(self, topic, msg:ReqMeta):
        """
            request instruments
        """
        # import pdb; pdb.set_trace()
        _msg = ReqMeta(
            start_date=int(num2date(msg.start_date).strftime("%Y%m%d")), 
            end_date=int(num2date(msg.end_date).strftime("%Y%m%d")),
            sid=msg.sid
        )
        rq = Request(topic=topic, msg=_msg)
        _c = self.getChan()
        self.async_client.run(rq.model_dump(), _c)
        events = self.get_data(_c)
        return events 
    
    def get_close(self, msg: ReqMeta):
        """
            request instruments
        """
        rq = Request(topic='close', msg=msg)
        _c = self.getChan()
        self.async_client.run(rq.model_dump(), _c)
        closes = self.get_data(_c)
        return closes

    def factor(self, msg: ReqMeta): # sid
        close = self.get_close(msg)
        adjust = self.get_event("adjustment", msg)
        right = self.get_event("rightment", msg)
        from bt_sdk.core.helper.calc_factor import calc_factor
        factors = calc_factor(close, adjust, right)
        return factors
    
    def cancel(self, q):
        super().cancel(q)
    

__all__ = ["MdApi"]
