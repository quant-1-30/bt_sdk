# /usr/bin/env python3
# -*- coding: utf-8 -*-

from contextlib import contextmanager
from bt_sdk.core.client.api import Api
from bt_sdk.core.model import ReqMeta, Request
from bt_sdk.utils.dt_utility import num2date


class MdApi(Api):
    params = (
            ("protocol", "zmq"),
            ("timeout", -1),  # Default timeout
              )

    def get_calendar(self): 
        """
            request calendar
        """
        msg = Request(topic='calendar', body=ReqMeta())
        chan = self.get_channel()
        self.async_client.run(msg.model_dump(), chan)
        cals = self.get_data(chan)
        return cals
    
    def get_instrument(self):
        """
            request instruments
        """
        rq = Request(topic='instrument', body=ReqMeta())
        chan = self.get_channel()
        self.async_client.run(rq.model_dump(), chan)
        instruments = self.get_data(chan)
        return instruments 

    @contextmanager
    def subscribe(self, meta:ReqMeta):
        """
            request market data
        """
        rq = Request(topic='tick', body=meta)
        chan = self.get_channel()
        self.async_client.run(rq.model_dump(), chan)
        try:
            yield chan
        finally:
            self.cancel(chan)
    
    def get_event(self, topic, meta:ReqMeta):
        """
            request instruments
        """
        # import pdb; pdb.set_trace()
        event_meta = ReqMeta(
            start_date=int(num2date(meta.start_date).strftime("%Y%m%d")), 
            end_date=int(num2date(meta.end_date).strftime("%Y%m%d")),
            sid=meta.sid
        )
        rq = Request(topic=topic, body=event_meta)
        chan = self.get_channel()
        self.async_client.run(rq.model_dump(), chan)
        events = self.get_data(chan)
        return events 
    
    def get_close(self, meta: ReqMeta):
        """
            request instruments
        """
        rq = Request(topic='close', body=meta)
        chan = self.get_channel()
        self.async_client.run(rq.model_dump(), chan)
        closes = self.get_data(chan)
        return closes

    def factor(self, meta: ReqMeta): # sid
        close = self.get_close(meta)
        adjust = self.get_event("adjustment", meta)
        right = self.get_event("rightment", meta)
        from bt_sdk.core.helper.factor import calc_factor
        factors = calc_factor(close, adjust, right)
        return factors
    
    def cancel(self, q):
        super().cancel(q)
    

__all__ = ["MdApi"]
