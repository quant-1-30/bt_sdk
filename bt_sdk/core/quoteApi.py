# /usr/bin/env python3
# -*- coding: utf-8 -*-

from utils.dt_utilty import market_utc
from core.meta import QuoteMeta
from core.async_client import AsyncDatagramClient
from utils.wrapper import singleton


@singleton
class QuoteApi(object):
    """
        udp client 
    """
    def __init__(self):
        # provider_uri
        self.quote_client = AsyncDatagramClient(addr=("127.0.0.1",9999))

    def _on_async(self, req_meta: QuoteMeta):
        return self.quote_client.run(req_meta)

    def onSubCalendar(self, req_meta: QuoteMeta):
        # req = Quote(rpc_type="calendar", meta=req_meta)
        req = {"rpc_type": "calendar", 
               "meta": req_meta.model_dump()}
        calendars = self._on_async(req)
        return calendars
    
    def onSubAsset(self, req_meta: QuoteMeta):
        req = {"rpc_type": "instrument", 
               "meta": req_meta.model_dump()}
        instruments = self._on_async(req)
        return instruments

    # def onSubIntrady(self, req_meta: QuoteMeta):
    #     """
    #         tick data
    #     """
    #     assert req_meta.start_date  == req_meta.end_date, "start_date and end_date must be the same"
    #     s, e = market_utc(req_meta.start_date)
    #     meta = {"start_date": int(s.timestamp()), "end_date": int(e.timestamp()), "sid": req_meta.sid}
    #     req = QuoteEvent(rpc_type="dataset", meta=QuoteMeta(**meta))
    #     inner_ticks = self._on_async(req)
    #     return inner_ticks
    
    def onSubTicks(self, req_meta: QuoteMeta, intraday: bool = False):
        """
            tick data
        """
        if intraday:
            assert req_meta.start_date  == req_meta.end_date, "start_date and end_date must be the same"
            s, e = market_utc(req_meta.start_date)

            req_meta =  QuoteMeta(start_date=s, end_date=e, sid=req_meta.sid)

        req = {"rpc_type": "tick", 
               "meta": req_meta.model_dump()}
        ticks = self._on_async(req)
        return ticks
    
    def onSubEvent(self, event_type: str, req_meta: QuoteMeta):
        """
            adjustment / rightment
        """
        assert event_type in ["adjustment", "rightment"], "event_type must be adjustment or rightment"
        req = {"rpc_type": event_type, 
               "meta": req_meta.model_dump()}
        datas = self._on_async(req)
        return datas

quote_api = QuoteApi()

__all__ = ["quote_api"]
