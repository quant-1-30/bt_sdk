# /usr/bin/env python3
# -*- coding: utf-8 -*-

from core.async_client import AsyncDatagramClient
from core.model import *
from utils.wrapper import singleton
from utils.dt_utilty import market_utc


@singleton
class QuoteApi(object):
    """
        udp client 
    """
    def __init__(self):
        # provider_uri
        self.quote_client = AsyncDatagramClient(addr=("127.0.0.1",9999))

    def onSubCalendar(self, req_meta: QuoteMeta):
        # req = QuoteEvent(rpc_type="calendar", meta=req_meta)
        event = QuoteEvent(event_type=QuoteType.Calendar, meta=req_meta)
        calendars = self._on_callback(event)
        return calendars

    def onSubAsset(self, req_meta: QuoteMeta):
        event = QuoteEvent(event_type=QuoteType.Instrument, meta=req_meta)
        instruments = self._on_callback(event)
        return instruments
    
    def onSubTicks(self, req_meta: QuoteMeta):
        """
            tick data
        """
        if req_meta.start_date  == req_meta.end_date:
            s, e = market_utc(req_meta.start_date)
            req_meta = QuoteMeta(start_date=s, end_date=e, sid=req_meta.sid)

        event = QuoteEvent(event_type=QuoteType.Tick, meta=req_meta)
        ticks = self._on_callback(event)
        return ticks
    
    def onSubEvent(self, event_type: str, req_meta: QuoteMeta):
        """
            adjustment / rightment
        """
        assert event_type in ["adjustment", "rightment"], "event_type must be adjustment or rightment"
        event = QuoteEvent(event_type=event_type, meta=req_meta)
        datas = self._on_callback(event)
        return datas
    
    def _on_callback(self, event: QuoteEvent):
        data = self.quote_client.run(event)
        return data


quote_api = QuoteApi()

__all__ = ["quote_api"]
