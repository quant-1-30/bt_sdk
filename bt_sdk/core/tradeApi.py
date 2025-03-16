# /usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Dict, Any
from urllib.parse import urljoin
from core.meta import LoginMeta, QuoteMeta, OrderMeta, AuthMeta, EventMeta, RangeMeta
from core.async_client import AsyncApiClient
from core.const import ApiEndpoint, ApiMethod
from core.quoteApi import quote_api
from utils.dt_utilty import market_utc
from utils.wrapper import singleton


@singleton
class TradeApi(object):
    """
    # How to implement a tradeApi:
    ---
    ## Basics
    A tradeApi should satisfies:
    * this class should be thread-safe:
        * all methods should be thread-safe
        * no mutable shared properties between objects.
    * all methods should be non-blocked
    * satisfies all requirements written in docstring for every method and callbacks.
    * automatically reconnect if connection lost.

    All the XxxData passed to callback should be constant, which means that
        the object should not be modified after passing to on_xxxx.
    So if you use a cache to store reference of data, use copy.copy to create a new object
    before passing that data into on_xxxx
    """
    __slots__ = ["async_client", "addr"]

    def __init__(self, addr="http://localhost:8100"):
        # validate and ssl
        self.async_client = AsyncApiClient(addr)
        self.addr = addr

    def on_trade(self, auth_meta: AuthMeta, order: OrderMeta):

        order_meta = order.model_dump()
        sid = order_meta.pop("sid")
        # build order_meta
        resp = quote_api.onSubAsset(QuoteMeta(sid=[sid]))
        asset_meta = resp[0][0]
        asset_meta.pop("name")

        order_meta["asset"] = asset_meta

        # intraday tickes
        intraday_ticks = quote_api.onSubTicks(QuoteMeta(sid=[sid], 
                                                       start_date=int(order.created_at),
                                                       end_date=int(order.created_at)), 
                                                       intraday=True)

        # params = {"auth": auth_meta.model_dump(),
        params = {
            "experiment_id": auth_meta.experiment_id,
            "orderMeta": order_meta, 
            "meta": intraday_ticks
        }

        # transaction status
        req = {"endpoint": urljoin(self.addr, ApiEndpoint.TRADE.value), 
               "method": ApiMethod.POST.value, 
               "headers": {"Authorization": "Bearer a8894326-edf0-48ff-8a15-ca82e2d8a74f"},
               "params": params}
        resp = self.async_client.run(req)
        return resp
    
    def on_event(self, auth_meta: AuthMeta, event_meta: EventMeta):

        event_data = quote_api.onSubEvent(event_meta.event_type, event_meta.meta)
        params = {
            "experiment_id": auth_meta.experiment_id,
            "meta": event_data,
            "event_type": event_meta.event_type
        }

        req = {"endpoint": urljoin(self.addr, ApiEndpoint.EVENT.value), 
               "method": ApiMethod.POST.value, 
               "headers": {"Authorization": "Bearer a8894326-edf0-48ff-8a15-ca82e2d8a74f"},
               "params": params}
        resp = self.async_client.run(req)
        return resp
    
    def on_query(self, auth_meta: AuthMeta, meta: RangeMeta) -> Dict[str, Any]:
        """
            sync close and dividend or right
        """
        params = {
            "experiment_id": auth_meta.experiment_id,
            "meta": meta.model_dump()
        }
        req = {"endpoint": urljoin(self.addr, ApiEndpoint.ACCOUNT.value), 
               "method": ApiMethod.GET.value,
               "headers": {"Authorization": "Bearer a8894326-edf0-48ff-8a15-ca82e2d8a74f"},
               "params": params}
        resp = self.async_client.run(req)
        return resp
    