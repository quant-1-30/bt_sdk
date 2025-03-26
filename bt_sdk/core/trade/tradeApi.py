# /usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Dict, Any, Tuple, Mapping

from core.async_client import AsyncStreamClient
from core.model import *
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

    def __init__(self, addr: Tuple[str, int]):
        self.async_client = AsyncStreamClient(addr)

    def on_trade(self, metadata: Mapping[str, Any]):
        """
            order event
        """
        order_meta = OrderMeta(**metadata)
        event = TradeEvent(event_type=TradeType.Order, meta=order_meta)
        data = self._on_callback(event)
        return data
        
    def on_end_of_session(self, metadata: Mapping[str, Any]):
        """
            eos event
        """
        eos_meta = EosMeta(**metadata)
        event = TradeEvent(event_type=TradeType.Eos, meta=eos_meta)
        data = self._on_callback(event)
        return data
    
    def on_query(self, metadata: Mapping[str, Any]) -> Dict[str, Any]:
        """
            vtorder / position / fund
        """
        query_meta = QueryMeta(**metadata)
        event = TradeEvent(event_type=TradeType.Query, meta=query_meta)
        data = self._on_callback(event)
        return data

    def _on_callback(self, event: TradeEvent):
        data = self.async_client.run(event)
        return data
    
trade_api = TradeApi()

__all__ = ["trade_api"]

