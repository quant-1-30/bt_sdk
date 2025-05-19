# /usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Dict, Any, Tuple, Mapping, Union

from bt_sdk.core.model import *
from bt_sdk.core.client.api import Api


# @singleton
class TdApi(Api):
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
    params = (("protocol", "tcp"),)

    def __init__(self, addr: Tuple[str, int]=(), client_id: str=""):
        self.addr = addr
        self.client_id = client_id

    def on_trade(self, meta: OrderMeta):
        """
            execution order in queue
        """
        msg = OrderMsg(topic="trade", msg=meta, client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        return q
    
    def reqOrder(self, meta: ReqMeta):
        """
            request order
        """
        msg = RequestMsg(topic="query_order", msg=meta, client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        return q
    
    def reqPosition(self, meta: ReqMeta):
        """
            request position
        """
        msg = RequestMsg(topic="query_position", msg=meta, client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        return q
    
    def reqAccount(self, meta: ReqMeta):
        """
            request account
        """
        msg = RequestMsg(topic="query_account", msg=meta, client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        return q
    
    def on_sync(self, meta: TimerMeta):
        """
            sync position / account on end of session
        """
        msg = SyncMsg(topic="sync", msg=meta, client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        return q


__all__ = ["TdApi"]

