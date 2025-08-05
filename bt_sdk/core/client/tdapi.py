# /usr/bin/env python3
# -*- coding: utf-8 -*-

import warnings
from typing import Dict, Any, Tuple, Mapping, Union

from bt_sdk.core.model import *
from bt_sdk.core.client.api import Api


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
        self._init()

    def set_cash(self, session: int, cash: float):
        """
            set cash
        """
        msg = {"session": session, "cash": cash}
        msg = Request(topic="set_cash", msg=msg, client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        status = self.get_data(q)
        return status

    def get_account(self):
        """
            latest account_info fundvalue and cash
        """
        msg = Request(topic="get_account", msg={}, client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        act = self.get_data(q)
        return act
    
    def get_position(self):
        """
            latest position_info 
        """
        msg = Request(topic="get_position", msg={}, client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        pos = self.get_data(q)
        return pos
           
    def trade(self, meta: OrderMeta):
        """
            execution order in queue
        """
        msg = Request(topic="order", msg=meta, client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        trades = self.get_data(q)
        return trades
    
    def subscribe(self, topic: str, meta: ReqMeta):
        """
            subscribe topic order / position / account
        """
        msg = Request(topic=f'query_{topic}', msg=meta, client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        return q
    
    def check(self, sdate, edate):
        """
            check event_data and sync_account
        """
        msg = Request(topic="check", msg=[sdate, edate], client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        status = self.get_data(q)
        return status
    
    def patch(self, session):
        """
            amend position which asset is on delist
        """
        msg = Request(topic="patch", msg=session, client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        status = self.get_data(q)
        return status
    
    def cancel(self, q):
        super().cancel(q)


__all__ = ["TdApi"]

