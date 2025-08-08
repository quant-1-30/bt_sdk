# /usr/bin/env python3
# -*- coding: utf-8 -*-

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
    params = (
        ("protocol", "tcp"),
        ("client_id", "")
              )

    def set_cash(self, session: int, cash: float):
        """
            set cash
        """
        msg = {"session": session, "cash": cash}
        msg = Request(topic="set_cash", msg=msg, client_id=self.p.client_id)
        _c = self.getChan()
        self.async_client.run(msg.model_dump(), _c)
        status = self.get_data(_c)
        return status

    def fetch_data(self, name):
        """
            get newest position and account 
        """
        topic = f"get_{name}"
        msg = Request(topic=topic, msg={}, client_id=self.p.client_id)
        _c = self.getChan()
        self.async_client.run(msg.model_dump(), _c)
        pos = self.get_data(_c)
        return pos
           
    def subscribe(self, topic: str, meta: ReqMeta):
        """
            subscribe topic order / position / account
        """
        msg = Request(topic=f'query_{topic}', msg=meta, client_id=self.p.client_id)
        _c = self.getChan()
        self.async_client.run(msg.model_dump(), _c)
        return _c
    
    def trade(self, meta: OrderMeta):
        """
            execution order in queue
        """
        msg = Request(topic="order", msg=meta, client_id=self.p.client_id)
        _c = self.getChan()
        self.async_client.run(msg.model_dump(), _c)
        trades = self.get_data(_c)
        return trades
    
    def check(self, sdate, edate):
        """
            check event_data and sync_account
        """
        msg = Request(topic="check", msg=[sdate, edate], client_id=self.p.client_id)
        _c = self.getChan()
        self.async_client.run(msg.model_dump(), _c)
        status = self.get_data(_c)
        return status
    
    def final(self, session):
        """
            amend position which asset is on delist
        """
        msg = Request(topic="patch", msg=session, client_id=self.p.client_id)
        _c = self.getChan()
        self.async_client.run(msg.model_dump(), _c)
        status = self.get_data(_c)
        return status
    
    def cancel(self, q):
        super().cancel(q)


__all__ = ["TdApi"]

