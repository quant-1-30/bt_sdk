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

    def set_cash(self, msg: CashMeta):
        """
            set cash
        """
        rq = Request(topic="set_cash", msg=msg, client_id=self.p.client_id)
        _c = self.getChan()
        self.async_client.run(rq.model_dump(), _c)
        status = self.get_data(_c)
        return status

    def fetch(self, topic):
        """
            get n position and account 
        """
        topic = f"get_{topic}"
        rq = Request(topic=topic, msg=ReqMeta(), client_id=self.p.client_id)
        _c = self.getChan()
        self.async_client.run(rq.model_dump(), _c)
        data = self.get_data(_c)
        return data
           
    def subscribe(self, topic, msg:ReqMeta):
        """
            subscribe topic order / position / account
        """
        rq = Request(topic=f'query_{topic}', msg=msg, client_id=self.p.client_id)
        _c = self.getChan()
        self.async_client.run(rq.model_dump(), _c)
        return _c
    
    def trade(self, meta: OrderMeta):
        """
            execution order in queue
        """
        rq = Request(topic="order", msg=meta, client_id=self.p.client_id)
        _c = self.getChan()
        self.async_client.run(rq.model_dump(), _c)
        trades = self.get_data(_c)
        return trades
    
    def check(self, msg: ReqMeta):
        # a. check event_data and sync_account between sdate and edate
        # b. sync last date in case of asset delist
        rq = Request(topic="check", msg=msg, client_id=self.p.client_id)
        _c = self.getChan()
        self.async_client.run(rq.model_dump(), _c)
        status = self.get_data(_c)
        return status
    
    def cancel(self, q):
        super().cancel(q)


__all__ = ["TdApi"]

