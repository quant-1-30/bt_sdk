# /usr/bin/env python3
# -*- coding: utf-8 -*-
from contextlib import contextmanager

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
        ("timeout", 5.0),  # Default timeout 
        ("client_id", "")
        )

    def set_cash(self, meta: CashMeta):
        """
            set cash
        """
        rq = Request(topic="set_cash", body=meta, client_id=self.p.client_id)
        chan = self.get_channel()
        self.async_client.run(rq.model_dump(), chan)
        status = self.get_data(chan)
        return status

    def fetch(self, topic):
        """
            get n position and account 
        """
        rq = Request(topic=f"get_{topic}", body=ReqMeta(), client_id=self.p.client_id)
        chan = self.get_channel()
        self.async_client.run(rq.model_dump(), chan)
        raw_data = self.get_data(chan)
        if not raw_data:
            return None
        o = Account(**raw_data[0]["body"]) if topic == "account" else Position(**raw_data[0]["body"])
        return o

    @contextmanager   
    def subscribe(self, topic, meta:ReqMeta):
       """
           subscribe topic order / position / account
       """
       rq = Request(topic=f'query_{topic}', body=meta, client_id=self.p.client_id)
       chan = self.get_channel()
       self.async_client.run(rq.model_dump(), chan)
       try:
           yield chan
       finally:
           self.cancel(chan)
    
    def submit(self, meta: OrderMeta):
        """
            execution order in queue
        """
        rq = Request(topic="order", body=meta, client_id=self.p.client_id)
        chan = self.get_channel()
        self.async_client.run(rq.model_dump(), chan)
        raw_data = self.get_data(chan)
        if not raw_data:
            return None
        bits = [OrderBit(**d["body"]) for d in raw_data]
        return bits
    
    def chain(self, meta: ReqMeta):
        # a. check event_data and sync_account between sdate and edate
        # b. sync last date in case of asset delist
        rq = Request(topic="chain", body=meta, client_id=self.p.client_id)
        chan = self.get_channel()
        self.async_client.run(rq.model_dump(), chan)
        status = self.get_data(chan)
        return status
    
    def cancel(self, q):
        super().cancel(q)


__all__ = ["TdApi"]

