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
    
    def register(self, meta: ExpMeta):
        rq = Request(topic="register", body=meta) 
        chan = self.get_channel()
        self.async_client.run(rq.model_dump(), chan)
        resp = self.get_data(chan)
        print("register resp ", resp)
        if "experiment_id" in resp[0]["body"]:
            self.experiment_id = resp[0]["body"]["experiment_id"] 

    def set_cash(self, meta: CashMeta):
        """
            set cash
        """
        rq = Request(topic="set_cash", body=meta, experiment_id=self.experiment_id)
        chan = self.get_channel()
        self.async_client.run(rq.model_dump(), chan)
        resp = self.get_data(chan)
        return resp

    def fetch(self, topic):
        """
            get n position and account 
        """
        rq = Request(topic=f"get_{topic}", body=ReqMeta(), experiment_id=self.experiment_id)
        chan = self.get_channel()
        self.async_client.run(rq.model_dump(), chan)
        resp = self.get_data(chan)
        if not resp:
            return None
        o = Account(**resp[0]["body"]) if topic == "account" else Position(**resp[0]["body"])
        return o

    @contextmanager   
    def subscribe(self, topic, meta:ReqMeta):
       """
           subscribe topic order / position / account
       """
       rq = Request(topic=f'query_{topic}', body=meta, experiment_id=self.experiment_id)
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
        rq = Request(topic="submit", body=meta, experiment_id=self.experiment_id)
        chan = self.get_channel()
        self.async_client.run(rq.model_dump(), chan)
        resp = self.get_data(chan)
        if not resp:
            return None
        bits = [OrderBit(**d["body"]) for d in resp]
        return bits
    
    def on_dt_over(self, meta: ReqMeta):
        # a. check event_data and sync_account between sdate and edate
        # b. sync last date in case of asset delist
        rq = Request(topic="on_dt_over", body=meta, experiment_id=self.experiment_id)
        chan = self.get_channel()
        self.async_client.run(rq.model_dump(), chan)
        resp = self.get_data(chan)
        return resp
    
    def cancel(self, q):
        super().cancel(q)


__all__ = ["TdApi"]

