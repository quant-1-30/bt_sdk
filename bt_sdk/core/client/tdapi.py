# /usr/bin/env python3
# -*- coding: utf-8 -*-

from contextlib import contextmanager

from typing import List, Dict, Any, Union, Generator
from bt_sdk.core.model import *
from bt_sdk.core.data import *
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
    
    def register(self, body: Experiment) -> Resp:
        rq = Request(topic="register", body=body, experiment_id="register")  # use api method as experiment_id where is null
        chan = self.get_channel()
        self.async_client.run(rq.model_dump(), chan)
        datas = self.get_data(chan)
        if not datas:
            return None
        resp = Resp(datas[0]["body"])
        return resp

    def set_cash(self, body: Cash, experiment_id) -> Resp:
        """
            set cash
        """
        rq = Request(topic="set_cash", body=body, experiment_id=experiment_id)
        chan = self.get_channel()
        self.async_client.run(rq.model_dump(), chan)
        datas = self.get_data(chan)
        if not datas:
            return None
        resp = Resp(datas[0]["body"])
        return resp

    def getvalue(self, topic, experiment_id='') -> List[Union[Account, Position]]:
        """
            get n position and account 
        """
        resp = None
        rq = Request(topic=f"get_{topic}", body=Query(), experiment_id=experiment_id)
        chan = self.get_channel()
        self.async_client.run(rq.model_dump(), chan)
        datas = self.get_data(chan)
        if not datas:
            return None
        resp = [Account(**d[0]["body"]) if topic == "account" else Position(**resp[0]["body"]) for d in datas]
        return resp

    @contextmanager   
    def subscribe(self, topic, body:Query, experiment_id) -> Generator[Any, None, None]:
       """
           subscribe topic order / position / account
       """
       rq = Request(topic=f'query_{topic}', body=body, experiment_id=experiment_id)
       chan = self.get_channel()
       self.async_client.run(rq.model_dump(), chan)
       try:
           yield chan
       finally:
           self.cancel(chan)
    
    def submit(self, body: Order, experiment_id) -> List[Trade]:
        """
            execution order in queue
        """
        rq = Request(topic="submit", body=body, experiment_id=experiment_id)
        chan = self.get_channel()
        self.async_client.run(rq.model_dump(), chan)
        datas = self.get_data(chan)
        if not datas:
            return None
        resp = [Trade(**d["body"]) for d in datas]
        return resp
    
    def on_dt_over(self, body: Query, experiment_id:str) -> Resp:
        rq = Request(topic="on_dt_over", body=body, experiment_id=experiment_id)
        chan = self.get_channel()
        self.async_client.run(rq.model_dump(), chan)
        datas = self.get_data(chan)
        if not datas:
            return None
        resp = Resp(body=datas[0]["body"])
        return resp
    
    def cancel(self, q):
        super().cancel(q)


__all__ = ["TdApi"]

