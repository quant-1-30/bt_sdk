# /usr/bin/env python3
# -*- coding: utf-8 -*-

import warnings
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
        self._init()

    def getAccount(self):
        """
            latest account_info fundvalue and cash
        """
        msg = RequestMsg(topic="get_account", msg={}, client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        return q
    
    def getPosition(self):
        """
            latest position_info 
        """
        msg = RequestMsg(topic="get_position", msg={}, client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        return q
          
    def subscribe(self, topic: str, meta: ReqMeta):
        """
            subscribe topic order / position / account
        """
        msg = RequestMsg(topic=f'query_{topic}', msg=meta, client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        return q
    
    def placeOrder(self, meta: OrderMeta):
        """
            execution order in queue
        """
        msg = OrderMsg(topic="order", msg=meta, client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        return q
    
    def onTimer(self, session: int):
        """
            sync position / account on end of session
        """
        msg = TimerMsg(topic="timer", msg=session, client_id=self.client_id)
        q = self.getTickQueue()
        self.async_client.run(msg.model_dump(), q)
        return q
    
    def cancel(self, q):
        super().cancel(q)


__all__ = ["TdApi"]

