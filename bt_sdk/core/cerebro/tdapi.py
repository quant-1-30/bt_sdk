# /usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Dict, Any, Tuple, Mapping, Union

from core.model import *
from utils.wrapper import singleton
from .api import Api


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

    def __init__(self, addr: Tuple[str, int]=()):
        self.addr = addr
    
    def on_login(self, msg: LoginMsg):
        """
            login
        """
        q = self.async_client.run(msg.model_dump())
        self.client_id = self.get_data(q)[0]

    def on_trade(self, msg: OrderMsg):
        """
            execution order in queue
        """
        q = self.async_client.run(msg.model_dump())
        trades = self.get_data(q)[0]
        return trades
        
    def on_timer(self, msg: TimerMsg):
        """
            sync position / account / fund
        """
        q = self.async_client.run(msg.model_dump())
        timers = self.get_data(q)[0]
        return timers


__all__ = ["TdApi"]

