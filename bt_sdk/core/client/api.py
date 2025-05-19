# /usr/bin/env python3
# -*- coding: utf-8 -*-

import warnings
import itertools
import threading
import collections
from queue import Queue

from bt_sdk.meta import MetaParams, with_metaclass
from bt_sdk.core.client.async_client import AsyncDatagramClient, AsyncStreamClient
from bt_sdk.utils.wrapper import retry_connection, singleton
from bt_sdk.utils.diagnosal import on_ping


class MetaApi(MetaParams):
    
    def donew(cls, *args, **kwargs):
        """
            async_client / addr / client_id
        """
        _obj, args, kwargs = super(MetaApi, cls).donew(*args, **kwargs)
        
        if not hasattr(_obj, "connected"):
            raise ValueError("connected method is not implemented")

        async_client = AsyncDatagramClient if _obj.p.protocol == "udp" else AsyncStreamClient
        _obj.async_client = async_client(addr=_obj.p.addr)
        # init lock and tickerId
        _obj.qs = collections.OrderedDict()  # key: tickerId -> queues
        _obj._lock_q = threading.Lock()
        _obj.nextTickerId = itertools.count()

        return _obj, args, kwargs


class Api(with_metaclass(MetaApi, object)):

    params = (("client_id", ""), ("addr", ""), ("delay", "ms"))
    
    def __enter__(self):
        return self
    
    def getTickQueue(cls, start=False):
        '''Creates tick/Queue for data delivery to a data feed'''
        q = Queue()
        if start:
            q.put(None)
            return q

        with cls._lock_q:
            tickerId = next(cls.nextTickerId)
            cls.qs[tickerId] = q
        return q 
    
    def reuseQueue(cls, tickerId):
        '''Reuses queue for tickerId, returning the new tickerId and q'''
        with cls._lock_q:
            q = cls.qs.pop(tickerId, None)  # invalidate old
            tickerId = next(cls.nextTickerId)  # get new tickerId
            cls.qs[tickerId] = q  # Update qs: tickerId -> q
        return q
    
    def canceledData(self, q: Queue):
        """
            logic cliend_id: vector<queue> and put eof into queue of vector<queue>
        """
        warnings.warn("cancelData is not completed implemented")
        q.put("eof")

    @retry_connection(max_attempts=3, delay=1)
    def connected(self):
        """
            self.async_client.connected on ping
        """
        return on_ping(self.p.addr[0], self.p.delay)
    
    def disconnected(self):
        """
            disconnect from the server
        """
        self.async_client.stop()
   
    def __exit__(self, exc_type, exc_value, traceback):
        self.async_client.stop()
        self.qs.clear()
