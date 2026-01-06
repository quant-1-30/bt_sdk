# /usr/bin/env python3
# -*- coding: utf-8 -*-

from contextlib import contextmanager

# from core.client.data cimport Position, Trade, Account # data as package
cimport core.client.data as cdata
from core.client.util cimport fast_uuid4_bytes
from core.client.async_client cimport AsyncStreamClient


cdef class TdApi:
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
    
    def __init__(self, 
                    bytes client_id, 
                    tuple addr=("127.0.0.1", 9999), 
                    int timeout=5):
        self.client_id = client_id
        self.async_client = AsyncStreamClient(addr, timeout)
    
    cpdef object register(self, dict body):
        cdef bytes req_id = fast_uuid4_bytes()
        body["client_id"] = self.client_id
        cdef dict rq = {"topic": b"register", "body": body}

        obs = self.async_client.run(req_id, rq)
        return obs

    cpdef object set_cash(self, bytes experiment_id, dict body): # experiment_id
        cdef bytes req_id = fast_uuid4_bytes()
        cdef dict rq = {"topic": b"set_cash", "experiment_id": experiment_id, "body": body}

        obs = self.async_client.run(req_id, rq)
        return obs

    cpdef object getvalue(self, bytes experiment_id, str req_type):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef dict rq = {"topic": b"get_data", "experiment_id": experiment_id, "body": {"type": req_type}}

        obs = self.async_client.run(req_id, rq)
        return obs
    
    cpdef object subscribe(self, bytes experiment_id, dict body):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef dict rq = {"topic": b"subscribe", "experiment_id": experiment_id, "body": body}

        obs = self.async_client.run(req_id, rq)
        return obs
    
    cpdef object submit(self, bytes experiment_id, dict body):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef dict rq = {"topic": b"submit", "experiment_id": experiment_id, "body": body}

        obs = self.async_client.run(req_id, rq)
        return obs
    
    cpdef object on_dt_over(self, bytes experiment_id, dict body):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef dict rq = {"topic": b"on_dt_over", "experiment_id": experiment_id, "body": body}
        
        obs = self.async_client.run(req_id, rq)
        return obs
    
    cpdef void close(self):
        self.async_client.close()
