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
                    str client_id, 
                    tuple addr=("127.0.0.1", 9999), 
                    int timeout=5):
        self.client_id = client_id
        self.async_client = AsyncStreamClient(addr, timeout)
    
    cpdef object register(self, dict body):
        cdef bytes req_id = fast_uuid4_bytes()

        body["topic"] = "register"
        obs = self.async_client.run(req_id, body)
        return obs

    cpdef object set_cash(self, dict body): # experiment_id
        cdef bytes req_id = fast_uuid4_bytes()

        body["topic"] = "set_cash"
        obs = self.async_client.run(req_id, body)
        return obs

    cpdef object getvalue(self, dict body):
        cdef bytes req_id = fast_uuid4_bytes()

        body["topic"] = "get_data"
        obs = self.async_client.run(req_id, body)
        return obs
    
    # @contextmanager   
    cpdef object subscribe(self, dict body):
        cdef bytes req_id = fast_uuid4_bytes()

        obs = self.async_client.run(req_id, body)
        return obs
    
    cpdef object submit(self, dict body):
        cdef bytes req_id = fast_uuid4_bytes()

        body["topic"] = "order"
        obs = self.async_client.run(req_id, body)
        return obs
    
    cpdef object on_dt_over(self, dict body):
        cdef bytes req_id = fast_uuid4_bytes()
        
        body["topic"] = "on_dt_over"
        obs = self.async_client.run(req_id, body)
        return obs
    
    cpdef void close(self):
        self.async_client.close()
