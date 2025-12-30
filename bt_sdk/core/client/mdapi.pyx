# /usr/bin/env python3
# -*- coding: utf-8 -*-

from contextlib import contextmanager

from core.client.async_client cimport AsyncZmqClient 
from core.client.util cimport fast_uuid4_bytes


cdef class MdApi:

    def __init__(self,
                    tuple addr=("127.0.0.1", 8888),
                    int timeout = 5):
        self.async_client = AsyncZmqClient(addr=addr, timeout=timeout)

    cpdef object get_calendar(self): 
        """
            request calendar
        """
        cdef bytes req_id = fast_uuid4_bytes()
        cdef dict rq = {"topic": "calendar"}

        obs = self.async_client.run(req_id, rq)
        return obs
    
    cpdef object get_instrument(self):
        """
            request instruments
        """
        cdef bytes req_id = fast_uuid4_bytes()
        cdef dict rq = {"topic": "instrument"}

        obs = self.async_client.run(req_id, rq)
        return obs
    
    cpdef object get_benchmark(self, bytes index):
        """
            request index 000001 000680 399006 399001
        """
        cdef bytes req_id = fast_uuid4_bytes()
        cdef dict rq = {"topic": "index", "body": {"sid": [index]}}

        obs = self.async_client.run(req_id, rq)
        return obs 
    
    cpdef object get_event(self, str topic, dict body):
        """
            request instruments
        """
        cdef bytes req_id = fast_uuid4_bytes()
        cdef dict rq = {"topic": topic, "body": body}
        
        obs = self.async_client.run(req_id, rq)
        return obs

    # @contextmanager
    cpdef object subscribe(self, dict body):
        """使用迭代器模式替代上下文管理器"""
        cdef bytes req_id = fast_uuid4_bytes()
        cdef dict rq = {"topic": "tick", "body": body}

        obs = self.async_client.run(req_id, rq)
        return obs
        
    cpdef object get_close(self, dict body):
        """
            request instruments
        """
        cdef bytes req_id = fast_uuid4_bytes()
        cdef dict rq = {"topic": "close", "body": body}
        
        obs = self.async_client.run(req_id, rq)
        return obs

    #cpdef dict factor(self, body: Query): # sid
    #    close = self.get_close(body)
    #    adjust = self.get_event("adjustment", body)
    #    right = self.get_event("rightment", body)
    #    from bt_sdk.core.helper.factor import calc_factor
    #    factors = calc_factor(close, adjust, right)
    #    return factors
    
    cpdef void close(self):
        self.client.close()


__all__ = ["MdApi"]
