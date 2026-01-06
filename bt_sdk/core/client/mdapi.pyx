# /usr/bin/env python3
# -*- coding: utf-8 -*-

import reactivex.operators as ops
import pyarrow as pa
# from contextlib import contextmanager

from core.client.async_client cimport AsyncZmqClient 
from core.client.util cimport fast_uuid4_bytes


cdef inline object obs2table(object observable):
    cdef list result
    result = observable.pipe(
        ops.to_list() # ops.to_list() pack into list end of 
    ).run()
    if result:
        # return pa.Table.from_batches(result)  # pa.RecordBatch
        return pa.concat_tables(result) # zero_copy accumlate chunk ptr not reallocate / just when combine_chunks() 
    return {}


cdef class MdApi:

    def __init__(self,
                    tuple addr=("127.0.0.1", 8888),
                    int timeout = 5):
        self.async_client = AsyncZmqClient(addr=addr, timeout=timeout)

    def __enter__(self):
        return self 

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

    cpdef object get_factor(self, dict body):
        cdef object obs_close, obs_adjust, obs_right
        cdef object close, adjust, right

        obs_close = self.get_close(body)
        obs_adjust = self.get_event("adjustment", body)
        obs_right = self.get_event("rightment", body)
        close = obs2table(obs_close)
        adjust = obs2table(obs_adjust)
        right = obs2table(obs_right)
        from bt_sdk.core.helper.factor import calc_factor
        factors = calc_factor(close, adjust, right)
        return factors
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            print(f"Error: {exc_type}, {exc_val}, {exc_tb}")
        self.async_client.close()


__all__ = ["MdApi"]
