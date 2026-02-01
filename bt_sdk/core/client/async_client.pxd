#!/usr/bin/env python3
# -*- coding: utf-8 -*-


cdef class AsyncClient:
    cdef bint _running
    cdef object listen_task
    cdef object loop
    cdef object _loop_thread

    cdef void _finalize_task(self, object future)
    
    cdef object wrap_protocol(self, bytes req_id, object msg)
    
    cpdef object run(self, bytes req_id, object msg)

    cpdef void close(self)

    
cdef class AsyncStreamClient(AsyncClient):
    cdef str host
    cdef int port
    cdef bint _initialized
    cdef bint is_background_loop
    cdef object _conn_lock
    cdef object _connection_cache
    cdef int timeout
    cdef object _req_futures
    cdef object loop
    cdef object listen_task
    
    cpdef void attach_loop(self, loop, bint is_background=?)

    cdef object wrap_protocol(self, bytes req_id, object msg)


cdef class AsyncRpcClient(AsyncClient):
    cdef str addr
    cdef int timeout
    cdef bint is_background_loop
    cdef bint _connected
    cdef object loop
    cdef object rpc_client
    
    cpdef void attach_loop(self, loop, bint is_background=?)

    cdef object wrap_protocol(self, bytes req_id, object msg) # virtual / cython not supported nested function 