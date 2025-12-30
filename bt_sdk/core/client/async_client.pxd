#!/usr/bin/env python3
# -*- coding: utf-8 -*-

cdef class AsyncClient:
    cdef object _global_bus
    cdef bint _running
    cdef object loop
    cdef object _loop_thread
    
    cdef void _init_event_loop(self)

    cdef void _run_event_loop(self)

    cdef void _finalize_task(self, object future)

    cpdef void close(self)


cdef class AsyncZmqClient(AsyncClient):
    cdef str addr
    cdef int timeout
    cdef object context
    cdef object socket
    
    cpdef void close(self) # virtual due to cython not supported nested function
    
    cdef void shutdown_async_resources(self)


cdef class AsyncStreamClient(AsyncClient):
    cdef str host
    cdef int port
    cdef object _connection_cache
    cdef int timeout
    
    cpdef void close(self) # virtual
