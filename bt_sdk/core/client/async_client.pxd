#!/usr/bin/env python3
# -*- coding: utf-8 -*-


cdef class AsyncClient:
    cdef object _req_subject 
    cdef object _req_futures 
    cdef bint _running
    cdef object loop
    cdef object _loop_thread
    
    cdef void _init_event_loop(self)

    cdef void _run_event_loop(self)

    cdef void _finalize_task(self, object future)
    
    cdef object wrap_protocol(self, bytes req_id, object msg)
    
    cpdef object run(self, bytes req_id, object msg)

    cpdef void close(self)


cdef class AsyncZmqClient(AsyncClient):
    cdef str addr
    cdef int timeout
    cdef object context
    cdef object socket
    cdef readonly object listen_task
    
    cdef object wrap_protocol(self, bytes req_id, object msg) # virtual / cython not supported nested function
    
    
cdef class AsyncStreamClient(AsyncClient):
    cdef str host
    cdef int port
    cdef object _conn_lock
    cdef object _connection_cache
    cdef int timeout
    cdef readonly object listen_task
    
    cdef object wrap_protocol(self, bytes req_id, object msg)
    