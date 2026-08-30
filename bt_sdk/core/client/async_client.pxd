cdef class AsyncClient:
    cdef bint _running
    cdef object listen_task
    cdef object loop
    cdef object _loop_thread

    cdef void _finalize_task(self, object future)
    
    cdef object wrap_protocol(self, bytes req_id, object msg)
    
    cpdef object run(self, bytes req_id, object msg)
    
    cdef _on_cleanup_done(self, task)
    
    cpdef void close(self)

    
cdef class AsyncRpcClient(AsyncClient):
    cdef int timeout
    cdef bint _connected
    cdef object _conn_lock
    cdef object _pending_tasks
    cdef object loop
    cdef object rpc_client

    cpdef void attach_loop(self, loop)
    cpdef void reset_connection(self)

    cdef object wrap_protocol(self, bytes req_id, object msg) # virtual / cython not supported nested function 
    
    cdef inline void _safe_on_completed(self, object subject)

    cdef inline void _safe_on_error(self, object subject, object error)
    