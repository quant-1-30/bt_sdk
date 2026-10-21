cdef class AsyncClient:
    cdef bint _running
    cdef public int timeout  # declared here (not on subclasses): AsyncClient.close() reads it
    cdef object listen_task
    cdef object _shutdown_task
    cdef object loop

    cdef void _finalize_task(self, object future)
    
    cdef object wrap_protocol(self, bytes req_id, object msg)
    
    cpdef object run(self, bytes req_id, object msg)
    
    cdef _on_cleanup_done(self, task)
    
    cpdef void close(self)

    
cdef class AsyncRpcClient(AsyncClient):
    # timeout + loop are inherited from AsyncClient (declaring them here would
    # shadow the base slots and break AsyncClient.close())
    cdef bint _connected
    cdef object _conn_lock
    cdef object _pending_tasks
    cdef object _pending_futs
    cdef object rpc_client

    cpdef void attach_loop(self, loop)
    cpdef void revive(self)
    cpdef void reset_connection(self)

    cdef object wrap_protocol(self, bytes req_id, object msg) # virtual / cython not supported nested function 
    
    cdef inline void _safe_on_completed(self, object subject)

    cdef inline void _safe_on_error(self, object subject, object error)

    cdef inline object _submit_cross_loop(self, object coro)

    cdef inline void _cancel_cross_loop(self, object fut)
    