from libc.stdint cimport int32_t


cdef class MdApi:
    cdef bint _is_initialized
    cdef public bint _is_shared
    cdef public int32_t timeout
    cdef public object loop
    cdef public object async_client
    
    cpdef start(self, object loop)

    cpdef object get_instrument(self, int32_t timeout=*)

    cpdef object get_factor(self, object body, int32_t forward_type, int32_t timeout=*)
    
    cpdef object subscribe(self, object body, int32_t topic)
    
    cpdef void disconnect(self)
