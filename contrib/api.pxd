cdef class TdApi:
    cdef bint is_background
    cdef bytes client_id
    cdef public object loop
    cdef object async_client
    
    cdef object _send_request(self, int topic, bytes experiment_id=*, object body=*, int sub_topic=*)

    cpdef object register(self, object body)

    cpdef object set_cash(self, bytes experiment_id, object body)

    cpdef object getvalue(self, bytes experiment_id)

    cpdef object subscribe(self, int topic, bytes experiment_id, object body)

    cpdef object submit(self, bytes experiment_id, object body)

    cpdef object on_dt_over(self, bytes experiment_id, object body)

    cpdef void disconnect(self)