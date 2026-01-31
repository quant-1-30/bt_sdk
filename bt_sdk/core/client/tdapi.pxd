cdef enum BrokerTopic:
    Register = 0
    SetCash = 1
    Submit = 2
    DayOver = 3 
    GetValue = 4
    Subscribe = 5

cpdef enum SubTopic:
    Order = 0
    Position = 1
    Account = 2

cpdef enum OrderType:
    Buy = 0
    Sell = 1
    Unkown = 2

cpdef enum ExecType:
    Market = 0
    Close = 1
    Limit = 2
    Stop = 3
    StopLimit = 4
    StopTrail = 5
    StopTrailLimit = 6
    Historical = 7


cdef class TdApi:
    cdef bytes client_id
    cdef object async_client
    cdef object _loop_thread
    
    cdef void _run_loop_thread(self, object loop)
    
    cdef object _send_request(self, int topic, bytes experiment_id=*, object body=*, int sub_topic=*)

    cpdef object register(self, object body)

    cpdef object set_cash(self, bytes experiment_id, object body)

    cpdef object getvalue(self, int topic, bytes experiment_id)

    cpdef object subscribe(self, int topic, bytes experiment_id, object body)

    cpdef object submit(self, bytes experiment_id, object body)

    cpdef object on_dt_over(self, bytes experiment_id, object body)

    cpdef void disconnect(self)
