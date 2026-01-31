cpdef enum RpcTopic:
    Calendar = 0
    Instrument = 1
    Index = 2
    Tick = 3 
    Close = 4
    Adjustment = 5
    Rightment = 6
 
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
    Open = 0
    Market = 1
    COC = 2
    Limit = 3
    Stop = 4
    StopLimit = 5
    StopTrail = 6
    StopTrailLimit = 7
    Historical = 8


cdef class Api:
    cdef object _loop_thread

    cdef object _init_event_loop(self)

    cdef void _run_event_loop(self, loop)


cdef class TdApi(Api):
    cdef bytes client_id
    cdef object async_client
    
    cdef object _send_request(self, int topic, bytes experiment_id=*, object body=*, int sub_topic=*)

    cpdef object register(self, object body)

    cpdef object set_cash(self, bytes experiment_id, object body)

    cpdef object getvalue(self, int topic, bytes experiment_id)

    cpdef object subscribe(self, int topic, bytes experiment_id, object body)

    cpdef object submit(self, bytes experiment_id, object body)

    cpdef object on_dt_over(self, bytes experiment_id, object body)

    cpdef void disconnect(self)


cdef class MdApi(Api):
    cdef int timeout
    cdef object async_client
    cdef object loop
    
    cpdef object get_calendar(self)

    cpdef object get_instrument(self)

    cpdef object get_benchmark(self, object body)

    cpdef object subscribe(self, object body)
    
    cpdef object get_event_obs(self, int topic, object body)

    cpdef object get_close_obs(self, object body)
    
    cpdef object get_factor(self, object body)
    
    cpdef void disconnect(self)
