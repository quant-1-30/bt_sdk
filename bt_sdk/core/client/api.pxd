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


cdef class MdApi:
    cdef bint is_background
    cdef int timeout
    cdef public object loop
    cdef object async_client
    
    cpdef object get_calendar(self)

    cpdef object get_instrument(self)

    cpdef object get_benchmark(self, object body)

    cpdef object subscribe(self, object body)
    
    cpdef object get_subscribe(self, object body)
    
    cpdef object get_close(self, object body)
    
    cpdef object get_event(self, int topic, object body)

    cpdef object get_factor(self, object body)
    
    cpdef void disconnect(self)
