# distutils: language = c++
# cython: language_level=3

import asyncio
import datetime
import threading
import pyarrow as pa

from libc.stdint cimport uint8_t, int64_t
from libc.time cimport gmtime, time_t, tm

DEF HOURS_PER_DAY = 24
DEF MINUTES_PER_HOUR = 60
DEF SECONDS_PER_MINUTE = 60
DEF OPEN_OFFSET = 34200
DEF CLOSE_OFFSET = 54000
DEF SECONDS_PER_DAY = 86400 
DEF SHANGHAI_OFFSET = 28800


cdef struct UuidFrame:
    uint8_t data[16]


# cdef struct CTime: # replace datetime
#     int year, month, day, hour, minute, second, microsecond

cdef struct MarketTime:
    int64_t open_ts
    int64_t close_ts


cdef extern from "<uuid/uuid.h>" nogil:
    ctypedef uint8_t uuid_t[16]
    void uuid_generate(uuid_t out)
    void uuid_unparse_lower(const uuid_t uu, char *out)


# C typedef / cython ctypedef
cdef inline UuidFrame fast_uuid4_c() nogil: # C数组 ---> python object, 视为以\0结尾的字符串类似于C++ string
    cdef UuidFrame uu
    uuid_generate(uu.data)
    return uu

cpdef inline bytes fast_uuid4_bytes():
    cdef uuid_t uu
    uuid_generate(uu)
    return (<char*>uu)[:16]

cpdef inline str fast_uuid4_str(): # no python object
    cdef uuid_t uu
    cdef char out[37] # 36byte + \0
    with nogil:
        uuid_generate(uu)
        uuid_unparse_lower(uu, out)
    return out[:36].decode('ascii')


cdef inline object num2date(double x, bint native=True, bint ordinal=True):
    """
    Unix 时间戳直接算术转换
    """
    cdef int64_t ix = <int64_t>x
    cdef int year, month, day, hour, minute, second, microsecond
    cdef double remainder
    cdef int64_t ts

    if not ordinal and 10000000 < ix < 30000000:
        year = <int>(ix // 10000)
        month = <int>((ix % 10000) // 100)
        day = <int>(ix % 100)
        return datetime.datetime(year, month, day)

    if not ordinal: # Unix
        ts = <int64_t>(x - ( SHANGHAI_OFFSET if native else 0 ))
        return datetime.datetime.fromtimestamp(ts)

    # ordinal  0001-01-01 days 
    dt_base = datetime.datetime.fromordinal(<int>ix)
    remainder = x - ix
    
    # C ops --->  Python divmod
    remainder *= HOURS_PER_DAY
    hour = <int>remainder
    remainder = (remainder - hour) * MINUTES_PER_HOUR
    minute = <int>remainder
    remainder = (remainder - minute) * SECONDS_PER_MINUTE
    second = <int>remainder
    microsecond = <int>((remainder - second) * 1000000)
    
    if microsecond < 10: microsecond = 0
    elif microsecond > 999990:
        microsecond = 0
        second += 1 # 简单进位逻辑

    return datetime.datetime(dt_base.year, dt_base.month, dt_base.day, 
                             hour, minute, second, microsecond)


cdef inline MarketTime market_utc(int64_t ts, bint native=True) nogil :
    cdef MarketTime mt
    cdef int64_t offset = SHANGHAI_OFFSET if native else 0
    cdef int64_t local_day_start_utc = ((ts + offset) // 86400) * 86400 - offset
    
    mt.open_ts = local_day_start_utc + <int64_t>OPEN_OFFSET
    mt.close_ts = local_day_start_utc + <int64_t>CLOSE_OFFSET
    return mt


cdef inline int64_t ts_to_int_date(int64_t ts, bint native=True) nogil: # only cdef nogil
    # C api / tzinfo="Asia/Shanghai" native
    cdef time_t rawtime = <time_t>(ts) if native else <time_t>(ts+28800) 
    cdef tm* info = gmtime(&rawtime)
    return (info.tm_year + 1900) * 10000 + (info.tm_mon + 1) * 100 + info.tm_mday


cdef inline object _merge_tables(list batches, bint is_group=True): # cdef reduce python overhead
    cdef bytes sid_byte
    cdef dict sid_to_batches = {} 
    cdef dict aligned = {}
    cdef list sid_batch
    cdef object batch, table

    if not is_group:
        return pa.concat_tables(batches, promote_options="permissive") # zero_copy accumlate chunk ptr not reallocate / just when combine_chunks() 

    for batch in batches:
        sid_byte = batch.schema.metadata.get(b"sid")
        if sid_byte not in sid_to_batches:
            sid_to_batches[sid_byte] = []

        sid_batch = sid_to_batches[sid_byte]
        sid_batch.append(batch) # int.from_bytes()
 
    for sid_byte, bulk_batch in sid_to_batches.items():
        aligned[sid_byte] = pa.concat_tables(bulk_batch, promote_options='default') # pa.Table.from_batches(bulk_batch) # to_pydict() No 
    return aligned 
