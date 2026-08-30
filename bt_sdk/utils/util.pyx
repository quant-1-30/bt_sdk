# distutils: language = c++
# cython: language_level=3

import asyncio
import datetime
import threading
import pyarrow as pa
import polars as pl

from libc.stdint cimport uint8_t, int64_t


# C typedef / cython ctypedef
cdef extern from "<uuid/uuid.h>" nogil:
    ctypedef uint8_t uuid_t[16]
    void uuid_generate(uuid_t out)
    void uuid_unparse_lower(const uuid_t uu, char *out)


# C数组 ---> python object, 视为以\0结尾的字符串类似于C++ string
cdef UuidFrame fast_uuid4_c() nogil: 
    cdef UuidFrame uu
    uuid_generate(uu.data)
    return uu

cpdef str fast_uuid4_str(): # no python object
    cdef uuid_t uu
    cdef char out[37] # 36byte + \0
    with nogil:
        uuid_generate(uu)
        uuid_unparse_lower(uu, out)
    return out[:36].decode('ascii')


cpdef bytes fast_uuid4_bytes():
    cdef uuid_t uu
    uuid_generate(uu)
    return (<char*>uu)[:16]


cpdef object _merge2DataFrame(list batches, bint is_group=True): # cdef reduce python overhead
    cdef bytes sid_byte
    cdef dict sid_to_batches = {} 
    cdef dict aligned = {}
    cdef list sid_batch
    cdef object batch, table

    if not batches:
        if is_group:
            return {}
        return pl.DataFrame()

    if not is_group:
        table = pa.concat_tables(batches, promote_options="permissive")
        return pl.from_arrow(table, rechunk=False)  # avoid extra copy

    for batch in batches:
        meta = batch.schema.metadata or {}
        sid_byte = meta.get(b"sid")

        if sid_byte not in sid_to_batches:
            sid_to_batches[sid_byte] = []

        sid_batch = sid_to_batches[sid_byte]
        sid_batch.append(batch)
 
    for sid_byte, bulk_batch in sid_to_batches.items():
        aligned_table = pa.concat_tables(bulk_batch, promote_options='default')
        aligned[sid_byte] = pl.from_arrow(aligned_table, rechunk=False)  # avoid extra copy
    return aligned
