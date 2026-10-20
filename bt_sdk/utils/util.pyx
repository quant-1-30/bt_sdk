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
    cdef list ungrouped_batches = []
    cdef object batch, table, merged_tbl, full_df, part_dict, raw_key, key_val, sub_df

    if not batches:
        if is_group:
            return {}
        return pl.DataFrame()

    if not is_group:
        table = batches[0] if len(batches) == 1 else pa.concat_tables(batches, promote_options="permissive")
        return pl.from_arrow(table, rechunk=False)  # avoid extra copy

    for batch in batches:
        if batch is None or batch.num_rows == 0:
            continue
        meta = batch.schema.metadata or {}
        sid_byte = meta.get(b"sid")

        if sid_byte is not None:
            # 兼容带 metadata[b"sid"] 的单 sid 帧 (如 Adjust / Right 或旧版单标的帧)
            if sid_byte not in sid_to_batches:
                sid_to_batches[sid_byte] = []
            sid_batch = sid_to_batches[sid_byte]
            sid_batch.append(batch)
        else:
            # 整批发送的多 sid 帧 (Tick / Daily / Close): 收集后一次性转 Polars 在 Rust 层按 sid 切分
            ungrouped_batches.append(batch)
 
    for sid_byte, bulk_batch in sid_to_batches.items():
        # permissive (same as the non-group path): tolerate schema drift between
        # batches (e.g. null-typed vs typed column) instead of raising ArrowInvalid
        aligned_table = bulk_batch[0] if len(bulk_batch) == 1 else pa.concat_tables(bulk_batch, promote_options='permissive')
        aligned[sid_byte] = pl.from_arrow(aligned_table, rechunk=False)  # avoid extra copy

    if ungrouped_batches:
        merged_tbl = (
            ungrouped_batches[0]
            if len(ungrouped_batches) == 1
            else pa.concat_tables(ungrouped_batches, promote_options="permissive")
        )
        full_df = pl.from_arrow(merged_tbl, rechunk=False)
        if full_df.height > 0 and "sid" in full_df.columns:
            part_dict = full_df.partition_by("sid", as_dict=True, maintain_order=True)
            for raw_key, sub_df in part_dict.items():
                key_val = raw_key[0] if isinstance(raw_key, tuple) else raw_key
                if key_val is None:
                    continue
                if isinstance(key_val, bytes):
                    sid_byte = key_val
                elif isinstance(key_val, str):
                    sid_byte = key_val.encode("utf-8")
                else:
                    sid_byte = str(key_val).encode("utf-8")

                if sid_byte in aligned:
                    aligned[sid_byte] = pl.concat([aligned[sid_byte], sub_df], rechunk=False)
                else:
                    aligned[sid_byte] = sub_df

    return aligned
