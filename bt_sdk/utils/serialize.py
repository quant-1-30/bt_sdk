# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import msgpack
import struct
from typing import Any, Mapping


def default_encoder(obj):
    if isinstance(obj, uuid.UUID):
        return obj.bytes # str(obj) 
    # elif isinstance(obj, datetime.datetime):
    #     return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not serializable")

def pack(msg: Mapping) -> bytes:
    # bytes ---> base64 encode(bytes-like ---> bytes-like object)
    # decode = {k: base64.b64decode(v) for k, v in body.items()}
    # decode = {k: json.loads(v.decode("utf-8")) for k, v in decode.items()}
    return msgpack.packb(msg, use_bin_type=True, default=default_encoder)

def unpack(data: bytes) -> list:
    decode_data = msgpack.unpackb(data, use_list=False, raw=False, strict_map_key=False)
    return decode_data

# struct_fmt = {
#     "md": {
#             "calendar": ">i",
#             "instrument": ">6sii",
#             "tick": ">liiiiiq",
#             "adjustment": "",
#             "rightment": "",
#     },
#     "td": {
#             "order": ">IIdd",
#             "position": ">6sIIdId16s",
#             "account": ">ff",
#     },
# }

# struct_format = {"tick": ">liiiiiq",  # l -- long / q -- long long / L /Q unsigned
#                  "calendar": ">i", 
#                  "instrument": ">6sii", 
#                  "adjustment": ">6siiiii", 
#                  "rightment": ">6siiii"}
      
# def struct_pack(msg: str, obj) -> Any:  # ! / > stands for big-endian / i16s (uuid) / I{length}s () 
#     if obj:
#         # import pdb; pdb.set_trace()
#         data = obj if isinstance(obj, tuple) else (obj,)
#         packed = struct.pack(struct_format[msg], *data)
#         return packed
