# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import uuid
import struct
from typing import Any


struct_fmt = {
    "md": {
            "calendar": ">i",
            "instrument": ">6sii",
            "tick": ">liiiiiq",
            "adjustment": "",
            "rightment": "",
    },
    "td": {
            "order": ">IIdd",
            "position": ">6sIIdId16s",
            "account": ">ff",
    },
}


def msg_unpack(_type: str, msg_type, msg: bytes) -> Any:
    # # 网络传输base64编码替换 +与/ 特殊字符 返回bytes
    # # bytes ---> base64 encode(bytes-like ---> bytes-like object)
    # # base64 decode (str / bytes-like ---> bytes-like)
    # body = metadata.pop("body")
    # decode = {k: base64.b64decode(v) for k, v in body.items()}
    # decode = {k: json.loads(v.decode("utf-8")) for k, v in decode.items()}
    # msgpack / struct 
    # zlib --- stream data / gzip --- file
    #  uuid_obj = uuid.UUID(bytes=unpacked[0])
    #  return str(uuid_obj)
    if msg:
        try:
            unpacked = struct.unpack(struct_fmt[_type][msg_type], msg)
            # uuid.UUID(bytes=unpacked[-1]) / byte.decode("utf-8")
            return unpacked
        except Exception as e:
            print("msg_unpack error: ", e)
            return ''
    return ''


__all__ = ["msg_unpack"]
