# !/usr/bin/env python3
# -*- coding: utf-8 -*-

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
            "trade": ">iiff",
            "order": ">iiff",
            "position": "!6sifiif16s",
            "account": "!iffii16s",
            "sync": "!f",
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
    msg_type = "order" if msg_type == "trade" else msg_type
    if msg:
        unpacked = struct.unpack(struct_fmt[_type][msg_type], msg)
        return unpacked
    return ''


__all__ = ["msg_unpack"]
