# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import uuid
import struct
from typing import Any


struct_md = {
    "calendar": ">i",
    "instrument": ">6sii",
    "tick": ">liiiiiq",
    "adjustment": "",
    "rightment": "",
}

struct_td = {
    "login": "!16s",
    "order": ">iiff",
    "position": "!6sifiif16s",
    "account": "!iffii16s",
    "timer": "!f",
}

def md_unpack(msg_type: str, msg: bytes) -> Any:
    if msg:
        # import pdb
        # pdb.set_trace()
        unpacked = struct.unpack(struct_md[msg_type], msg)
        return unpacked
    return ''


def td_unpack(msg_type: str, msg: bytes) -> Any:
    if msg:
        unpacked = struct.unpack(struct_td[msg_type], msg)
        if msg_type == "login":
            uuid_obj = uuid.UUID(bytes=unpacked[0])
            return str(uuid_obj)
        return unpacked
    return ''


__all__ = ["td_unpack", "md_unpack"]
