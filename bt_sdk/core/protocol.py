#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import msgspec
from typing import List, Union, Any, Dict


class QueryBody(msgspec.Struct, frozen=True, tag="query"):
    start_date: int
    end_date: int 
    sid: List[bytes] = []


class RegisterBody(msgspec.Struct, frozen=True, tag="register"):
    client_id: bytes
    strategy: str
    extra_info: str
    identity: bytes


class CashBody(msgspec.Struct, frozen=True, tag="cash"):
    session: int # 19900202
    cash: float


class OrderBody(msgspec.Struct, frozen=True, tag="order"):
    sid: bytes
    pricelimit: int
    sizer_ratio: int
    order_type: int
    exec_type: int
    created_dt: int
    filler: bytes # oco / occ / smooth / likehood


class Event(msgspec.Struct, frozen=True):
    topic: str
    sub_topic: str = ""
    experiment_id: bytes = b""
    body: Union[QueryBody, RegisterBody, CashBody, OrderBody, None] = None # tag to find body in Union strict
    # body: EmptyBody = msgspec.field(default_factory=EmptyBody)


class ExperimentBody(msgspec.Struct, frozen=True, tag="experiment"):
    experiment_id: bytes


class TradeBody(msgspec.Struct, frozen=True, tag="trade"):
    vtorder_id: bytes
    executed_dt: int
    executed_size: int
    executed_price: float
    comm: float


class PositionBody(msgspec.Struct, frozen=True, tag="position"):
    sid: bytes
    datetime: int 
    size: int
    available: int
    cost_basis: float
    pnl: float
    experiment_id: bytes


class AccountBody(msgspec.Struct, frozen=True, tag="account"):
    datetime: int
    portfolio_value: float
    cash: float
    pnl: float
    leverage: float
    margin: float
    experiment_id: bytes


class Empty(msgspec.Struct, frozen=True, tag="empty"):
    pass


class ErrMSg(msgspec.Struct, frozen=True, tag="error"):
    error: str


class Sentinel(msgspec.Struct, frozen=True, tag="sentinel"):
    pass


class Resp(msgspec.Struct, frozen=True):
    body: Union[ExperimentBody, TradeBody, PositionBody, AccountBody, Empty, ErrMSg, Sentinel, None]=None


ResponseTypes = List[Resp]

# global
_ENCODER = msgspec.msgpack.Encoder()
_DECODER = msgspec.msgpack.Decoder(type=Event)
_RespDECODER = msgspec.msgpack.Decoder(type=ResponseTypes) # msgspec.msgpack.Decoder(type=Resp)
