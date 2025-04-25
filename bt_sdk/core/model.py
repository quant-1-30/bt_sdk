#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import pydantic
from pydantic import Field, field_validator, ConfigDict
from typing import List, Union, Tuple, Mapping, Any


class OrderMeta(pydantic.BaseModel):
    client_id: str
    sid: str
    size: int = Field(default=0)
    sizer_cash: int
    price: int
    pricelimit: int
    created_at: int
    exectype: int

    # @field_validator('exectype')
    # def validate_exectype(cls, v):
    #     if v not in [ExecType.Market, ExecType.Close, ExecType.Limit, ExecType.Stop, ExecType.StopLimit]:
    #         raise ValueError('Invalid exectype')
    #     return v

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )

class ReqMeta(pydantic.BaseModel):
    sub_topic: str
    client_id: str
    start_time: int = Field(default=19900101)
    end_time: int = Field(default=30000101)
    sids: List[str] = Field(default=[])

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )

class TimerMeta(pydantic.BaseModel):

    sub_topic: str
    msg: int

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )

class OrderMsg(pydantic.BaseModel):
    topic: str = Field(default="default")
    msg: OrderMeta

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )

class RequestMsg(pydantic.BaseModel):

    topic: str = Field(default="query")
    msg: ReqMeta

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )

class TimerMsg(pydantic.BaseModel):

    topic: str = Field(default="timer")
    msg: TimerMeta

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )


__all__ = ["OrderMeta", "ReqMeta", "TimerMeta", "OrderMsg", "RequestMsg", "TimerMsg"]
