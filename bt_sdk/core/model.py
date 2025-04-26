#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import pydantic
from pydantic import Field, field_validator, ConfigDict
from typing import List, Union, Tuple, Mapping, Any

class LoginMeta(pydantic.BaseModel):
    user_id: str
    phone: int
    auto_register: bool = Field(default=True)

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )

    @field_validator('phone')
    def validate_phone(cls, v):
        if not re.match(r'1[3-9]\d{9}$', str(v)):
            raise ValueError('Invalid phone number')
        return v

class OrderMeta(pydantic.BaseModel):
    client_id: str
    sid: str
    size: int = Field(default=0)
    price: int
    pricelimit: int
    exec_type: int
    order_type: int
    created_at: int
    sizer_cash: int

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
    client_id: str
    sub_topic: str
    start_time: int = Field(default=19900101)
    end_time: int = Field(default=30000101)
    sids: List[str] = Field(default=[])

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )

class TimerMeta(pydantic.BaseModel):
    client_id: str
    timer: str

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )

class LoginMsg(pydantic.BaseModel):
    topic: str = Field(default="login")
    msg: LoginMeta

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


__all__ = ["LoginMeta", "OrderMeta", "ReqMeta", "TimerMeta", 
           "OrderMsg", "RequestMsg", "TimerMsg", "LoginMsg"]

