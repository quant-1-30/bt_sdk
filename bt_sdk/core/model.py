#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import pydantic
from pydantic import Field, field_validator, ConfigDict
from typing import List, Union, Tuple, Mapping, Any


class AuthMeta(pydantic.BaseModel):
    client_id: str = Field(default="")
    token: str = Field(default="")


class ReqMeta(pydantic.BaseModel):
    start_date: int = Field(default=19900101)
    end_date: int = Field(default=30000101)
    sid: List[str] = Field(default=[])

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )

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



class TimerMeta(pydantic.BaseModel):
    timer: str
    session: int

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
    topic: str = Field(default="order")
    msg: OrderMeta
    auth: AuthMeta = Field(default=AuthMeta())

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )

class RequestMsg(pydantic.BaseModel):

    topic: str = Field(default="query")
    msg: ReqMeta
    auth: AuthMeta = Field(default=AuthMeta())

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )

class TimerMsg(pydantic.BaseModel):

    topic: str = Field(default="timer")
    msg: TimerMeta
    auth: AuthMeta = Field(default=AuthMeta())

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )


__all__ = ["LoginMeta", "OrderMeta", "ReqMeta", "TimerMeta", "AuthMeta",
           "OrderMsg", "RequestMsg", "TimerMsg", "LoginMsg"]

