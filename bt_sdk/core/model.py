#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import pydantic
from pydantic import Field, field_validator, ConfigDict
from typing import List, Union, Tuple, Mapping, Any, Dict


class ReqMeta(pydantic.BaseModel):
    start_date: int = Field(default=19900101)
    end_date: int = Field(default=30000101)
    sid: List[str] = Field(default=[])

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )


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


class OrderMsg(pydantic.BaseModel):
    topic: str = Field(default="order")
    client_id: str = Field(default="")
    msg: OrderMeta

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )


class RequestMsg(pydantic.BaseModel):

    topic: str = Field(default="query")
    client_id: str = Field(default="")
    msg: Union[ReqMeta, Dict[str, Any]]

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )


class TimerMsg(pydantic.BaseModel):
    topic: str = Field(default="timer")
    client_id: str = Field(default="")
    msg: int 


__all__ = [ "ReqMeta", "RequestMsg", "OrderMeta","OrderMsg", "TimerMsg"]

