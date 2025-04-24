#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import pydantic
from enum import Enum
from pydantic import Field, field_validator, ConfigDict
from typing import List, Union, Tuple, Mapping, Any


class Req(pydantic.BaseModel):
    start_date: int = Field(default=19900101)
    end_date: int = Field(default=30000101)
    sid: List[str] = Field(default=[])

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )

class ExecType(Enum):
    Market = 0
    Close = 1
    Limit = 2
    Stop = 3
    StopLimit = 4


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


class Msg(pydantic.BaseModel):
    topic: str
    msg: Any



__all__ = ["Req", "OrderMeta", "Msg"]
