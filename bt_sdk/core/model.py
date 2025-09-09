#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import pydantic
from dataclasses import dataclass
from datetime import datetime
from pydantic import Field, field_validator, ConfigDict
from typing import List, Union, Optional, Mapping


__all__ = ["ReqMeta", "CashMeta",  "OrderMeta", "Request", "OrderBit", "Position", "Account"]


class ReqMeta(pydantic.BaseModel):
    start_date: int = Field(default=0)
    end_date: int = Field(default=int(datetime.now().strftime("%Y%m%d")))
    sid: List[str] = Field(default=[])

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )


class CashMeta(pydantic.BaseModel):
    session: int
    cash: int = Field(default=0)


class OrderMeta(pydantic.BaseModel):
    sid: str
    size: int = Field(default=0)
    price: int
    pricelimit: int
    exec_type: int
    order_type: int
    created_at: int
    sizer_ratio: float = Field(default=0.0)

    # @field_validator('exectype')
    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )


class Request(pydantic.BaseModel):

    topic: str
    body: Union[ReqMeta, CashMeta, OrderMeta]
    client_id: str = Field(default="")

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )

# ------------------------------------------------------------------- return obj -------------------------------------------------------------

class OrderBit(pydantic.BaseModel):

        executed_at: int
        executed_size: int
        executed_price: float
        comm: float
        direction: bool


class Position(pydantic.BaseModel):
    sid: str
    datetime: int
    size: int
    available: int
    price: float
    pnl: float

    def justopen(self):
         return self.size > 0 and self.available == 0
    
    def isclosd(self):
         return self.size == 0


# @dataclass(frozen=True)
class Account(pydantic.BaseModel):
    datetime: int
    portfolio_value: float
    cash: float
    leverage: int
    margin: float
    client_id: str

