#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import pydantic
from enum import Enum
from pydantic import Field, field_validator, ConfigDict
from typing import List, Union, Tuple, Mapping, Any, Dict


__all__ = [  "OrderType", "ExecType", "OrderMeta", "ReqMeta", "Request"]


CHUNK_HEADER_FORMAT = ">HB"

class OrderType(Enum):
    Buy = 0
    Sell = 1


class ExecType(Enum):
    Open = 0
    Market = 1
    Close = 2
    Limit = 3
    Stop = 4
    StopLimit = 5


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
    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )


class ReqMeta(pydantic.BaseModel):
    start_date: int = Field(default=19900101)
    end_date: int = Field(default=30000101)
    sid: List[str] = Field(default=[])

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )


class Request(pydantic.BaseModel):

    # topic: str = Field(default="query")
    topic: str
    client_id: str
    msg: Union[ReqMeta, OrderMeta, Dict[str, Any]]

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )
