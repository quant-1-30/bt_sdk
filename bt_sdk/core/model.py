#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import pydantic
from pydantic import Field, field_validator, ConfigDict
from typing import List, Union, Tuple, Mapping, Any, Dict


__all__ = [ "OrderMeta", "ReqMeta", "Request"]


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

    topic: str
    msg: Union[ReqMeta, OrderMeta, Any]
    client_id: str = Field(default="")

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )
