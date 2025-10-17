#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import pydantic
from datetime import datetime
from uuid import UUID, uuid4
from pydantic import Field, field_validator, ConfigDict
from typing import List, Union, Any, Dict


__all__ = ["ExpMeta", "CashMeta",  "OrderMeta", "ReqMeta", "Request"]


class ExpMeta(pydantic.BaseModel):
    strategy: str
    client_id: str
    assets: str
    
    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )


class CashMeta(pydantic.BaseModel):
    session: int
    cash: int = Field(default=0)
    
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
    sizer_ratio: float = Field(default=0.0)

    # @field_validator('exectype')
    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )


class ReqMeta(pydantic.BaseModel):
    start_date: int = Field(default=0)
    end_date: int = Field(default=int(datetime.now().strftime("%Y%m%d")))
    sid: List[str] = Field(default=[])

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )

    @field_validator('start_date', 'end_date', mode='before')
    def validate_date(cls, v):
        if isinstance(v, str):
            try:
                dt = datetime.strptime(v, '%Y%m%d')
                return int(dt.timestamp())
            except ValueError:
                raise ValueError(f"Invalid date format: {v}. Expected 'YYYYMMDD'.")
        elif isinstance(v, int):
            # Assuming the integer is in YYYYMMDD format
            try:
                dt = datetime.strptime(str(v), '%Y%m%d')
                return int(dt.timestamp())
            except ValueError:
                raise ValueError(f"Invalid date format: {v}. Expected 'YYYYMMDD'.")
        elif isinstance(v, float):
            return int(v)
        elif isinstance(v, datetime):
            return int(v.timestamp())
        else:
            raise TypeError(f"Unsupported type for date: {type(v)}. Expected str, int, float, or datetime.")


class Request(pydantic.BaseModel):

    topic: str
    body: Union[ExpMeta, CashMeta, OrderMeta, ReqMeta]
    # experiment_id: str = Field(default="default")
    experiment_id: str
    request_id: UUID = Field(default_factory=uuid4)

    def model_dump(self, *args, **kwargs) -> Dict[str, Any]:
       """重写 model_dump 方法，将 UUID 转为字符串"""
       data = super().model_dump(*args, **kwargs)
       # 处理 UUID 字段
       if 'request_id' in data and isinstance(data['request_id'], UUID):
           data['request_id'] = str(data['request_id'])        
       return data

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )
