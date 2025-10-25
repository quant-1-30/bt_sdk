#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import pydantic
from datetime import datetime
from uuid import UUID, uuid4
from pydantic import Field, field_validator, ConfigDict
from typing import List, Union, Any, Dict


__all__ = ["Experiment", "Cash",  "Order", "Query", "Request"]


class Experiment(pydantic.BaseModel):
    strategy: str
    client_id: str
    appendix: str
    
    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )


class Cash(pydantic.BaseModel):
    session: int
    cash: int = Field(default=0)
    
    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )


class Order(pydantic.BaseModel):
    sid: str
    size: int = Field(default=0)
    price: int
    pricelimit: int
    exec_type: int
    order_type: int
    created_at: int
    sizer_ratio: float = Field(default=0.0)

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )


class Query(pydantic.BaseModel):
    start_date: int = Field(default=19900101)
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
    body: Union[Experiment, Cash, Order, Query]
    experiment_id: str = Field(default="null") # default is used for mdapi
    request_id: UUID = Field(default_factory=uuid4)

    def model_dump(self, *args, **kwargs) -> Dict[str, Any]:
       data = super().model_dump(*args, **kwargs)
       if 'request_id' in data and isinstance(data['request_id'], UUID):
           data['request_id'] = str(data['request_id'])        
       return data

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )
