#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'lib')) # append C++ binding
import adj_factor
import polars as pl
# import pyarrow as pa
from typing import List, Union, Dict


def adjust2struct(df: pl.DataFrame):
    """C++ Struct"""
    events = []
    if df is not None and df.height > 0:
        for row in df.iter_rows(named=True):
            event = adj_factor.AdjustmentEvent()
            event.ex_date = row.get("ex_date", 0)
            event.bonus_share = row.get("bonus_share", 0.0)
            event.transfer = row.get("transfer", 0.0)
            event.bonus = row.get("bonus", 0.0)
            events.append(event)
    return events        


def right2struct(df: pl.DataFrame):
    """C++ Struct"""
    events = []
    if df is not None and df.height > 0:
        for row in df.iter_rows(named=True):
            event = adj_factor.RightmentEvent() 
            event.ex_date = row.get("ex_date", 0)
            event.price = row.get("price", 0.0)
            event.ratio = row.get("ratio", 0.0)
            events.append(event)
    return events


def _calc_factor(c_df: pl.DataFrame, adj_df: pl.DataFrame, rgt_df: pl.DataFrame, forward: int):
    # Polars.to_list() / pa.Table.to_pylist() 
    vector_trading = c_df["day"].to_list()
    vector_close = c_df["close"].to_list()
    
    vector_adjust_event = adjust2struct(adj_df)
    vector_right_event = right2struct(rgt_df) 

    factor_type = adj_factor.AdjustType.Forward if forward == 1 else adj_factor.AdjustType.Backward 
    factors = adj_factor.calc_adjust_factors(
        vector_trading, 
        vector_close, 
        vector_adjust_event, 
        vector_right_event, 
        factor_type
    )
    return factors


def calc_factor(
    closes: Dict[bytes, pl.DataFrame], 
    adjs: Dict[bytes, pl.DataFrame], 
    rgts: Dict[bytes, pl.DataFrame], 
    forward: int
) -> Dict[bytes, adj_factor.FactorResult]:
    
    if not closes:
        return {}

    factor_sids = {}
    for sid, close_df in closes.items():
        adj_df = adjs.get(sid, pl.DataFrame())
        rgt_df = rgts.get(sid, pl.DataFrame())
        
        factor_sids[sid] = _calc_factor(close_df, adj_df, rgt_df, forward) 
        
    return factor_sids


def _apply_factor(df: pl.DataFrame, adj_factors: dict, adjust_type: int) -> pl.DataFrame:
    if df.height == 0:
        return df

    # Tick ---> Day
    if "tick" in df.columns and "day" not in df.columns:
        df = df.sort("tick").with_columns(
            day = pl.from_epoch(pl.col("tick"), time_unit="s").dt.strftime("%Y%m%d").cast(pl.Int32)
        )
    
    if not adj_factors:
        return df

    sorted_dates = sorted(adj_factors.keys())
    sorted_factors = [adj_factors[d] for d in sorted_dates]
    factor_dates = [19000101] + sorted_dates 
    
    if adjust_type == 1: # qfq 
        factors = sorted_factors + [1.0]
    else: # hfq 
        factors = [1.0] + sorted_factors

    factor_df = pl.DataFrame({
        "day": factor_dates,
        "factor": factors
    }).with_columns(
        pl.col("day").cast(pl.Int32)
    ).set_sorted("day")

    # ==========================================
    # join_asof
    # ==========================================
    df = df.sort("day") # Polars join_asof 

    # strategy="backward" right <= left nearly row
    # strategy="forward" right >= left
    # strategy="nearest" abs(left - right) minimum
    joined_df = df.join_asof(
        factor_df,
        on="day",             
        strategy="backward"   
    )

    price_cols = [c for c in ["open", "high", "low", "close"] if c in joined_df.columns]
    
    exprs = []
    for col in price_cols:
        exprs.append((pl.col(col) * pl.col("factor")).alias(col))
        
    if "volume" in joined_df.columns:
        exprs.append((pl.col("volume") / pl.col("factor")).alias("volume"))

    adjusted_df = joined_df.with_columns(exprs).drop("factor") 
    return adjusted_df


def apply_factor(
    raw_data: Dict[bytes, pl.DataFrame], 
    adj_factors: Dict[bytes, adj_factor.FactorResult], 
    adjust_type: int
) -> Dict[bytes, pl.DataFrame]:
    
    adjusted_array = {}
    for sid, df in raw_data.items():
        factor_obj = adj_factors.get(sid)
        factors_dict = factor_obj.adj_factors if factor_obj else {}
        
        adjusted_array[sid] = _apply_factor(df, factors_dict, adjust_type)
        
    return adjusted_array
