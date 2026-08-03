#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import asyncio
import logging
import polars as pl
from typing import List, Union, Dict

logger = logging.getLogger(__name__)

# Add C++ binding path once (idempotent)
_LIB_DIR = os.path.join(os.path.dirname(__file__), 'lib')
if _LIB_DIR not in sys.path:
    sys.path.append(_LIB_DIR)

import adj_factor

eps = 1e-3 


def adjust2struct(df: pl.DataFrame):
    """C++ Struct — batch extract columns for 10-50x speedup over iter_rows"""
    if df is None or df.height == 0:
        return []

    ex_dates = df["ex_date"].to_list()
    bonus_shares = df["bonus_share"].to_list()
    transfers = df["transfer"].to_list()
    bonuses = df["bonus"].to_list()

    events = []
    for i in range(len(ex_dates)):
        event = adj_factor.AdjustmentEvent()
        event.ex_date = ex_dates[i]
        event.bonus_share = bonus_shares[i]
        event.transfer = transfers[i]
        event.bonus = bonuses[i]
        events.append(event)
    return events


def right2struct(df: pl.DataFrame):
    """C++ Struct — batch extract columns for 10-50x speedup over iter_rows"""
    if df is None or df.height == 0:
        return []

    ex_dates = df["ex_date"].to_list()
    prices = df["price"].to_list()
    ratios = df["ratio"].to_list()

    events = []
    for i in range(len(ex_dates)):
        event = adj_factor.RightmentEvent()
        event.ex_date = ex_dates[i]
        event.price = prices[i]
        event.ratio = ratios[i]
        events.append(event)
    return events


def _calc_factor(c_df: pl.DataFrame, adj_df: pl.DataFrame, rgt_df: pl.DataFrame, forward: int):
    # Polars.to_list() / pa.Table.to_pylist()
    vector_trading = c_df["day"].to_list()
    vector_close = c_df["close"].to_list()

    vector_adjust_event = adjust2struct(adj_df)
    vector_right_event = right2struct(rgt_df)

    if forward == 1:
        factor_type = adj_factor.AdjustType.Forward
    elif forward == 2:
        factor_type = adj_factor.AdjustType.Backward
    else:
        raise ValueError(f"Invalid forward type: {forward}, expected 1 (Forward) or 2 (Backward)")

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


async def calc_factor_async(
    closes: Dict[bytes, pl.DataFrame],
    adjs: Dict[bytes, pl.DataFrame],
    rgts: Dict[bytes, pl.DataFrame],
    forward: int
) -> Dict[bytes, adj_factor.FactorResult]:
    """
    Async version of calc_factor — parallelizes C++ factor calculation across sids
    using run_in_executor. Requires py::call_guard<py::gil_scoped_release>() in the
    pybind11 binding (already done) for true parallelism.
    """
    if not closes:
        return {}

    async def _calc(sid):
        close_df = closes[sid]
        adj_df = adjs.get(sid, pl.DataFrame())
        rgt_df = rgts.get(sid, pl.DataFrame())
        loop = asyncio.get_running_loop()
        # C++ calc runs in thread pool with GIL released → true parallelism
        result = await loop.run_in_executor(
            None, _calc_factor, close_df, adj_df, rgt_df, forward
        )
        return sid, result

    results = await asyncio.gather(*[_calc(sid) for sid in closes])
    return dict(results)


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

    if adjust_type == 1:  # qfq
        factors = sorted_factors + [1.0]
    else:  # hfq
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
    df = df.sort("day")  # Polars join_asof

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
        # exprs.append((pl.col("volume") / pl.col("factor").alias("volume"))
        # protect against division by zero: clip factor to a small epsilon
        exprs.append(
            pl.when(pl.col("factor").abs() <= eps)
            .then(pl.col("volume"))  
            .otherwise(pl.col("volume") / pl.col("factor"))  
            .alias("volume")
        )

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