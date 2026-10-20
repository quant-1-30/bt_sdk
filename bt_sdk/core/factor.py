#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import asyncio
import logging
import numpy as np
import polars as pl
from typing import Dict

logger = logging.getLogger(__name__)

# Add C++ binding path once (idempotent)
_LIB_DIR = os.path.join(os.path.dirname(__file__), 'lib')
if _LIB_DIR not in sys.path:
    sys.path.append(_LIB_DIR)

import adj_factor

eps = 1e-3
_EMPTY_I32 = np.empty(0, dtype=np.int32)
_EMPTY_F64 = np.empty(0, dtype=np.float64)


def adjust2struct(df: pl.DataFrame):
    """Convert adjustment DataFrame columns into C++ AdjustmentEvent list."""
    if df is None or df.height == 0:
        return []
    events = []
    for ex_d, bs, tr, bn in zip(
        df["ex_date"].to_list(),
        df["bonus_share"].to_list(),
        df["transfer"].to_list(),
        df["bonus"].to_list(),
    ):
        ev = adj_factor.AdjustmentEvent()
        ev.ex_date = ex_d
        ev.bonus_share = bs
        ev.transfer = tr
        ev.bonus = bn
        events.append(ev)
    return events


def right2struct(df: pl.DataFrame):
    """Convert rightment DataFrame columns into C++ RightmentEvent list."""
    if df is None or df.height == 0:
        return []
    events = []
    for ex_d, pr, rt in zip(
        df["ex_date"].to_list(),
        df["price"].to_list(),
        df["ratio"].to_list(),
    ):
        ev = adj_factor.RightmentEvent()
        ev.ex_date = ex_d
        ev.price = pr
        ev.ratio = rt
        events.append(ev)
    return events


def _calc_factor(c_df: pl.DataFrame, adj_df: pl.DataFrame, rgt_df: pl.DataFrame, forward: int):
    if forward == 1:
        factor_type = adj_factor.AdjustType.Forward
    elif forward == 2:
        factor_type = adj_factor.AdjustType.Backward
    else:
        raise ValueError(f"Invalid forward type: {forward}, expected 1 (Forward) or 2 (Backward)")

    has_adj = adj_df is not None and adj_df.height > 0
    has_rgt = rgt_df is not None and rgt_df.height > 0

    # Fast short-circuit: when an instrument has neither adjustment nor rightment
    # events (or no close rows), skip sorting/extracting the 8000-row close table.
    if (not has_adj and not has_rgt) or c_df is None or c_df.height == 0:
        return adj_factor.calc_adjust_factors_columns(
            _EMPTY_I32, _EMPTY_F64,
            _EMPTY_I32, _EMPTY_F64, _EMPTY_F64, _EMPTY_F64,
            _EMPTY_I32, _EMPTY_F64, _EMPTY_F64,
            factor_type,
        )

    # The C++ engine requires trading_dates sorted ascending (binary search +
    # preclose = previous row). Server streams ORDER BY day ASC; only sort when
    # not already ordered to avoid a redundant O(N log N) sort + rechunk copy.
    if not c_df["day"].is_sorted():
        c_df = c_df.sort("day")

    # Zero-copy contiguous NumPy views into the Polars columns (avoids boxing
    # Python int/float lists and avoids Python AdjustmentEvent loop allocation).
    vector_trading = c_df["day"].to_numpy()
    vector_close = c_df["close"].to_numpy()

    if has_adj:
        adj_ex = adj_df["ex_date"].to_numpy()
        adj_bs = adj_df["bonus_share"].to_numpy()
        adj_tr = adj_df["transfer"].to_numpy()
        adj_bn = adj_df["bonus"].to_numpy()
    else:
        adj_ex = _EMPTY_I32
        adj_bs = adj_tr = adj_bn = _EMPTY_F64

    if has_rgt:
        rgt_ex = rgt_df["ex_date"].to_numpy()
        rgt_pr = rgt_df["price"].to_numpy()
        rgt_rt = rgt_df["ratio"].to_numpy()
    else:
        rgt_ex = _EMPTY_I32
        rgt_pr = rgt_rt = _EMPTY_F64

    return adj_factor.calc_adjust_factors_columns(
        vector_trading,
        vector_close,
        adj_ex,
        adj_bs,
        adj_tr,
        adj_bn,
        rgt_ex,
        rgt_pr,
        rgt_rt,
        factor_type,
    )


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
    forward: int,
    max_concurrency: int = 0
) -> Dict[bytes, adj_factor.FactorResult]:
    """
    Async version of calc_factor — parallelizes C++ factor calculation across sids
    using run_in_executor. Requires py::call_guard<py::gil_scoped_release>() in the
    pybind11 binding (already done) for true parallelism.

    Concurrency is bounded (default: cpu_count, min 4): an unbounded gather over
    thousands of sids used to submit every job at once, spiking memory (all
    DataFrames + extracted lists resident simultaneously) and queueing behind
    the small default thread pool. A semaphore keeps at most N computations in
    flight, which also matches the real parallelism ceiling — sort/to_list hold
    the GIL, only the C++ kernel scales with cores.
    """
    if not closes:
        return {}

    if max_concurrency <= 0:
        max_concurrency = max(4, os.cpu_count() or 4)

    sem = asyncio.Semaphore(max_concurrency)
    sids = list(closes.keys())
    # Partition sids into coarse chunks (at most max_concurrency * 2 batches)
    # so thousands of sids don't pay per-sid coroutine + run_in_executor
    # scheduling overhead (~30us/call vs ~2us/sid C++ compute).
    target_batches = min(len(sids), max_concurrency * 2)
    chunk_size = max(1, (len(sids) + target_batches - 1) // target_batches)
    chunks = [sids[i:i + chunk_size] for i in range(0, len(sids), chunk_size)]

    def _calc_batch(sid_batch):
        empty_df = pl.DataFrame()
        return [
            (
                sid,
                _calc_factor(
                    closes[sid],
                    adjs.get(sid, empty_df),
                    rgts.get(sid, empty_df),
                    forward,
                ),
            )
            for sid in sid_batch
        ]

    async def _run_chunk(sid_batch):
        loop = asyncio.get_running_loop()
        async with sem:
            return await loop.run_in_executor(None, _calc_batch, sid_batch)

    batch_results = await asyncio.gather(*[_run_chunk(chunk) for chunk in chunks])
    out = {}
    for batch in batch_results:
        out.update(batch)
    return out


def _apply_factor(df: pl.DataFrame, adj_factors: dict, adjust_type: int) -> pl.DataFrame:
    if adjust_type not in (1, 2):
        # mirror the strict validation in _calc_factor: anything that is not
        # 1 (qfq) / 2 (hfq) must fail fast instead of silently falling back
        # to hfq
        raise ValueError(f"Invalid adjust type: {adjust_type}, expected 1 (qfq) or 2 (hfq)")

    if df.height == 0:
        return df

    # Tick ---> Day
    # NOTE: server `tick` is UTC second-precision epoch. Must convert UTC ->
    # Asia/Shanghai before extracting YYYYMMDD so timestamps in 00:00..07:59 CST
    # (16:00..23:59 UTC of the previous day) align with the server's own
    # _DAY_COLUMN (`datetime AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Shanghai'`).
    if "tick" in df.columns and "day" not in df.columns:
        if not df["tick"].is_sorted():
            df = df.sort("tick")
        sh_dt = (
            pl.from_epoch(pl.col("tick"), time_unit="s")
            .dt.replace_time_zone("UTC")
            .dt.convert_time_zone("Asia/Shanghai")
        )
        df = df.with_columns(
            day=(
                sh_dt.dt.year() * 10000
                + sh_dt.dt.month().cast(pl.Int32) * 100
                + sh_dt.dt.day().cast(pl.Int32)
            ).cast(pl.Int32)
        ).set_sorted("day")

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
    if not df["day"].is_sorted():
        df = df.sort("day")
    else:
        df = df.set_sorted("day")

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