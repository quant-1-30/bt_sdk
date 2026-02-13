#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'lib/factor/lib')) # append C++ binding
import adj_factor


def adjust2struct(table_data):
    events = []
    if table_data:
        num_rows = table_data.num_rows

        for i in range(num_rows):
            event = adj_factor.AdjustmentEvent()
            event.ex_date = table_data.column("ex_date")[i]
            event.bonus_share = table_data.column("bonus_share")[i]
            event.transfer = table_data.column("transfer")[i]
            event.bonus = table_data.column("bonus")[i]
            events.append(event)
    return events        


def right2struct(table_data):
    """
        Convert rightment data to RgtStruct
    """
    events = []

    if table_data:
        num_rows = table_data.num_rows

        for i in range(num_rows):
            event = adj_factor.RightmentEvent() 
            event.ex_date = table_data.column("ex_date")[0]
            event.price = table_data.column("price")[0]
            event.ratio = table_data.column("ratio")[0]
            events.append(event)
    return events


def _calc_factor(c_table, adj_table, rgt_table, forward):
    vector_trading = c_table.column("day").to_pylist()
    vector_close = c_table.column("close").to_pylist()
    vector_adjust_event = adjust2struct(adj_table)
    vector_right_event = right2struct(rgt_table) 
    # print("calc_factor vector :", vector_trading, vector_close, vector_adjust_event, vector_right_event)

    factor_type = adj_factor.AdjustType.Forward if forward else adj_factor.AdjustType.Backward 
    factors = adj_factor.calc_adjust_factors(
        vector_trading, 
        vector_close, 
        vector_adjust_event, 
        vector_right_event, 
        factor_type
    )
    return factors


def calc_factor(closes, adjs, rgts, sids, forward=True):
    if not closes:
        return {}

    factor_sids = {}
    for sid in sids:
        close_table = closes.get(sid, {})
        adj_table = adjs.get(sid, {})
        rgt_table = rgts.get(sid, {})
        factor_sids[sid] = _calc_factor(close_table, adj_table, rgt_table, forward=forward) 
    return factor_sids
