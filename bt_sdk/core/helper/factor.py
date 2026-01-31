#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../lib/factor/lib')) # append C++ binding
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
            event.ex_date = table_data.column("ex_data")[0]
            event.price = table_data.column("price")[0]
            event.ratio = table_data.column("ratio")[0]
            events.append(event)
    return events


def calc_factor(close, adjust, right, forward=True):
    if not close:
        return {}
    vector_trading = close.column("date").to_pylist()
    vector_close = close.column("close").to_pylist()
    vector_adjust_event = adjust2struct(adjust)
    vector_right_event = right2struct(right) 
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

# factor.pyx

# cdef list adjust2struct(object table_data):
#     cdef list events = []
#     cdef int num_rows = table_data.num_rows
#     cdef int i

#     for i range(num_rows):
#         event = adj_factor.AdjustmentEvent()
#         event.ex_date = table_data["ex_date"][i]
#         event.bonus_share = table_data["bonus_share"][i]
#         event.transfer = table_data["transfer"][i]
#         event.bonus = table_data["bonus"][i]
#         events.append(event)
#     return event        

# cdef list right2struct(object table_data):
#     """
#         Convert rightment data to RgtStruct
#     """
#     cdef list events = []
#     cdef int num_rows = table_data.num_rows
#     cdef int i

#     for i in range(num_rows):
#         event = adj_factor.RightmentEvent() 
#         event.ex_date = table_data["ex_data"][0]
#         event.price = table_data["price"][0]
#         event.ratio = table_data["ratio"][0]
#         events.append(event)
#     return events

# cdef dict calc_factor(object close, object adjust, object right, bint forward=True):
#     cdeff list vector_trading, vector_close, vector_adjust_event, vector_right_event
#     vector_trading = close.column("date")
#     # cdef cnp.ndarray arr = table[col_name].to_numpy()
#     # cdef vector[double] v = arr
#     vector_close = close.column("close")
#     vector_adjust_event = adjust2struct(dadjust)
#     vector_right_event = right2struct(right) 

#     factor_type = adj_factor.AdjustType.Forward if forward else adj_factor.AdjustType.Backward 
#     factors = adj_factor.calc_adjust_factors(
#         vector_trading, 
#         vector_close, 
#         vector_adjust_event, 
#         vector_right_event, 
#         factor_type
#     )
#     return factors
