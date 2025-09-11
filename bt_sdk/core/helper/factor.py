
#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../lib/factor/lib'))
import adj_factor

__all__ = ["calc_factor"]


def adjust2struct(data):
    event = adj_factor.AdjustmentEvent()
    event.ex_date = data[2]
    event.bonus_share = data[3]
    event.transfer = data[4]
    event.bonus = data[5]
    return event        

def right2struct(data):
    """
        Convert rightment data to RgtStruct
    """
    event = adj_factor.RightmentEvent() 
    event.ex_date = data[2]
    event.price = data[3]
    event.ratio = data[4]
    return event

def calc_factor(close, adjust, right, forward=True):
    vector_trading = [c["body"]["line"][0][0] for c in close]
    vector_close = [c["body"]["line"][0][1] for c in close]
    vector_adjust_event = [adjust2struct(data["body"]) for data in adjust]
    vector_right_event = [right2struct(data["body"]) for data in right]

    factor_type = adj_factor.AdjustType.Forward if forward else adj_factor.AdjustType.Backward 
    factors = adj_factor.calc_adjust_factors(
        vector_trading, 
        vector_close, 
        vector_adjust_event, 
        vector_right_event, 
        factor_type
    )
    return factors
