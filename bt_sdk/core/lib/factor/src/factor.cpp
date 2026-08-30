#include <vector>
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <map>

#include "factor.hpp"

namespace rpc_feed {
namespace adjust {

FactorResult calc_adjust_factors(
    const std::vector<int>& trading_dates,
    const std::vector<double>& close,
    const std::vector<AdjustmentEvent>& adj_events,
    const std::vector<RightmentEvent>& right_events,
    AdjustType type)
{
    if (close.size() != trading_dates.size()) {
        throw std::invalid_argument("close size must match trading_dates size");
    }
    
    // 
    std::map<int, double> event_factors;
    
    // lambda 
    auto process_event = [&](int ex_date, double factor) {
        if (event_factors.count(ex_date)) {
            event_factors[ex_date] *= factor;
        } else {
            event_factors[ex_date] = factor;
        }
    };
    
    // process adjustment events
    // trading_dates is sorted ascending (server ORDER BY sid, day ASC; the SDK
    // also sorts before extraction), so lower_bound is valid.
    for (const auto& e : adj_events) {
        // ex_date can fall inside a suspension gap: the close stream emits rows
        // only for days with trades, so an ex_date with no row would never be
        // found by exact match (verified live: ~3% of adjustment events).
        // Map it to the first trading date >= ex_date (resumption day) — the
        // exchange applies the ex-right at resumption, and the last traded
        // close before it is the correct preclose reference.
        auto it = std::lower_bound(trading_dates.begin(), trading_dates.end(), e.ex_date);
        if (it == trading_dates.end() || it == trading_dates.begin()) continue;

        size_t idx = it - trading_dates.begin();
        double preclose = close[idx - 1];
        if (preclose <= 0) continue;

        double stock_ratio = e.bonus_share / Multiply   + e.transfer / Multiply;
        double cash_ratio = e.bonus / (Multiply * preclose);
        double factor = (1.0 - cash_ratio) / (1.0 + stock_ratio);
        process_event(*it, factor);
    }
    
    // process rightment events
    // Rightment: shareholders buy additional shares at a discounted offer
    // price. Value conservation over the ex-right basket gives the exchange
    // reference price
    //   ref = (preclose + price * r) / (1 + r),   r = ratio / 10
    // so the per-event factor applied to pre-close prices is
    //   factor = (preclose + price * r) / (preclose * (1 + r))
    // The previous formula 1/(1+r) implicitly assumed price == preclose and
    // mispriced discounted offers (verified live on 600030 2022-01-27:
    // 10-for-1.5 @ 14.43 with preclose 25.70 -> ref 24.23, actual 24.15,
    // while 1/(1+r) implied 22.35, a -7.3% error).
    // Same lower_bound alignment as adjustment events: ex_date may fall in a
    // suspension gap, and mapping to the resumption day lets same-day
    // adj+rgt events merge on one key.
    for (const auto& e : right_events) {
        auto it = std::lower_bound(trading_dates.begin(), trading_dates.end(), e.ex_date);
        if (it == trading_dates.end() || it == trading_dates.begin()) continue;

        size_t idx = it - trading_dates.begin();
        double preclose = close[idx - 1];
        if (preclose <= 0) continue;

        double r = e.ratio / Multiply;
        if (r <= -1.0) continue; // guard degenerate ratio (division by zero)

        double factor = (preclose + e.price * r) / (preclose * (1.0 + r));
        process_event(*it, factor);
    }
    
    std::map<int, double> adj_factors;
    
    if (event_factors.empty()) {
        return FactorResult{event_factors, adj_factors};
    }
    
    // order by ex_date
    std::vector<std::pair<int, double>> sorted_events;
    sorted_events.reserve(event_factors.size());
    for (const auto& kv : event_factors) {
        sorted_events.push_back(kv);
    }
    std::sort(sorted_events.begin(), sorted_events.end());
    
    if (type == AdjustType::Forward) {
        double cum_factor = 1.0;
        for (auto it = sorted_events.rbegin(); it != sorted_events.rend(); ++it) {
            if (std::abs(it->second) > 1e-9) { // avoid zero, same guard as Backward
                cum_factor *= it->second;
            }
            adj_factors[it->first] = cum_factor;
        }
    } else if (type == AdjustType::Backward) {
        double cum_factor = 1.0;
        for (const auto& it : sorted_events) {
            if (std::abs(it.second) > 1e-9) { // avoid zero 
                cum_factor /= it.second; 
            }
            adj_factors[it.first] = cum_factor;
        }
    }
    
    return FactorResult{event_factors, adj_factors};
}

}
}