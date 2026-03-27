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
    for (const auto& e : adj_events) {
        auto it = std::find(trading_dates.begin(), trading_dates.end(), e.ex_date);
        if (it == trading_dates.end() || it == trading_dates.begin()) continue;
        
        size_t idx = it - trading_dates.begin();
        if (idx == 0) continue;
        
        double preclose = close[idx - 1];
        if (preclose <= 0) continue;
        
        double stock_ratio = e.bonus_share / Multiply   + e.transfer / Multiply;
        double cash_ratio = e.bonus / (Multiply * preclose);
        double factor = (1.0 - cash_ratio) / (1.0 + stock_ratio);
        process_event(e.ex_date, factor);
    }
    
    // process rightment events
    for (const auto& e : right_events) {
        double factor = 1.0 / (1.0 + e.ratio / Multiply);
        process_event(e.ex_date, factor);
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
            cum_factor *= it->second;
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
