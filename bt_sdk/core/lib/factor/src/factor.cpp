#include <vector>
#include <algorithm>
#include <cstdint>
#include <cmath>
#include <map>

#include "factor.hpp"

namespace rpc_feed {
namespace adjust {

namespace {

// Map an ex_date (which may fall inside a trading suspension gap) to the first
// trading date >= ex_date (resumption day) and return the last traded close
// strictly before it as `preclose`.
// Domain rule (N1): when `it == dates_end` (`ex_date > trading_dates.back()`),
// the corporate action is an announced FUTURE event that has NOT been
// implemented yet — no ex-right price jump has occurred and no preclose exists,
// so it MUST NOT be included in adjustment factors.
inline bool find_preclose(
    const int* dates_begin,
    const int* dates_end,
    const double* close,
    int ex_date,
    int& matched_date,
    double& preclose)
{
    auto it = std::lower_bound(dates_begin, dates_end, ex_date);
    if (it == dates_end || it == dates_begin) return false;
    size_t idx = static_cast<size_t>(it - dates_begin);
    preclose = close[idx - 1];
    if (preclose <= 0) return false;
    matched_date = *it;
    return true;
}

inline void merge_event(std::map<int, double>& event_factors, int ex_date, double factor)
{
    auto it = event_factors.find(ex_date);
    if (it != event_factors.end()) {
        it->second *= factor;
    } else {
        event_factors.emplace(ex_date, factor);
    }
}

inline void apply_adj_event(
    const int* dates_begin,
    const int* dates_end,
    const double* close,
    int ex_date,
    double bonus_share,
    double transfer,
    double bonus,
    std::map<int, double>& event_factors)
{
    int matched_date = 0;
    double preclose = 0.0;
    if (!find_preclose(dates_begin, dates_end, close, ex_date, matched_date, preclose)) {
        return;
    }
    double stock_ratio = bonus_share / Multiply + transfer / Multiply;
    double cash_ratio = bonus / (Multiply * preclose);
    double factor = (1.0 - cash_ratio) / (1.0 + stock_ratio);
    merge_event(event_factors, matched_date, factor);
}

inline void apply_rgt_event(
    const int* dates_begin,
    const int* dates_end,
    const double* close,
    int ex_date,
    double price,
    double ratio,
    std::map<int, double>& event_factors)
{
    int matched_date = 0;
    double preclose = 0.0;
    if (!find_preclose(dates_begin, dates_end, close, ex_date, matched_date, preclose)) {
        return;
    }
    double r = ratio / Multiply;
    if (r <= -1.0) return;
    double factor = (preclose + price * r) / (preclose * (1.0 + r));
    merge_event(event_factors, matched_date, factor);
}

inline FactorResult accumulate_factors(
    std::map<int, double>&& event_factors,
    AdjustType type)
{
    std::map<int, double> adj_factors;
    if (event_factors.empty()) {
        return FactorResult{std::move(event_factors), std::move(adj_factors)};
    }

    if (type == AdjustType::Forward) {
        double cum_factor = 1.0;
        for (auto it = event_factors.rbegin(); it != event_factors.rend(); ++it) {
            if (std::abs(it->second) > 1e-9) {
                cum_factor *= it->second;
            }
            adj_factors[it->first] = cum_factor;
        }
    } else if (type == AdjustType::Backward) {
        double cum_factor = 1.0;
        for (const auto& kv : event_factors) {
            if (std::abs(kv.second) > 1e-9) {
                cum_factor /= kv.second;
            }
            adj_factors[kv.first] = cum_factor;
        }
    }

    return FactorResult{std::move(event_factors), std::move(adj_factors)};
}

} // namespace

FactorResult calc_adjust_factors_columns(
    const int* trading_dates,
    size_t n_dates,
    const double* close,
    size_t n_close,
    const int* adj_ex_dates,
    const double* adj_bonus_share,
    const double* adj_transfer,
    const double* adj_bonus,
    size_t n_adj,
    const int* rgt_ex_dates,
    const double* rgt_price,
    const double* rgt_ratio,
    size_t n_rgt,
    AdjustType type)
{
    if (n_close != n_dates) {
        throw std::invalid_argument("close size must match trading_dates size");
    }

    std::map<int, double> event_factors;
    if ((n_adj == 0 && n_rgt == 0) || n_dates == 0) {
        return FactorResult{std::move(event_factors), {}};
    }

    const int* dates_begin = trading_dates;
    const int* dates_end = trading_dates + n_dates;

    for (size_t i = 0; i < n_adj; ++i) {
        apply_adj_event(
            dates_begin, dates_end, close,
            adj_ex_dates[i], adj_bonus_share[i], adj_transfer[i], adj_bonus[i],
            event_factors);
    }

    for (size_t i = 0; i < n_rgt; ++i) {
        apply_rgt_event(
            dates_begin, dates_end, close,
            rgt_ex_dates[i], rgt_price[i], rgt_ratio[i],
            event_factors);
    }

    return accumulate_factors(std::move(event_factors), type);
}

FactorResult calc_adjust_factors(
    const int* trading_dates,
    size_t n_dates,
    const double* close,
    size_t n_close,
    const std::vector<AdjustmentEvent>& adj_events,
    const std::vector<RightmentEvent>& right_events,
    AdjustType type)
{
    if (n_close != n_dates) {
        throw std::invalid_argument("close size must match trading_dates size");
    }

    std::map<int, double> event_factors;
    if ((adj_events.empty() && right_events.empty()) || n_dates == 0) {
        return FactorResult{std::move(event_factors), {}};
    }

    const int* dates_begin = trading_dates;
    const int* dates_end = trading_dates + n_dates;

    for (const auto& e : adj_events) {
        apply_adj_event(
            dates_begin, dates_end, close,
            e.ex_date, e.bonus_share, e.transfer, e.bonus,
            event_factors);
    }

    for (const auto& e : right_events) {
        apply_rgt_event(
            dates_begin, dates_end, close,
            e.ex_date, e.price, e.ratio,
            event_factors);
    }

    return accumulate_factors(std::move(event_factors), type);
}

FactorResult calc_adjust_factors(
    const std::vector<int>& trading_dates,
    const std::vector<double>& close,
    const std::vector<AdjustmentEvent>& adj_events,
    const std::vector<RightmentEvent>& right_events,
    AdjustType type)
{
    return calc_adjust_factors(
        trading_dates.data(),
        trading_dates.size(),
        close.data(),
        close.size(),
        adj_events,
        right_events,
        type);
}

}
}