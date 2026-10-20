#include <vector>
#include <algorithm>
#include <cstdint>


namespace rpc_feed {
namespace adjust {

const int Multiply = 10; 

struct AdjustmentEvent {
    int ex_date;       // ex_date
    double bonus_share; // 送股
    double transfer;    // 转股
    double bonus;       // 现金分红
};

struct RightmentEvent {
    int ex_date;       // ex_date
    double price;   // 配股价格
    double ratio;   // 配股比例
};

enum class AdjustType {
    Forward,  // 前复权
    Backward  // 后复权
};


struct FactorResult {
    std::map<int, double> raw_factors; // 每天事件因子
    std::map<int, double> adj_factors; // 前/后复权累计复权因子
};

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
    AdjustType type);

FactorResult calc_adjust_factors(
    const int* trading_dates,
    size_t n_dates,
    const double* close,
    size_t n_close,
    const std::vector<AdjustmentEvent>& adj_events,
    const std::vector<RightmentEvent>& right_events,
    AdjustType type);

FactorResult calc_adjust_factors(
    const std::vector<int>& trading_dates,
    const std::vector<double>& close,
    const std::vector<AdjustmentEvent>& adj_events,
    const std::vector<RightmentEvent>& right_events,
    AdjustType type);

}
}