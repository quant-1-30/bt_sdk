#include <vector>
#include <algorithm>
#include <cstdint>


namespace rpc_feed {
namespace adjust {

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

// 用于返回两个结果
struct FactorResult {
    std::map<int, double> raw_factors; // 每天事件因子
    std::map<int, double> adj_factors; // 累计复权因子
};

int get_pre_index(const std::vector<int>& trading_dates, int ex_date);

FactorResult calc_adjust_factors(
    const std::vector<int>& trading_dates,
    const std::vector<double>& close,
    const std::vector<AdjustmentEvent>& adj_events,
    const std::vector<RightmentEvent>& right_events,
    AdjustType type);

}
}