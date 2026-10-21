from bt_protocol.constant import RpcTopic


MaxDate = 30000000

_QUOTE_SCALE = {
    "tick": 1.0, "open": 1e-5, "high": 1e-5, "low": 1e-5, "close": 1e-5, "volume": 1e-3, "amount": 1e-3,
}
_BENCHMARK_QUOTE_SCALE = {
    "tick": 1.0, "open": 1e-5, "high": 1e-5, "low": 1e-5, "close": 1e-5, "volume": 1.0, "amount": 1.0,
}

Scale = {
    RpcTopic.Tick: _QUOTE_SCALE,
    RpcTopic.Close: _QUOTE_SCALE,
    RpcTopic.Daily: _QUOTE_SCALE,
    RpcTopic.Adjustment: {
        "bonus_share": 1e-3, "transfer": 1e-3, "bonus": 1e-3,
    },
    RpcTopic.Rightment: {
        "price": 1e-3, "ratio": 1e-3,
    },
}

# Benchmark indices store volume and amount at 1x (unscaled) to avoid overflow:
# setting factor=1.0 yields ndigits=0 in get_scale_ndigits, skipping 1e-3 scaling.
BenchmarkScale = {
    RpcTopic.Tick: _BENCHMARK_QUOTE_SCALE,
    RpcTopic.Close: _BENCHMARK_QUOTE_SCALE,
    RpcTopic.Daily: _BENCHMARK_QUOTE_SCALE,
}
