from bt_protocol.constant import RpcTopic


MaxDate=30000000

Scale = {
        # benchmark amount not  1e-3
        RpcTopic.Tick: {
            "tick": 1.0, "open": 1e-5, "high": 1e-5, "low": 1e-5, "close": 1e-5, "volume": 1e-3, "amount": 1e-3,
        }, 
        RpcTopic.Close: {
            "tick": 1.0, "open": 1e-5, "high": 1e-5, "low": 1e-5, "close": 1e-5, "volume": 1e-3, "amount": 1e-3,
        },
        RpcTopic.Daily: {
            "tick": 1.0, "open": 1e-5, "high": 1e-5, "low": 1e-5, "close": 1e-5, "volume": 1e-3, "amount": 1e-3,
        },
        RpcTopic.Adjustment: {
            "bonus_share": 1e-3, "transfer": 1e-3, "bonus": 1e-3, # adjustment
        },
        RpcTopic.Rightment: {
            "price": 1e-3, "ratio": 1e-3 # rightment
        },
}
