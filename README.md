# bt_sdk

> 高性能量化行情数据 SDK — Cython + C++17 + gRPC Async + ReactiveX

[![Python](https://img.shields.io/badge/Python-3.11%2B-blue)](https://www.python.org/)
[![Cython](https://img.shields.io/badge/Cython-3.x-green)](https://cython.org/)
[![C++](https://img.shields.io/badge/C%2B%2B-17-orange)](https://isocpp.org/)
[![gRPC](https://img.shields.io/badge/gRPC-async-red)](https://grpc.io/)

---

## ✨ 特性亮点

### 🚀 极致性能

| 技术 | 实现细节 |
|------|----------|
| **Cython 热路径** | gRPC 通信层、UUID 生成均用 `cdef`/`cpdef` 编译，关闭 `boundscheck`/`wraparound` |
| **C++17 因子引擎** | 复权因子计算用 pybind11 绑定，`-O3` 优化，纯计算**释放 GIL** (`py::call_guard<py::gil_scoped_release>()`) |
| **Arrow 零拷贝管道** | Arrow IPC `zero_copy` 解析；scale 列变换后用 `Table.from_arrays` 原数组引用重组（不拷贝未修改列）→ `pl.from_arrow(rechunk=False)` |
| **有界并行因子计算** | `calc_factor_async` 用 `Semaphore(cpu_count)` 限流，避免上千标的全部驻留内存 |
| **libuuid C 级调用** | UUID 生成直接调 `libuuid`，`nogil` 上下文，无 Python 对象开销 |

### ⚡ 并发安全

| 问题 | 解决方案 |
|------|----------|
| **连接初始化竞态** | `asyncio.Lock` + double-check 保护 gRPC channel 创建，防止高并发首波请求创建多 channel；`cleanup` 先摘引用再关 channel，避免与并发 `initialize` 交错 |
| **Task GC 回收** | 同 loop 路径 `_pending_tasks` 持有 Task 强引用；跨 loop 路径 `_pending_futs` 持有 `run_coroutine_threadsafe` Future 强引用，`finally` 中自动清理 |
| **关闭时在途流未终止** | `_async_shutdown` 主动 cancel 全部在途 Task/Future（有界等待 5s），再关闭 gRPC channel |
| **pxd 槽位遮蔽** | `loop`/`timeout` 仅在基类声明一次，派生类重复声明会创建第二个 C 槽位导致 `close()` 读到 None（曾使 channel 关闭路径整体失效） |
| **Subject 隔离与释放** | `wrap_protocol` 为每次订阅隔离 `Subject` 并在 `teardown` 中自动 `dispose()`；查询接口统一走 `fetch_tables_async` 零 RxPY 开销 |
| **跨 Loop 调用** | 自动检测当前 loop，`run_coroutine_threadsafe` + `wrap_future` 安全跨线程 |

### 📡 响应式数据流

```python
# gRPC stream → RxPY Observable → 管道操作
observable = md_api.subscribe(query, RpcTopic.Tick)

observable.pipe(
    ops.map(lambda d: d["data"]),
    ops.buffer_with_time_or_count(1.0, 500),
    ops.filter(lambda batch: len(batch) > 0)
).subscribe(on_next=process_batch)
```

### 🔀 三模式 API

同一数据源，三种调用方式：

```python
# 1. 同步（阻塞，适合脚本/回测）
df = md_api.get_instrument()

# 2. 异步（非阻塞，适合 Ray Actor / asyncio 应用）
df = await md_api.get_instrument_async()

# 3. 订阅流（持续推送，适合实时策略）
obs = md_api.subscribe(query, RpcTopic.Tick)
```

---

## 🏗️ 架构

```
┌─────────────────────────────────────────────────────────────┐
│                      用户代码 / 策略引擎                        │
├─────────────────────────────────────────────────────────────┤
│  ctx.py  — external_mdapi_context / initialize_runner        │
│            全局 AsyncRunner 单例（后台守护线程 + 独立 loop）      │
├─────────────────────────────────────────────────────────────┤
│  MdApi (api.pyx)                                            │
│  ├── get_instrument_async()  →  pl.DataFrame                │
│  ├── get_factor_async()      →  asyncio.gather × 3 + C++    │
│  ├── rpc_async()             →  跨 loop 直查                  │
│  └── subscribe()             →  Observable                   │
├─────────────────────────────────────────────────────────────┤
│  AsyncRpcClient (async_client.pyx)                          │
│  ├── wrap_protocol → (Observable, Subject)                  │
│  ├── _ensure_connection → asyncio.Lock + double-check       │
│  └── _pending_tasks → Task 强引用防 GC                         │
├─────────────────────────────────────────────────────────────┤
│  RpcClient (rpc/client.pyx)                                 │
│  ├── grpc.aio channel (keepalive + HTTP2 窗口调优)            │
│  ├── rpc_callback → Arrow IPC → Table.from_arrays scale     │
│  └── _dispatch_rpc → Instrument/Tick/Daily/Close/Adj/Rgt     │
├─────────────────────────────────────────────────────────────┤
│  C++ Factor Engine (lib/factor/)                            │
│  ├── calc_adjust_factors()  [GIL released]                  │
│  ├── Forward / Backward 复权                                  │
│  └── Polars join_asof 应用因子                                 │
└─────────────────────────────────────────────────────────────┘
```

---

## 📦 安装

### 从源码构建

```bash
# 安装 Poetry
curl -sSL https://install.python-poetry.org | python3 -

# 安装依赖并编译 Cython/C++ 扩展
poetry install

# 或仅编译扩展（开发模式）
poetry run python setup.py build_ext --inplace
```

### 构建 Wheel

```bash
poetry build --format wheel
```

**系统要求**：Python ≥ 3.11、CMake、C++17 编译器（clang/gcc/MSVC）

---

## 🚀 快速上手

### 异步模式（推荐）

```python
import asyncio
from bt_sdk.ctx import external_mdapi_context
from bt_protocol._protocol import QueryBody
from bt_protocol.constant import RpcTopic, FactorTopic

async def main():
    with external_mdapi_context() as md_api:
        # 查询合约列表
        instruments = await md_api.get_instrument_async()
        print(f"Instruments: {len(instruments)} rows")

        # 查询复权因子（内部 3 路 gRPC 并发 + C++ 并行计算）
        query = QueryBody(start_date=20100101, end_date=20260630, sid=[b"300374"])
        factors = await md_api.get_factor_async(query, FactorTopic.Qfq)
        print(f"Factors: {list(factors.keys())}")

asyncio.run(main())
```

### 同步模式

```python
from bt_sdk.ctx import external_mdapi_context

with external_mdapi_context() as md_api:
    df = md_api.get_instrument()  # 阻塞调用，内部转发到 runner loop
    print(df)
```

### 订阅实时流

```python
import asyncio
import reactivex.operators as ops
from bt_sdk.ctx import external_mdapi_context
from bt_protocol.constant import RpcTopic

async def main():
    with external_mdapi_context() as md_api:
        query = QueryBody(start_date=20200101, end_date=20260630, sid=[b"300374"])
        observable = md_api.subscribe(query, RpcTopic.Tick)

        chan = []
        loop = asyncio.get_running_loop()
        done = loop.create_future()

        observable.pipe(
            ops.map(lambda d: d["data"]),
        ).subscribe(
            on_next=chan.append,
            on_completed=lambda: loop.call_soon_threadsafe(done.set_result, True),
            on_error=lambda e: loop.call_soon_threadsafe(done.set_exception, e),
        )

        await done
        print(f"Received {len(chan)} batches")

asyncio.run(main())
```

---

## 📚 API 参考

### `external_mdapi_context(addr_str=None, timeout=30)`

上下文管理器，自动初始化全局 `AsyncRunner` 并绑定 `MdApi`。

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `addr_str` | `str` | `env MD_ADDR` 或 `127.0.0.1:50051` | gRPC 服务地址 |
| `timeout` | `int` | `30` | 单次请求超时（秒） |

### `MdApi`

| 方法 | 返回类型 | 说明 |
|------|----------|------|
| `get_instrument()` | `pl.DataFrame` | 同步查询合约列表 |
| `get_instrument_async()` | `pl.DataFrame` | 异步查询合约列表 |
| `get_factor(body, forward)` | `Dict[bytes, FactorResult]` | 同步计算复权因子（内部自动拉取**全历史** close/adj/rgt 事件） |
| `get_factor_async(body, forward)` | `Dict[bytes, FactorResult]` | 异步并行计算（3 路 gRPC + C++ 有界并行） |
| `rpc_async(body, rpc_type)` | `pl.DataFrame` | 跨 loop 直查 |
| `subscribe(body, topic)` | `Observable` | 订阅实时数据流 |

> **复权因子为何拉全历史**：hfq 锚定上市日（需要窗口之前的除权事件），qfq 锚定最新价（需要窗口之后的事件），且每个事件的因子都需要 ex_date 前一交易日的 close 作锚点。`get_factor*` 会自动把请求加宽到 `[19900101, 20991231]`（合法 YYYYMMDD 边界；哨兵值如 `0`/`MaxDate` 会被服务端解析为非法时间戳），传入的 `start_date/end_date` 仅作为 sid 过滤之外的语义保留。也因此大量标的的全历史因子计算属于批量操作，注意内存与超时预算（`timeout` 参数即流收集预算，默认取 MdApi 级默认值）。

同步方法与 `subscribe` 在未 `start(loop)`（或 loop 已关闭）时会立即抛出带明确信息的 `RuntimeError`。
- **单例 `timeout` 语义**：`GetMdApi(addr, timeout)` 按 `addr` 缓存进程级共享单例，再次调用传入不同正数 `timeout` 时会更新该共享实例的默认超时预算（last caller wins）；若单次调用需独立超时，请直接在 `get_instrument(timeout=...)` / `get_factor(..., timeout=...)` / `rpc_async(..., timeout=...)` 上传参。
- **`disconnect()` vs `DisposeMdApi(addr)`**：`disconnect()` 仅关闭当前底层 gRPC channel（实例仍保留在注册表中，后续 `GetMdApi(addr)` 或 `start(loop)` 会自动复活并在下次请求时重连）；`DisposeMdApi(addr)` 则关闭连接并将其从全局注册表中移除；进程退出由 `atexit` 自动调用 `dispose()` 统一清理。

### 数据主题

| Topic | 说明 | Scale 字段 |
|-------|------|-----------|
| `RpcTopic.Instrument` | 合约列表 | — |
| `RpcTopic.Tick` | Tick 逐笔 | open/high/low/close ×1e-5, volume ×1e-3 |
| `RpcTopic.Daily` | 日线 | 同 Tick |
| `RpcTopic.Close` | 收盘价 | 同 Tick |
| `RpcTopic.Adjustment` | 除权除息 | bonus_share/transfer/bonus ×1e-3 |
| `RpcTopic.Rightment` | 配股 | price/ratio ×1e-3 |

---

## 🔧 性能优化详解

### GIL 释放（C++ → Python 并行）

```cpp
// pybind_factor.cpp
m.def("calc_adjust_factors", &calc_adjust_factors, ...,
      py::call_guard<py::gil_scoped_release>());  // ← 计算期间释放 GIL
```

C++ 因子计算不回调 Python，释放 GIL 后其他协程可并发执行。配合 `asyncio.gather` + `run_in_executor` 实现 **N 只股票并行计算**。

### Arrow 零拷贝

```python
# rpc_callback: 未修改的列以原数组引用进入新 Table（from_arrays 不拷贝数据），
# 仅 scale/cast 列产生新数组（这是 scale 语义的必要成本）
table = pa.Table.from_arrays(cols, names=names, metadata=table.schema.metadata)

# _merge2DataFrame: rechunk=False 避免 Polars 额外拷贝
aligned[sid] = pl.from_arrow(table, rechunk=False)
```

### 批量列提取

```python
# 旧: iter_rows(named=True) → 逐行 Python dict（慢 10-50x）
# 新: to_list() → C 级批量提取
ex_dates = df["ex_date"].to_list()
bonus_shares = df["bonus_share"].to_list()
```

---

## 🧪 测试

```bash
# 运行全部测试（需要 gRPC 服务端）
MD_ADDR=127.0.0.1:50051 poetry run pytest

# 运行并发修复单元测试（纯本地，无需服务端）
poetry run pytest tests/test_concurrency_fix.py -v
```

---

## 📁 项目结构

```
bt_sdk/
├── ctx.py                         # 全局入口：runner 生命周期 + context manager
├── core/
│   ├── factor.py                  # 复权因子：Polars 处理 + C++ 调度（同步/异步）
│   ├── client/
│   │   ├── api.pyx / api.pxd     # MdApi：同步/异步/订阅接口
│   │   └── async_client.pyx      # AsyncRpcClient：Observable + 并发安全
│   ├── rpc/
│   │   └── client.pyx            # gRPC 底层：channel + Arrow 解码 + topic 分发
│   └── lib/factor/               # C++17 pybind11 复权因子引擎
│       ├── include/factor.hpp    # 数据结构定义
│       ├── src/factor.cpp        # 核心算法
│       └── pybind_factor.cpp     # Python 绑定（GIL released）
└── utils/
    ├── runner.py                 # AsyncRunner：后台 asyncio loop 单例
    ├── util.pyx                  # UUID + DataFrame 合并（Cython 加速）
    └── wrapper.py                # 装饰器工具集
```

---

## ⚠️ 注意事项

- **Python 版本**：要求 ≥ 3.11（使用了 `asyncio.Lock` 无参构造等新特性）
- **gRPC fork**：已设 `GRPC_ENABLE_FORK_SUPPORT=0`，避免 macOS spawn 问题
- **内网通信**：gRPC 使用 `insecure_channel`（无 TLS），仅适用于可信内网
- **边界检查关闭**：Cython 模块关闭了 `boundscheck`/`wraparound`，输入数据须保证合法

---

## 📄 License

Proprietary — Internal use only.