## 1. 项目概览

- **名称**：`bt-sdk`（包目录名为 `bt_sdk`）
- **版本**：`0.14.3`
- **定位**：面向市场数据（Market Data）的 Python SDK，封装了与后端 `btDataFeed` gRPC 服务的通信、数据解码、复权因子计算等功能。
- **核心能力**：
  - 通过 gRPC 流式拉取 Instrument（合约）、Tick、Daily、Close、Adjustment（除权除息）、Rightment（配股）等行情数据。
  - 把服务端返回的 Arrow IPC bytes 解码为 `pyarrow.Table`，并转换为 `polars.DataFrame`。
  - 提供同步和异步两类 API（`get_instrument` / `get_instrument_async`、`get_factor` / `get_factor_async` 等）。
  - 使用 Cython 编写高性能网络/工具模块，使用 pybind11 + C++17 编写复权因子计算模块。
- **项目仓库语言**：代码标识符使用英文；注释、README 笔记、提交记录主要使用中文。

## 2. 技术栈

- **语言**：Python（要求 `>=3.11,<3.15`），Cython 扩展，C++17 扩展。
- **包管理**：Poetry（`pyproject.toml` + `poetry.lock`）。
- **构建工具**：
  - `poetry-core` 作为 PEP 517 构建后端。
  - 自定义构建脚本 `build_ext.py`（内部调用 `setup.py` 的 `get_ext_modules()`）。
  - Cython 3.x 编译 `.pyx`，pybind11 编译 C++ 扩展。
  - 构建依赖：`poetry-core`、`wheel`、`pybind11`、`cmake`、`Cython>=3.0`、`numpy`、`setuptools`。
- **网络通信**：`grpcio`（`grpc.aio` 异步通道）、`bt-protocol`（提供 protobuf 定义和序列化）。
- **数据与计算**：`pyarrow`、`polars`、`pandas`、`numpy`、`msgspec`、`msgpack`。
- **响应式流**：`reactivex`（RxPY），用于把 gRPC 流封装成 `Observable`。
- **其他**：`toolz`、`ping3`、`urllib3`、`pydantic`、`typing-extensions`、`annotated-types`、`pygments`。

## 3. 项目结构

```text
bt_sdk/
├── ctx.py                    # 入口：external_mdapi_context / get_md_api / AsyncRunner 全局生命周期
├── core/
│   ├── factor.py             # 复权因子 Polars 处理 + 调用 C++ adj_factor
│   ├── client/
│   │   ├── api.pyx / api.pxd # MdApi：同步/异步/订阅接口（Cython）
│   │   └── async_client.pyx / async_client.pxd  # AsyncRpcClient：Observable 包装 gRPC 流（Cython）
│   ├── rpc/
│   │   └── client.pyx / client.pxd  # 底层 gRPC 客户端、Arrow 解码、topic 分发（Cython）
│   └── lib/factor/           # C++17 pybind11 扩展
│       ├── include/factor.hpp
│       ├── src/factor.cpp
│       └── pybind_factor.cpp
└── utils/
    ├── runner.py             # AsyncRunner：后台独立 asyncio 事件循环
    ├── wrapper.py            # 装饰器/工具函数（singleton、LazyProperty、deprecated 等）
    └── util.pyx / util.pxd   # Cython 工具：fast_uuid4、_merge2DataFrame

tests/
└── test_mdapi.py             # pytest-asyncio 集成测试

scripts/
└── deploy.sh                 # 构建 wheel 并上传到内部 devpi

配置：
├── pyproject.toml            # Poetry 配置、依赖、构建脚本声明
├── setup.py                  # setuptools 入口，定义所有 Extension
├── build_ext.py              # Poetry 自定义构建脚本（被 pyproject.toml build 引用）
├── MANIFEST.in               # sdist 包含规则
├── pytest.ini               # pytest 配置
├── Dockerfile                # 基于 python:3.9 的容器镜像（注意与 Python 版本要求不一致）
└── README.md                 # 开发者笔记（中文）
```

## 4. 构建与安装

### 4.1 准备工作

```bash
# 安装 Poetry（如未安装）
curl -sSL https://install.python-poetry.org | python3 -

# 安装运行时与构建依赖（含 Cython、pybind11、cmake 等）
poetry install --no-root
# 或安装当前包（会触发扩展构建）
poetry install
```

### 4.2 本地开发构建（原地生成 .so）

```bash
poetry run python setup.py build_ext --inplace
```

### 4.3 打包 wheel

```bash
# 使用 Poetry（推荐）
poetry build --format wheel

# 或使用 build（仓库 README 中的笔记）
python -m build --wheel --no-isolation
```

> 注意：`.gitignore` 已忽略 `*.so` 和 `*.cpp`，但仓库中目前仍保留部分生成的 `.cpp` 文件（已被 Git 跟踪），重新构建时会覆盖。

### 4.4 构建产物

- wheel 中通过 `pyproject.toml` 的 `include` 配置打包 `bt_sdk/**/*.so` 与 `bt_sdk/**/*.pyx`。
- `MANIFEST.in` 控制源码分发包（sdist）内容，包含 `.so` / `.dylib` / `.pyd`。

## 5. 测试

### 5.1 运行测试

```bash
poetry run pytest
```

### 5.2 测试特性

- 使用 `pytest-asyncio`，默认 fixture loop scope 为 `function`（见 `pytest.ini`）。
- 测试文件 `tests/test_mdapi.py` 是**集成测试**，依赖一个真实运行的行情服务：
  - 默认地址：`127.0.0.1:50051`。
  - 可通过环境变量 `MD_ADDR` 覆盖，例如 `MD_ADDR=192.168.2.100:50051 poetry run pytest`。
- 每个测试用例会创建新的 `MdApi` 实例（`GetMdApi` 按地址做全局单例，`external_mdapi_context` 复用全局 `AsyncRunner`）。
- 订阅类测试会等待流 `on_completed` 后才继续，若服务端不结束流可能导致测试挂起。

## 6. 运行时架构

1. **入口层**：`bt_sdk.ctx`
   - `initialize_runner()` 启动一个后台守护线程运行独立 `asyncio` 事件循环（`AsyncRunner` 单例）。
   - `get_md_api(addr)` 调用 `GetMdApi(...)` 获取（或创建）`MdApi` 实例。
   - `external_mdapi_context(...)` 提供上下文管理器，自动 `start()` API 并复用全局 runner。

2. **API 层**：`bt_sdk.core.client.api.MdApi`
   - `start(loop)`：把 `MdApi` 绑定到某个事件循环，底层 `AsyncRpcClient.attach_loop(loop)`。
   - 同步方法（`get_instrument`、`get_factor`、`subscribe`）通过 `asyncio.run_coroutine_threadsafe` 把协程提交到 API 所属事件循环。
   - 异步方法（`get_instrument_async`、`get_factor_async`、`rpc_async`）直接 await。
   - `subscribe(...)` 返回一个 **RxPY Observable**，可对数据做 `pipe`/`subscribe`。

3. **客户端层**：`bt_sdk.core.client.async_client.AsyncRpcClient`
   - 内部持有 `bt_sdk.core.rpc.client.RpcClient`。
   - `wrap_protocol` 创建一个 `Observable`，其订阅时启动 `_stream_request`，将 gRPC 响应推入 `Subject`。
   - 会根据当前是否在目标事件循环中，决定用 `loop.create_task` 还是 `asyncio.run_coroutine_threadsafe` 启动流。

4. **RPC 层**：`bt_sdk.core.rpc.client.RpcClient`
   - 使用 `grpc.aio.insecure_channel` 创建长连接，配置了 keepalive、消息大小、HTTP2 窗口等参数。
   - 根据 `RpcTopic` 分发到不同的 gRPC stub stream call（`InstrumentCall`、`TickStreamCall`、`DailyStreamCall` 等）。
   - `rpc_callback` 把 payload bytes 通过 Arrow IPC 流式读取为 `pyarrow.Table`，并对价格/成交量字段按 topic 做缩放。

5. **工具层**：`bt_sdk.utils.util`
   - `fast_uuid4_bytes()`：调用 libuuid 生成 16 字节 UUID，避免 Python 端开销。
   - `_merge2DataFrame(batches, is_group=True)`：把 Arrow Table 列表按 `sid` metadata 分组合并成 `polars.DataFrame` 字典。

6. **复权因子层**：`bt_sdk.core.factor` + `bt_sdk.core.lib.adj_factor`
   - Python 端把 Polars DataFrame 转为 C++ struct 列表，调用 C++ 计算累计复权因子。
   - 再用 Polars 的 `join_asof` 把因子应用到 open/high/low/close/volume。

## 7. 代码风格与约定

- **文件头**：Python 脚本常见 `#! /usr/bin/env python3` 和 `# -*- coding: utf-8 -*-`。
- **Cython 编译指令**：
  - `language_level=3`
  - 性能模块常关闭边界检查：`boundscheck=False`、`wraparound=False`
  - 使用 `cdef` / `cpdef` 减少 Python 调用开销，公共接口在 `.pxd` 中声明。
- **C/C++ 标准**：
  - Cython 扩展按 `-O3 -std=c++11` 编译。
  - pybind11 扩展按 `-std=c++17 -O3` 编译。
- **异步编程约定**（来自 README 笔记与代码）：
  - 在单线程 `asyncio` 环境（如 Ray Async Actor）中**禁止**调用 `future.result()`，应使用 `await` 或 `asyncio.wrap_future`。
  - 跨事件循环提交协程统一使用 `asyncio.run_coroutine_threadsafe` + `asyncio.wrap_future`。
  - 连接关闭时优先 `writer.close()`，不强制 `wait_closed()` 以避免网络差时阻塞恢复流程。
- **中文注释**：模块与关键实现处注释多为中文，新增代码建议保持中文注释风格。

## 8. 部署

- 脚本：`scripts/deploy.sh`
- 流程：
  1. 检查/安装 Poetry。
  2. 切到 devpi 服务器：`http://192.168.2.100:3141/`。
  3. 使用 `bt_sdk/dev` channel，用户名 `bt_sdk`，密码硬编码在脚本中。
  4. 清理 `dist/`，执行 `poetry build --format=wheel`，再 `devpi upload dist/*`。
- **安全提示**：该脚本包含明文密码与内部地址，请勿在公开环境使用或提交到公共仓库。

## 9. 安全注意事项

- gRPC 通道使用 `grpc.aio.insecure_channel`，**无 TLS 加密**，仅适用于可信内网。
- `scripts/deploy.sh` 包含硬编码的 devpi 密码和内部 IP，部署前务必审查环境。
- Cython 扩展关闭了数组边界检查（`boundscheck=False`、`wraparound=False`），输入数据必须保证合法，否则可能触发段错误。
- `.gitignore` 已忽略 `*.so`、`*.cpp`、`.env`、`.venv` 等敏感/生成文件；注意不要把生产配置文件或凭据加入版本控制。

## 10. 常见陷阱

- **gRPC fork 支持**：`bt_sdk/core/rpc/client.pyx` 已设置 `os.environ['GRPC_ENABLE_FORK_SUPPORT']='0'`，用于避免 macOS / spawn 模式下的 fork 问题。
- **事件循环归属**：`MdApi` 必须 `start(loop)` 绑定到有效事件循环；跨线程调用同步 API 时会通过 `run_coroutine_threadsafe` 转发。
- **RxPY Observable 的 `await`**：在 RxPY 中，`await observable` 会自动订阅并等待流结束，返回最后一个元素。需要返回 Observable 对象本身时应避免直接 `await`。
- **Dockerfile 版本不一致**：`Dockerfile` 基于 `python:3.9`，而 `pyproject.toml` 要求 `>=3.11`，构建/运行前请统一 Python 版本。

---