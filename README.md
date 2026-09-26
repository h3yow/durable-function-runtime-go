# durable-function-runtime-go

可在单机运行的持久化函数编排组件。执行状态由本地 JSON 文件承载（tmp+rename 原子写盘），
在并发投递、进程重启与定义变更下保证执行结论一致且可解释。

## 快速开始

```go
store, _ := durable.NewFileStore("data/store.json")
reg := durable.NewRegistry()
reg.Register(&durable.Definition{
    Name: "order", Version: "v1",
    Steps: []durable.StepDef{
        {Name: "charge", Func: chargeFn,
         Timeout: 5 * time.Second,
         Retry: durable.RetryPolicy{MaxAttempts: 3, BaseDelay: 100 * time.Millisecond, MaxDelay: 2 * time.Second}},
        {Name: "notify", Func: notifyFn},
    },
})
rt := durable.NewRuntime(store, reg, durable.Options{Workers: 8})
go rt.Run(ctx)

exec, created, err := rt.Submit(ctx, "order", "v1", "idem-key-123", []byte(`{"orderId":"A1"}`))
```

进程重启后用同一存储文件重新构造 `Runtime` 并调用 `Run`，即从未完成的步骤继续执行。

## 执行状态机

执行（Execution）与步骤（StepRecord）共用同一状态机：

```
Pending ──> Running ──> Succeeded            （正常路径）
   │           │
   │           ├──> Failed    （永久失败 / 重试耗尽 / 版本不兼容 / 定义缺失）
   │           ├──> TimedOut  （单次尝试超时）
   │           └──> Canceled  （主动取消）
   └── 退避中：Running 尝试失败后回到 Pending，并设置 NextRetryAt
```

- `Pending/Running` 为非终态；`Succeeded/Failed/Canceled/TimedOut` 为终态（`Status.Terminal()`）。
- 每次状态迁移都通过乐观并发（`Version` 递增的 CompareAndSwap）持久化，崩溃时不会丢迁移。
- 步骤按序执行：首步输入为执行输入，后续步骤输入为上一步**已持久化**的输出；
  执行输出为末步骤输出。
- 终态后不再被调度，执行 goroutine 退出即释放并发额度（信号量），
  不会残留悬挂执行或长期占用额度。

## 幂等语义

- 提交必须携带幂等键。存储层以幂等键为唯一约束做 `CreateIfAbsent`。
- 同一幂等键的并发重复提交：
  - 定义名、定义版本、输入哈希（SHA-256）三者一致 → **收敛**到同一执行，返回 `created=false`；
  - 任一不一致 → **拒绝**，返回 `ErrIdempotencyConflict`（可 `errors.Is` 判定）。
- 已完成步骤的输出已持久化，重启恢复时从 `NextStep` 继续，
  已完成步骤**不会重复执行、不重复产生副作用**；进行中的步骤按至少一次语义重试。

## 失败分类与重试

| 分类 | 触发 | 行为 | 终态 |
|---|---|---|---|
| `Transient` | 步骤返回普通 error | 指数退避重试：`BaseDelay * 2^(n-1)`，封顶 `MaxDelay`，上限 `MaxAttempts` | 未达上限不进终态 |
| `RetryExhausted` | 瞬时失败达到 `MaxAttempts` | 立即终态 | `Failed` |
| `Permanent` | 步骤返回 `durable.Permanent(err)` | 不重试，立即终态 | `Failed` |
| `Timeout` | 单次尝试超过 `StepDef.Timeout` | 不重试，立即终态 | `TimedOut` |
| `Canceled` | `Runtime.Cancel` | 立即终态，唤醒等待中的执行 | `Canceled` |
| `VersionIncompatible` | 恢复时锁定版本已不在注册表 | 立即终态 | `Failed` |
| `DefinitionUnknown` | 恢复时定义名不存在 | 立即终态 | `Failed` |

失败类别同时记录在执行的 `FailureKind` 与步骤记录的 `FailureKind` 上，
`FailureReason`/`LastError` 给出人读原因，各终态与拒绝原因均可区分。

## 版本兼容规则

- 定义按 `(Name, Version)` 注册，版本不可变：同名同版本重复注册时步骤结构必须一致，否则 panic。
- 执行在提交时**锁定** `DefinitionVersion`，进行中与重启恢复后都按启动时的语义继续，
  注册新版本不影响进行中的执行。
- 提交未知定义或不存在的版本：分别返回 `ErrDefinitionUnknown` / `ErrVersionIncompatible`。
- 恢复时锁定版本已不存在：执行进入 `Failed` + `VersionIncompatible`，
  **绝不**静默按新版本语义继续或产生错乱重放。

## 并发与恢复

- `Options.Workers` 为全局并发额度（信号量）。额度占满时执行留在队列，下轮再调度；
  退避中的执行让出额度，到期（`NextRetryAt`）后重新竞争。
- `inflight` 集合保证同一执行不会被重复调度。
- `Cancel` 先写终态再唤醒执行 goroutine，保证取消后无悬挂、无重复调度。
- 运行时停机（ctx 取消）时，进行中的步骤保持非终态，重启后从当前步骤恢复。

## 验证方法

```bash
go build ./...
go vet ./...
go test ./...          # 全部场景
go test -race ./...    # 竞态检测
go test -v ./...       # 查看幂等键、重试依据、版本判定日志
```

测试覆盖（`durable/runtime_test.go`）：

| 测试 | 场景 |
|---|---|
| `TestConcurrentIdempotentSubmit` | 32 路并发同键投递收敛为 1 个执行；同键不同内容返回 `ErrIdempotencyConflict` |
| `TestRestartRecovery` | 模拟进程退出后换新运行时恢复：已完成步骤副作用计数不增加，输出经持久化传递 |
| `TestRetryExhausted` | 瞬时失败按 1/2/4ms 退避重试，3 次上限后 `Failed`+`RetryExhausted` |
| `TestPermanentFailure` | `Permanent` 错误 1 次即终态，不消耗重试 |
| `TestStepTimeout` | 超时进入 `TimedOut`+`Timeout` |
| `TestVersionIncompatibleOnResume` | 恢复时旧版本缺失 → `Failed`+`VersionIncompatible`，版本仍锁定 v1 |
| `TestCancelReleasesSlot` | 取消后 `Canceled`+`Canceled`，单并发额度被释放，后续执行可完成 |
| `TestFailureReasonsDistinguishable` | 各失败类别与拒绝错误互不相同 |

单测日志（`go test -v`）中每个关键事件都打印幂等键（`idem=`）、
重试依据（`attempt=n/max backoff=...`）与版本判定（`def=name@version kind=...`）。
