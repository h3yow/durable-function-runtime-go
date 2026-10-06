# durable-function-runtime-go

单机可运行的**持久化函数编排组件**。以本地目录承载全部执行状态，保证在
**并发投递、进程重启、定义变更**三类干扰下，执行结论一致、终态可区分、过程可解释。

- 每个步骤的输入 / 输出 / 执行状态 / 尝试次数均落盘；
- 进程重启后从首个未完成步骤继续，已完成步骤绝不重复执行；
- 同一幂等键的并发重复提交收敛到同一执行；同键不同请求被明确拒绝；
- 瞬时失败按有上限指数退避重试；永久失败、超时、取消立即进入可区分终态；
- 可声明的**失败补偿（回滚）**：正向流程无法走完时，已成功且声明了补偿的
  步骤按相反顺序自动补偿，补偿进度落盘、可断点续补；
- 进行中的执行始终按**启动时钉住的定义版本**执行；未知 / 不兼容版本明确拒绝，
  绝不静默按新语义重放。

## 快速开始

```go
reg := durable.NewRegistry()
_ = reg.Register(durable.Definition{
    Name: "order-pipeline", Version: "1.0.0",
    Steps: []durable.Step{
        {Name: "validate", Fn: validateFn},
        {Name: "charge",   Fn: chargeFn, Compensate: refundFn}, // 声明补偿（回滚）动作
        {Name: "ship", Timeout: 5 * time.Second, Fn: shipFn},
    },
})

rt, _ := durable.New(ctx, durable.Config{
    Dir: "./state", MaxConcurrent: 8, Retry: durable.DefaultRetryPolicy(),
    CompRetry: durable.DefaultRetryPolicy(), // 补偿动作独立的重试策略
}, reg)
defer rt.Close()

res, err := rt.Start(ctx, "order-pipeline", "1.0.0", "order-42",
    json.RawMessage(`{"amount":99}`))
// res.ExecutionID 由幂等键确定性派生；重复提交返回同一 ID（NewExecution=false）

exec, _ := rt.GetExecution(ctx, res.ExecutionID) // 持久化快照
events, _ := rt.Events(ctx, res.ExecutionID)     // 追加式可解释轨迹
```

可执行演示：`go run ./example -dir ./state-demo`（并发投递 8 次，收敛为 1 个执行）。

## 本地存储布局

所有状态都在 `Config.Dir` 下（默认无外部依赖）：

```
state/
├── lock                       进程级 flock，同一时刻只允许一个 Runtime 打开该目录
├── exec/<execID>.json         执行快照（含每步状态、rev 版本号，原子写 + CAS）
├── claims/<sha256(key)>.json  幂等键占用（临时文件 + link 原子占位）
└── events/<execID>.log        追加事件日志，每行一个 JSON，seq 严格递增
```

- 执行快照采用「临时文件 + fsync + rename」原子落盘，写入带期望 `rev` 的
  CAS，防止并发覆盖；
- 幂等占用先写临时文件，再用 `link(2)` 原子占位，读者永远看不到半成品；
- 事件序号在「读末序号 → 分配 → 追加」互斥区内完成，重启后仍单调连续。

## 执行状态机

### 执行级状态（`ExecStatus`）

```
            Start
              │
              ▼
        ┌──────────┐  定义未知/版本不兼容   ┌──────────┐
        │ pending  │ ───────────────────▶ │ rejected │ (终态)
        └────┬─────┘                       └──────────┘
             │ 首个步骤开始
             ▼
        ┌──────────┐
        │ running  │◀──────── 步骤成功后仍有后续步骤（回到 pending 队列）
        └────┬─────┘
   ┌─────────┼──────────────┬─────────────────┐
   ▼         │              ▼                 ▼
succeeded    │          cancelled         timed_out
 (终态)      │ 永久失败/重试上限/超时/取消，且存在已声明补偿的成功步骤
             ▼
      ┌──────────────┐   全部补偿完成    failed / cancelled / timed_out
      │ compensating │ ───────────────▶ （落入触发时记录的原始终态）
      └──────┬───────┘
             │ 补偿重试上限耗尽/永久失败/补偿超时
             ▼
      compensation_failed (终态，与正向失败可区分)

      任意阶段定义版本不可用 → rejected（原始失败原因保留）
```

- `failed` 用 `Failure.Code` 区分 `StepPermanent`（永久失败）与
  `RetryLimitExceeded`（瞬时失败重试上限耗尽）；
- `timed_out` 对应 `StepTimeout`，**超时不可重试**，立即终态；
- `cancelled` 对应 `ExecutionCancelled`；
- `compensating` 是**非终态**：正向流程已收尾，正在逆序补偿；
- `compensation_failed` 是补偿自身失败的终态，与正向失败可区分；
- `rejected` 用 `Failure.Code` 区分 `DefUnknown` / `DefVersionIncompatible` /
  `DefVersionMissing`。

### 步骤级状态（`StepStatus`）

```
pending ──▶ running ──▶ succeeded（终态，重启后绝不重放）
               │
               ├─ 瞬时失败且未达上限 ──▶ retrying（持久化 not_before，退避后回 running）
               ├─ 瞬时失败且达到上限 ──▶ failed（执行 failed / RetryLimitExceeded）
               ├─ 永久失败          ──▶ failed（执行 failed / StepPermanent）
               ├─ 步骤超时          ──▶ timed_out（执行 timed_out / StepTimeout）
               └─ 取消 / 进程崩溃    ──▶ cancelled / 复位为 pending（见下）
```

**重启语义**：崩溃时处于 `running` 的步骤，其副作用未得到确认，因此在重启恢复时
复位为 `pending` 并记录 `running_step_reset` 事件后安全重试；已 `succeeded` 的
步骤输入/输出已持久化，恢复后直接跳过，不重复产生副作用。

## 幂等语义

- 执行 ID 由幂等键确定性派生：`exec-<sha256(key) 前 16 位>`；
- 建档依赖文件系统原子占位，因此**同一进程并发**与**跨进程同时启动**都只有一个
  占用创建成功，其余调用读取既有占用：
  - 请求指纹（`def + version + 规范化输入` 的 SHA-256）**相同** → 收敛：
    返回同一 `ExecutionID`，`IdempotencyError.Duplicate` 不触发错误，调用方
    轮询/等待同一结果即可；
  - 请求指纹**不同** → 返回 `*IdempotencyError{Code: IdempotencyConflict,
    Duplicate:false}`，明确拒绝并在错误中给出已绑定的执行 ID，原因可与
    「重复投递」区分；
- 若进程在「占用已建、执行尚未落盘」的窗口崩溃，恢复时按占用中的输入确定性
  补建 `pending` 执行，不会丢请求，也不会建出第二个执行。

## 失败分类与重试

- 步骤返回 `*durable.PermanentError`（可用 `errors.As` 解包）→ **永久失败**，
  立即 `failed/StepPermanent`，不重试；
- 其它错误默认归类为**瞬时失败**，按
  `min(InitialBackoff × Multiplier^(attempts-1), MaxBackoff)` 退避，
  总尝试次数（含首次）以 `RetryPolicy.MaxAttempts` 为硬上限；
- 达到上限 → `failed/RetryLimitExceeded`；
- 步骤级 `Timeout` 到期 → `timed_out/StepTimeout`，不进入重试链路；
- 退避窗口 `not_before` 已持久化：重启后仍遵守退避时刻；退避等待期间
  **不占用并发额度**；
- 取消意图 (`CancelRequested`) 持久化优先于一切调度：无论步骤返回 nil、错误
  还是 ctx 取消，驱动在落结论前都会重读磁盘上的取消标记，立即进入
  `cancelled`，不会把取消误判为瞬时错误去重试；
- 并发槽位是中断式信号量：等槽位期间收到取消/关闭立即返回；每次获取必有一次
  配对释放，终态后不存在悬挂执行、重复调度或被长期占用的额度（可用
  `rt.Inflight()` 观测，测试中已断言归零）。

## 失败补偿（回滚）

每个步骤可在定义里声明 `Compensate` 补偿动作（及独立的 `CompensateTimeout`）。

**触发条件**：当一次执行因永久失败、重试上限耗尽、步骤超时或被取消而无法
正常走完时，运行时自动进入 `compensating`；若没有任何「已成功且声明了补偿」
的步骤，则直接落入原始终态，不产生补偿开销。

**执行顺序**：按步骤定义的**相反顺序**（从后往前）依次补偿；未声明补偿的
步骤记为 `skipped` 并跳过，不影响其它步骤的补偿。补偿输入为该步骤成功时的
输出（无输出时回退为其输入）。

**持久化与恢复**：

- 补偿计划、每个补偿动作的状态/尝试次数/失败原因都落在执行快照的
  `Compensation` 字段里，事件日志记录 `compensation_started`、
  `comp_step_started/succeeded/retrying/failed/skipped`、
  `compensation_finished/failed`；
- 进程重启后从**尚未补偿完**的步骤继续；已 `succeeded` 的补偿绝不重复执行；
- 崩溃时处于 `running` 的补偿动作视为副作用未确认，恢复时复位为 `pending`
  重新补偿，不会被当成已完成；
- 同一执行的补偿被重复触发（取消与超时同时发生、重启后再次恢复）时收敛为
  同一份补偿计划：只建一次、每步只补一次、不重复扣尝试次数。

**补偿自身的失败**：

- 补偿动作使用 `Config.CompRetry` 指定的**独立**退避重试策略（与正向
  `Retry` 互不影响），同样有次数上限；
- 重试上限耗尽、永久失败或补偿超时 → 执行进入 `compensation_failed` 终态，
  原因码 `CompensationFailed` / `CompensationTimeout` 记在
  `Compensation.Failure`；触发补偿的**原始原因**始终保留在
  `Execution.Failure`（及 `Compensation.Trigger`/`TriggerStep`），不被覆盖；
- `CompensateTimeout` 到期后，即使补偿函数不理会取消信号，也会立即给出
  超时结论并**释放并发额度**（遗留 goroutine 的结果被丢弃）；
- 补偿退避等待期间同样不占用并发额度；执行彻底结束（含补偿结束）后无悬挂
  调度、无长期占用额度（`rt.Inflight()` 归零）。

**版本语义**：补偿按启动时钉住的定义版本执行。补偿期间若该版本已不可用，
执行进入 `rejected`，拒绝原因（`DefUnknown`/`DefVersionMissing` 等）记入
`Compensation.Failure`，与补偿执行失败（`CompensationFailed`）可区分，
**绝不**悄悄换成新版本去补偿。

## 版本兼容规则

定义以 `名称 + 语义化版本 (MAJOR.MINOR.PATCH)` 标识。执行在启动时把
`defName@defVersion` **钉住**并写入快照与每条事件。

| 恢复/推进时的注册情况 | 判定 (`VersionDecision`) | 终态原因码 | 处理 |
| --- | --- | --- | --- |
| 精确版本仍存在 | `accept_compatible` | — | 按钉住版本的步骤语义继续 |
| 名称已未知 | `def_unknown` | `DefUnknown` | 明确拒绝（启动与恢复都拒绝） |
| 存在同名定义，但主版本不同 | `version_incompatible` | `DefVersionIncompatible` | 明确拒绝，绝不按新版本重放 |
| 主版本相同但精确版本缺失 | `version_missing` | `DefVersionMissing` | 明确拒绝，不猜测兼容语义 |

- 兼容承诺只认「精确版本仍可用」：即使同主版本的新补丁版存在，也不自动迁移
  进行中的执行，避免错乱重放；
- 运行期调用 `Registry.Unregister` 移除定义后，进行中执行的下一次推进同样
  立即进入 `rejected`；
- **已终态**执行不再做版本校验，历史结论原样保留；
- 同名同版本重复注册时步骤签名（含补偿声明与超时设置）必须一致，否则注册
  即被拒绝；
- 运行期 `Register`/`Unregister` 与并发读取（`CheckVersion`/`Resolve`）通过
  读写锁隔离，可安全并行：注册新版本不会影响进行中执行钉住的语义，竞态
  检测下无数据竞争。

## 可解释性（事件轨迹）

每次状态迁移都追加一条事件（`rt.Events(id)`），包含：

- `seq`（严格递增）、`time`、`type`；
- `idemp_key`、`def_name`、`def_version`、`version_decision`；
- 步骤名、`attempt`、`max_attempts`、`backoff_ms`、`not_before`；
- 失败分类 `failure{class, code, retriable}` 与拒绝/失败 `reason`。

单测日志会显式打印幂等键、重试依据（attempt/maxAttempts/backoff）、
版本判定（versionDecision/reason），例如：

```
[durable] idempotency DEDUP idempKey="order-42" -> execution exec-3bf8... (converged)
[durable] execution=... step=charge RETRY attempt=1/3 backoff=50 retryReason=StepTransient
[durable] execution=... idempKey="order-42" COMPENSATING trigger=StepPermanent triggerStep="ship" finalStatus=failed
[durable] execution=... idempKey="order-42" COMP step=charge attempt=1/3 trigger=StepPermanent
[durable] recovery: REJECT execution=... versionDecision=version_incompatible reason=DefVersionIncompatible
```

## 验证方法

```bash
go test ./... -count=1 -v          # 全部场景（日志含幂等键/重试依据/补偿依据/版本判定）
go test ./... -race                # 竞态检测（含并发注册定义与补偿场景）
go test -run TestCompensation -v ./...  # 只看补偿（回滚）相关场景
go run ./example -dir ./state-demo # 端到端演示（可重复运行验证幂等与恢复）
```

测试覆盖（23 个场景）：

| 分类 | 测试 |
| --- | --- |
| 正常持久化 | `TestHappyPathStepsPersisted` |
| 并发重复投递收敛 | `TestConcurrentDuplicateSubmission` |
| 同键不同请求拒绝 | `TestIdempotencyConflict` |
| 重启恢复 / 已完成步骤不重放 | `TestRestartResume` |
| 重试上限 / 无悬挂调度与额度 | `TestRetryLimitExceeded` |
| 永久失败立即终态 | `TestPermanentFailureNoRetry` |
| 超时独立终态 | `TestStepTimeout` |
| 退避中取消 / 运行中取消 | `TestCancelDuringBackoff`, `TestCancelWhileStepRunning` |
| 定义未知 / 主版本不兼容（启动） | `TestStartRejectsUnknownDefinition`, `TestStartRejectsIncompatibleMajor` |
| 定义移除 / 主版本升级 / 同主版本版本缺失（恢复） | `TestRecoveryDefinitionRemoved`, `TestRecoveryInFlightMajorIncompatible`, `TestRecoverySameMajorVersionMissing` |
| 运行期注销定义 | `TestInFlightDefinitionUnregisteredWhileRunning` |
| 逆序补偿 / 未声明补偿跳过 | `TestCompensationReverseOrder`, `TestCompensationSkipsUndeclared` |
| 补偿重试上限 / 原始原因保留 | `TestCompensationRetryExhausted` |
| 补偿中重启恢复 / 已补步骤不重复 | `TestCompensationRestartRecovery` |
| 取消与超时同时触发补偿收敛 | `TestCompensationCancelAndTimeoutConverge` |
| 补偿期间定义版本不可用 | `TestCompensationDefinitionUnavailable` |
| 运行期注册新版本与执行并发推进 | `TestConcurrentRegisterWhileExecuting` |
| 补偿超时释放额度 | `TestCompensationTimeoutReleasesSlot` |

## 设计边界

- 单进程语义：状态目录由 flock 限定同一时刻只有一个 Runtime；多实例部署需要
  替换 `Store`（接口已预留 CAS、互斥建档、追加事件三个基本操作）；
- 步骤函数应尽量做到「副作用可安全重试」：崩溃时运行中的步骤可能在重启后再
  执行一次，组件保证的是*已确认成功*的步骤不重放；
- 调度 tick 为 250ms 轮询 + 事件唤醒，目标是正确性与可解释性而非极限吞吐。
