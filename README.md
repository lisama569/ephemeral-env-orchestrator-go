# ephemeral-env-orchestrator-go

本地可运行的临时环境编排器：用进程内资源模拟环境实例，提供申请、就绪、
续租、释放、租约到期（单个与批量）与准备失败的完整生命周期管理，并保证
并发申请与配额约束下状态转换可预期、可解释。

核心代码位于 `internal/orchestrator/`，不依赖任何外部服务。

## 生命周期状态机

```
                Acquire(id)
                    │
                    ▼
            ┌───────────────┐   ReportReady    ┌────────┐
            │ PROVISIONING  │ ───────────────▶ │ READY  │
            └───────────────┘                  └────────┘
                    │                              │
        ReportFailed│                              │Release
                    ▼                              ▼
              ┌──────────┐   租约到期(Expire)  ┌──────────┐
              │  FAILED  │ ◀── 仅非终态 ──▶   │ RELEASED │
              └──────────┘                    └──────────┘
                    ▲
                    │ 租约到期(Expire)
              ┌──────────┐
              │ EXPIRED  │（由 PROVISIONING/READY 到期转入）
              └──────────┘
```

- **非终态**：`PROVISIONING`、`READY`，占用配额。
- **终态**：`RELEASED`（显式释放）、`EXPIRED`（租约到期）、`FAILED`（准备失败）。
  终态不可逆，且不再占用配额。
- 三条回收路径（显式释放 / 租约到期 / 准备失败）都会归还恰好一份配额，
  最终都进入终态，不留下悬挂实例或过期状态。
- `Renew` 可作用于 `PROVISIONING` / `READY`：仅后移租约到期时间、不改状态、
  不占额外容量；终态不可续租复活，已实际过期的租约不可续回。
- `SweepExpired` 是 `Expire` 的批量形态：一次扫描把所有已到期的非终态实例
  统一转为 `EXPIRED`，语义与逐个 `Expire` 一致。

## 配额与租约规则

- **配额（Quota）**：同一时刻非终态实例数上限。`Acquire` 在 `live >= quota`
  时返回 `ErrQuotaExceeded`，且**不占用任何配额**、不留下实例记录（包括终态
  后同标识重新申请因容量不足被拒绝时，上一代终态记录也原地保留、不归档不新建）。
- **申请入参校验**：空标识返回 `ErrEmptyID`，租约时长为 0 或负值返回
  `ErrInvalidLease`。两类非法入参优先于容量判定，**不创建记录、不占用任何
  配额**，并与配额不足、标识不存在、实例已结束等失败明确区分。
- **幂等申请**：同一 ID 仍有活跃（非终态）实例时重复 `Acquire` 返回同一份
  实例快照（状态、创建时间一致），结果中 `Reused=true`；新建时 `Reused=false`，
  调用方据此区分本次是复用还是新建。幂等命中不重复创建、不重复占用配额，
  本次传入的租约时长被忽略。
- **同标识重新申请与历史保留**：同一 ID 的上一代实例进入终态后再次
  `Acquire`，会创建**全新一代**实例——新的创建时间、新的租约、从
  `PROVISIONING` 开始，并按新实例占用一份容量；容量不足时按原规则拒绝。
  上一代实例被归档到历史中，可用 `History(id)` 按代数从旧到新追溯；`Get(id)`
  始终返回当前一代。每个标识仅保留一条"当前一代"记录，因此同一时刻它
  至多占用一份容量。
- **租约（按申请指定）**：每次 `Acquire(id, ttl)` 都必须显式传入正的租约
  时长，实例记录 `ExpiresAt = CreatedAt + ttl`；同一标识重新申请时可以使用
  与上一代不同的时长。`Expire(id)` 仅在当前时间到达 `ExpiresAt` 后才把
  实例转为 `EXPIRED`；未到期时为无操作，返回当前状态。
- **续租（Renew）**：`Renew(id, extend)` 在租约到期前把 `ExpiresAt` 后移
  `extend`，可多次调用，每次基于当前到期时间继续顺延。续租不改变状态
  （`PROVISIONING` / `READY` 均可续），**不额外占用容量**；到期回收一律以
  续租后的最新 `ExpiresAt` 为准。
  - 标识从未申请：`ErrInstanceNotFound`；
  - 实例已走完生命周期：`ErrInstanceTerminal`，**终态不会被续租复活**；
  - 租约已实际到期（`now >= ExpiresAt`，即使还没被扫描回收）：
    `ErrLeaseExpired`，**过期后不允许再续回来**；
  - 空标识 / 非正延长时长：`ErrEmptyID` / `ErrInvalidLease`。
- **批量到期清理（SweepExpired）**：一次扫描回收所有 `now >= ExpiresAt`
  的活跃实例为 `EXPIRED`，归还各自容量，返回本次回收的实例快照（按标识
  字典序，便于核对）；没有到期实例时返回空切片。扫描与申请、就绪、释放、
  续租共用同一把互斥锁，单个实例的"到期判定 + 回收"在同一临界区内完成，
  因此并发下**既不会重复回收（已终态直接跳过），也不会漏掉扫描时刻已到期
  的实例**。
- **容量与状态总览（Stats）**：返回某一时刻的自洽快照：`Quota`（容量上限）、
  `Live`（非终态、占用容量的实例数）、`Available = Quota - Live`（剩余
  容量）、`ByState`（五个状态各自的当前一代实例数）。非终态两状态数量之和
  恒等于 `Live`；多个调用方并发查询时，每份快照都在同一临界区内采样，
  字段之间互相吻合，不会读到半个状态。
- **重复回收**：对已终态实例执行 `Release` / `Expire` / `ReportFailed`
  返回原实例快照与 `ErrInstanceTerminal`，**不重复扣减配额**。
- **并发守恒**：所有状态与配额计数在同一把互斥锁下变更；并发申请下成功数
  恰好等于配额、拒绝数可区分，实例之间互不覆盖。

### 可区分的失败原因

| 错误 | 含义 |
|---|---|
| `ErrEmptyID` | 申请/续租传入空标识，非法入参 |
| `ErrInvalidLease` | 租约时长或续租时长为 0/负值，非法入参 |
| `ErrQuotaExceeded` | 配额已满，申请被拒绝 |
| `ErrInstanceNotFound` | 指定标识的实例不存在（从未申请） |
| `ErrInstanceTerminal` | 实例已处于终态，操作被拒绝（终态不复活） |
| `ErrLeaseExpired` | 租约已实际到期，不能再续租 |

均可用 `errors.Is` 判定。

## API 概览

```go
orch := orchestrator.New(orchestrator.Config{Quota: 8})

res, err := orch.Acquire("env-1", time.Minute) // 申请；res.Reused 区分新建/复用
inst := res.Instance
inst, err = orch.Renew("env-1", 2*time.Minute) // 到期前续租，可多次，不占容量
inst, err = orch.ReportReady("env-1")          // PROVISIONING -> READY
inst, err = orch.ReportFailed("env-1")         // -> FAILED，归还配额
inst, err = orch.Release("env-1")              // -> RELEASED，归还配额
inst, err = orch.Expire("env-1")               // 到期后 -> EXPIRED，归还配额
swept := orch.SweepExpired()                   // 批量回收所有到期实例
stats := orch.Stats()                          // 容量与状态总览快照
inst, err = orch.Get("env-1")                  // 查询当前一代快照
history, err := orch.History("env-1")          // 追溯同标识的历代终态记录
n := orch.LiveCount()                          // 当前占用配额的实例数
_ = inst
```

`Config.Now` 可注入自定义时钟，便于在测试中控制租约到期。

## 本地验证

```bash
go build ./...                 # 编译
go test ./... -race -count=1   # 运行全部测试（含竞态检测）
go test ./internal/orchestrator/ -v -count=1   # 查看判定日志
```

测试覆盖（含原有行为与本轮新增能力）：

- 原有：并发申请守恒、配额超限拒绝、同标识幂等申请、并发重复申请同一标识、
  显式释放与重复释放、租约未到期/到期/重复到期回收、准备失败、未知实例操作；
- 续租：到期前续租后按新到期时间判定回收、READY 状态多次续租、续租不占容量、
  不存在/已终态/已实际过期三类失败可区分（终态不复活、过期不续回）、非法入参拒绝；
- 同标识复用：终态后同标识重新申请得到全新一代（新时间/新租约/从 PROVISIONING
  开始）、旧记录经 `History` 可追溯、重新申请按新实例占容量且容量不足按原规则
  拒绝且不留痕、活跃实例同标识仍幂等并通过 `Reused` 区分、空标识与非正租约
  拒绝且不占容量；
- 批量清理与总览：到期实例一次性全部回收且不重不漏、未到期与已终态不被波及、
  扫描与申请/续租/释放并发下容量守恒、`Stats` 各字段自洽、并发查询结果一致。

单测日志沿用 `request_id=<标识> basis=<判定依据> final_state=<最终状态>
err=<错误>` 格式，便于核对每次判定的依据与结论。
