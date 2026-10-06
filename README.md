# ephemeral-env-orchestrator-go

本地可运行的临时环境编排器：用进程内资源模拟环境实例，提供申请、就绪、
释放、续租、租约到期（单个与批量）与准备失败的完整生命周期管理，并保证
并发申请与配额约束下状态转换可预期、可解释。

核心代码位于 `internal/orchestrator/`，不依赖任何外部服务。

## 生命周期状态机

```
                Acquire(id, lease)
                    │
                    ▼
            ┌───────────────┐   ReportReady    ┌────────┐
            │ PROVISIONING  │ ───────────────▶ │ READY  │
            └───────────────┘                  └────────┘
                    │                              │
        ReportFailed│        Renew(续租，顺延 ExpiresAt)
                    ▼            ▲               │Release
              ┌──────────┐       └───────────────┤
              │  FAILED  │                       ▼
              └──────────┘                  ┌──────────┐
                    ▲          租约到期      │ RELEASED │
                    │  (Expire / SweepExpired)└──────────┘
              ┌──────────┐
              │ EXPIRED  │（由 PROVISIONING/READY 到期转入）
              └──────────┘
```

- **非终态**：`PROVISIONING`、`READY`，占用配额，可续租。
- **终态**：`RELEASED`（显式释放）、`EXPIRED`（租约到期）、`FAILED`（准备失败）。
  终态不可逆，且不再占用配额。
- 三条回收路径（显式释放 / 租约到期 / 准备失败）都会归还恰好一份配额，
  最终都进入终态，不留下悬挂实例或过期状态。

## 申请、续租与标识复用规则

- **配额（Quota）**：同一时刻非终态实例数上限。`Acquire` 在 `live >= quota`
  时返回 `ErrQuotaExceeded`，且**不占用任何配额**、不留下实例记录。
- **参数校验**：`Acquire(id, lease)` 要求标识非空、租约时长为正，否则分别
  返回 `ErrInvalidID` / `ErrInvalidLease`，同样不占用任何配额、不留下记录。
- **幂等与复用**：同一标识当前仍有活跃（非终态）实例时，`Acquire` 返回该
  实例且 `reused=true`，不重复占用配额；调用方可凭 `reused` 区分本次是
  复用已有活跃实例还是新建。
- **终态后同标识重新申请**：上一代实例走完生命周期后，同标识再次
  `Acquire` 会得到一个**全新实例**（新的创建时间与租约、从 PROVISIONING
  开始），`reused=false`，按新实例占用一份配额（不足时按配额规则拒绝）。
  旧代记录保留，可通过 `History(id)` 按代追溯（最旧在前，末位为当前代），
  `Get(id)` 始终返回当前代。
- **续租（Renew）**：`Renew(id, extension)` 为 PROVISIONING / READY 实例
  把到期时间顺延 `extension`（必须为正），可续多次、时长累加；续租本身
  **不额外占用配额**。租约一旦真正过期（当前时间已到达 `ExpiresAt`）即
  不可再续，返回 `ErrLeaseExpired`；已终态实例返回 `ErrInstanceTerminal`，
  不会被重新变回活跃。回收判定一律以最新的 `ExpiresAt` 为准。
- **重复回收**：对已终态实例执行 `Release` / `Expire` / `ReportFailed`
  返回原实例快照与 `ErrInstanceTerminal`，**不重复扣减配额**。
- **并发守恒**：所有状态与配额计数在同一把互斥锁下变更；并发申请下成功数
  恰好等于配额、拒绝数可区分，实例之间互不覆盖。

## 批量到期清理与容量总览

- **批量清理**：`SweepExpired()` 一次性扫描所有实例，把租约已到期的非终态
  实例全部回收为 `EXPIRED` 并归还配额，返回本次新回收的实例快照；未到期
  与已终态的实例不受影响。它与 `Acquire` / `ReportReady` / `Release` 等
  操作共用同一把锁，并发进行时每个实例只被回收一次，既不重复扣减也不漏收。
- **容量总览**：`Stats()` 返回某一时刻的配额使用情况与各状态分布：
  - `Quota` / `Live` / `Free`：配额上限、已占用（非终态实例数）、剩余；
  - `ByState`：各生命周期状态下的当前实例数（每标识只计当前代）；
  - `Records`：保留的实例记录总数（含各标识的历史终态记录）。
  总览在同一把锁内一次性计算，多个调用方并发查询时结果自洽：
  恒有 `Live == ByState 中非终态计数之和`、`Free == Quota - Live`。

### 可区分的失败原因

| 错误 | 含义 |
|---|---|
| `ErrQuotaExceeded` | 配额已满，申请被拒绝 |
| `ErrInstanceNotFound` | 指定标识的实例不存在 |
| `ErrInstanceTerminal` | 实例已处于终态，操作被拒绝 |
| `ErrInvalidID` | 申请标识为空 |
| `ErrInvalidLease` | 租约/续租时长非正 |
| `ErrLeaseExpired` | 租约已真正过期，不允许再续租 |

均可用 `errors.Is` 判定，且互不相同，调用方可区分容量不足、标识不存在、
实例已结束与非法输入。

## API 概览

```go
orch := orchestrator.New(orchestrator.Config{Quota: 8})

inst, reused, err := orch.Acquire("env-1", time.Minute) // 申请（幂等，reused 区分复用/新建）
inst, err = orch.Renew("env-1", 2*time.Minute)          // 续租：到期时间顺延，不占配额
inst, err = orch.ReportReady("env-1")                   // PROVISIONING -> READY
inst, err = orch.ReportFailed("env-1")                  // -> FAILED，归还配额
inst, err = orch.Release("env-1")                       // -> RELEASED，归还配额
inst, err = orch.Expire("env-1")                        // 到期后 -> EXPIRED，归还配额
expired := orch.SweepExpired()                          // 批量回收所有已到期实例
inst, err = orch.Get("env-1")                           // 查询当前代快照
hist, err := orch.History("env-1")                      // 查询全部代（含历史终态记录）
stats := orch.Stats()                                   // 容量与各状态分布总览
n := orch.LiveCount()                                   // 当前占用配额的实例数
```

`Config.Now` 可注入自定义时钟，便于在测试中控制租约到期。

## 本地验证

```bash
go build ./...                 # 编译
go test ./... -race -count=1   # 运行全部测试（含竞态检测）
go test ./internal/orchestrator/ -v -count=1   # 查看判定日志
```

测试覆盖：并发申请守恒、配额超限拒绝、同标识幂等申请与复用标记、并发重复
申请同一标识、显式释放与重复释放、租约未到期/到期/重复到期回收、准备失败、
未知实例操作，以及新增的：续租前后到期判定差异、READY 实例多次续租、过期
与终态后拒绝续租、终态后同标识重新申请与历史追溯、重新申请占用配额、非法
输入拒绝且不占容量、批量到期清理与重复扫描、批量清理与申请/释放并发时的
守恒、容量与状态总览及其并发自洽性。单测日志按
`request_id=<标识> basis=<判定依据> final_state=<最终状态> err=<错误>`
格式打印，便于核对每次判定的依据与结论。
