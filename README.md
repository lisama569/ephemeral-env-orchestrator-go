# ephemeral-env-orchestrator-go

本地可运行的临时环境编排器：用进程内资源模拟环境实例，提供申请、就绪、
释放、租约到期与准备失败的完整生命周期管理，并保证并发申请与配额约束下
状态转换可预期、可解释。

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

## 配额与租约规则

- **配额（Quota）**：同一时刻非终态实例数上限。`Acquire` 在 `live >= quota`
  时返回 `ErrQuotaExceeded`，且**不占用任何配额**、不留下实例记录。
- **幂等申请**：同一 ID 重复 `Acquire` 返回同一份实例快照（状态、创建时间
  一致），不重复创建、不重复占用配额。
- **租约（LeaseTTL）**：实例创建时记录 `ExpiresAt = CreatedAt + LeaseTTL`。
  `Expire(id)` 仅在当前时间到达 `ExpiresAt` 后才把实例转为 `EXPIRED`；
  未到期时为无操作，返回当前状态。
- **重复回收**：对已终态实例执行 `Release` / `Expire` / `ReportFailed`
  返回原实例快照与 `ErrInstanceTerminal`，**不重复扣减配额**。
- **并发守恒**：所有状态与配额计数在同一把互斥锁下变更；并发申请下成功数
  恰好等于配额、拒绝数可区分，实例之间互不覆盖。

### 可区分的失败原因

| 错误 | 含义 |
|---|---|
| `ErrQuotaExceeded` | 配额已满，申请被拒绝 |
| `ErrInstanceNotFound` | 指定标识的实例不存在 |
| `ErrInstanceTerminal` | 实例已处于终态，操作被拒绝 |

均可用 `errors.Is` 判定。

## API 概览

```go
orch := orchestrator.New(orchestrator.Config{Quota: 8, LeaseTTL: time.Minute})

inst, err := orch.Acquire("env-1")     // 申请（幂等）
inst, err = orch.ReportReady("env-1")  // PROVISIONING -> READY
inst, err = orch.ReportFailed("env-1") // -> FAILED，归还配额
inst, err = orch.Release("env-1")      // -> RELEASED，归还配额
inst, err = orch.Expire("env-1")       // 到期后 -> EXPIRED，归还配额
inst, err = orch.Get("env-1")          // 查询快照
n := orch.LiveCount()                  // 当前占用配额的实例数
```

`Config.Now` 可注入自定义时钟，便于在测试中控制租约到期。

## 本地验证

```bash
go build ./...                 # 编译
go test ./... -race -count=1   # 运行全部测试（含竞态检测）
go test ./internal/orchestrator/ -v -count=1   # 查看判定日志
```

测试覆盖：并发申请守恒、配额超限拒绝、同标识幂等申请、并发重复申请同一
标识、显式释放与重复释放、租约未到期/到期/重复到期回收、准备失败、未知
实例操作等。单测日志按 `request_id=<标识> basis=<判定依据>
final_state=<最终状态> err=<错误>` 格式打印，便于核对每次判定的依据与结论。
