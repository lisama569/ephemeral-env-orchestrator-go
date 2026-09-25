# ephemeral-env-orchestrator-go

用本地内存资源模拟的临时环境编排器：支持环境实例的申请、就绪与回收，
在并发申请、配额约束与异常回收下保证状态转换可预期、可解释。

## 生命周期状态机

```
                 Acquire(新标识或终态标识)
                        │
                        ▼
                  ┌───────────┐   MarkReady    ┌───────┐
                  │ preparing │ ─────────────▶ │ ready │
                  └───────────┘                └───────┘
                     │    │                      │   │
        FailPrepare  │    │ Release    Release   │   │ Sweep(租约到期)
                     ▼    ▼                      ▼   ▼
                 ┌────────┐                ┌──────────────────┐
                 │ failed │                │ released/expired │
                 └────────┘                └──────────────────┘
                        （以上均为终态，不再转换）
```

| 状态 | 含义 | 是否占用配额 |
| --- | --- | --- |
| `preparing` | 申请已受理，本地资源准备中 | 是 |
| `ready` | 资源就绪，租约计时中 | 是 |
| `failed` | 准备失败（`FailPrepare`） | 否 |
| `released` | 显式释放（`Release`） | 否 |
| `expired` | 租约到期（`Sweep`） | 否 |

状态转换规则：

- `Acquire(id, ttl)`：标识不存在或处于终态时创建 `preparing` 实例；
  标识已处于占用态时**原样返回既有实例**（`Reused=true`），不重复创建、不重复扣配额。
- `MarkReady(id)`：仅允许 `preparing → ready`，并以此时刻计算 `ExpiresAt = now + ttl`。
- `FailPrepare(id)`：仅允许 `preparing → failed`，归还配额；其他状态返回 `ErrInvalidState`。
- `Release(id)`：`preparing/ready → released` 并归还配额；对终态实例重复调用返回
  `Reclaimed=false` 及当前终态快照，**不重复扣减**；未知标识返回 `ErrNotFound`。
- `Sweep()`：将所有 `ExpiresAt <= now` 的 `ready` 实例置为 `expired` 并归还配额。

三条回收路径（显式释放 / 租约到期 / 准备失败）都收敛到"终态 + 配额归还一次"，
任一实例的配额扣减至多发生一次，因此并发混合回收下无悬挂实例、无重复扣减。

## 配额与租约规则

- **配额**：`New(maxActive)` 限定同时处于 `preparing/ready` 的实例数上限。
  计数在 `Acquire` 成功时 +1、进入任一终态时 -1，全部在互斥锁内完成，并发下守恒。
- **拒绝原因可区分**：申请失败返回 `*RejectError`：
  - `RejectQuotaExceeded`：容量已满，不占用任何配额；
  - `RejectInvalidArg`：标识为空或 TTL 非正，不占用任何配额。
- **租约**：实例进入 `ready` 时按 TTL 计算到期时刻；`Sweep` 负责惰性回收。
  测试通过 `WithClock` 注入可控时钟，无需真实等待。

## 本地验证

```bash
# 单元测试（含并发申请、配额超限、重复申请/回收、租约到期、混合异常回收）
go test ./... -race -v

# 生命周期演示
go run ./cmd/demo
```

测试日志会打印每个关键判定的 `申请标识 / 判定依据 / 最终状态`，例如：

```
申请标识=q3 判定依据=active=2,max=2,容量已满 拒绝原因=quota_exceeded
申请标识=rel 判定依据=首次 Reclaimed=true,重复 Reclaimed=false 最终状态=released 配额占用=0
判定依据=12 个实例并发混合回收,首次回收+到期总数应等于实例数 显式回收=12 到期回收=0 最终配额占用=0
```

## 代码结构

```
orchestrator/
  types.go         状态、拒绝原因、实例与结果类型
  orchestrator.go  编排器：申请/就绪/失败/释放/到期回收/统计
  orchestrator_test.go  生命周期、配额与并发测试
cmd/demo/main.go   本地演示程序
```
