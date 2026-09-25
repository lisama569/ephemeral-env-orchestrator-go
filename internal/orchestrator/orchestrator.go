package orchestrator

import (
	"fmt"
	"sync"
	"time"
)

// Orchestrator 管理本地环境实例的生命周期与配额。
// 所有方法并发安全；配额计数与实例状态在同一把互斥锁下变更，
// 保证并发下实例数与配额占用守恒、互不覆盖。
type Orchestrator struct {
	mu        sync.Mutex
	quota     int
	leaseTTL  time.Duration
	now       func() time.Time
	instances map[string]*EnvInstance
	live      int // 非终态实例数，即已占用配额
}

// New 按配置创建编排器。
func New(cfg Config) *Orchestrator {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Orchestrator{
		quota:     cfg.Quota,
		leaseTTL:  cfg.LeaseTTL,
		now:       now,
		instances: make(map[string]*EnvInstance),
	}
}

// snapshot 返回实例的拷贝，调用方拿到的是快照而非内部指针。
func snapshot(inst *EnvInstance) *EnvInstance {
	cp := *inst
	return &cp
}

// Acquire 申请一个环境实例。
// 同一 ID 重复申请返回已有实例（幂等），不重复占用配额；
// 配额不足时返回 ErrQuotaExceeded 且不占用任何配额。
func (o *Orchestrator) Acquire(id string) (*EnvInstance, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if inst, ok := o.instances[id]; ok {
		// 幂等：同一标识重复申请返回一致结果。
		return snapshot(inst), nil
	}
	if o.live >= o.quota {
		return nil, fmt.Errorf("acquire %q: %w (live=%d quota=%d)", id, ErrQuotaExceeded, o.live, o.quota)
	}
	now := o.now()
	inst := &EnvInstance{
		ID:        id,
		State:     StateProvisioning,
		CreatedAt: now,
		ExpiresAt: now.Add(o.leaseTTL),
		UpdatedAt: now,
	}
	o.instances[id] = inst
	o.live++
	return snapshot(inst), nil
}

// ReportReady 将 PROVISIONING 实例标记为 READY。
func (o *Orchestrator) ReportReady(id string) (*EnvInstance, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	inst, ok := o.instances[id]
	if !ok {
		return nil, fmt.Errorf("report ready %q: %w", id, ErrInstanceNotFound)
	}
	if inst.State.Terminal() {
		return snapshot(inst), fmt.Errorf("report ready %q: %w (state=%s)", id, ErrInstanceTerminal, inst.State)
	}
	inst.State = StateReady
	inst.UpdatedAt = o.now()
	return snapshot(inst), nil
}

// ReportFailed 将 PROVISIONING 实例标记为 FAILED 并释放其配额。
// 失败路径同样进入终态，不留下悬挂实例。
func (o *Orchestrator) ReportFailed(id string) (*EnvInstance, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	inst, ok := o.instances[id]
	if !ok {
		return nil, fmt.Errorf("report failed %q: %w", id, ErrInstanceNotFound)
	}
	if inst.State.Terminal() {
		return snapshot(inst), fmt.Errorf("report failed %q: %w (state=%s)", id, ErrInstanceTerminal, inst.State)
	}
	o.transitionLocked(inst, StateFailed)
	return snapshot(inst), nil
}

// Release 显式释放实例，转为 RELEASED 并归还配额。
// 对已终态实例重复释放返回原实例与 ErrInstanceTerminal，不重复扣减配额。
func (o *Orchestrator) Release(id string) (*EnvInstance, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	inst, ok := o.instances[id]
	if !ok {
		return nil, fmt.Errorf("release %q: %w", id, ErrInstanceNotFound)
	}
	if inst.State.Terminal() {
		return snapshot(inst), fmt.Errorf("release %q: %w (state=%s)", id, ErrInstanceTerminal, inst.State)
	}
	o.transitionLocked(inst, StateReleased)
	return snapshot(inst), nil
}

// Expire 回收租约已到期的实例，转为 EXPIRED 并归还配额。
// 租约未到期时不做变更，返回当前实例。
func (o *Orchestrator) Expire(id string) (*EnvInstance, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	inst, ok := o.instances[id]
	if !ok {
		return nil, fmt.Errorf("expire %q: %w", id, ErrInstanceNotFound)
	}
	if inst.State.Terminal() {
		return snapshot(inst), fmt.Errorf("expire %q: %w (state=%s)", id, ErrInstanceTerminal, inst.State)
	}
	if o.now().Before(inst.ExpiresAt) {
		// 租约未到期，不转换状态。
		return snapshot(inst), nil
	}
	o.transitionLocked(inst, StateExpired)
	return snapshot(inst), nil
}

// transitionLocked 将实例转入终态并归还配额。调用方须持有锁，
// 且已确认实例处于非终态，因此配额恰好扣减一次。
func (o *Orchestrator) transitionLocked(inst *EnvInstance, to EnvState) {
	inst.State = to
	inst.UpdatedAt = o.now()
	o.live--
}

// Get 返回实例快照；不存在时返回 ErrInstanceNotFound。
func (o *Orchestrator) Get(id string) (*EnvInstance, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	inst, ok := o.instances[id]
	if !ok {
		return nil, fmt.Errorf("get %q: %w", id, ErrInstanceNotFound)
	}
	return snapshot(inst), nil
}

// LiveCount 返回当前占用配额的非终态实例数。
func (o *Orchestrator) LiveCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.live
}
