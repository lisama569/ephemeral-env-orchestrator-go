package orchestrator

import (
	"fmt"
	"sync"
	"time"
)

// Orchestrator 管理本地环境实例的生命周期与配额。
// 所有方法并发安全；配额计数与实例状态在同一把互斥锁下变更，
// 保证并发下实例数与配额占用守恒、互不覆盖。
//
// 每个标识按“代”保留记录：当前代（最新一条）参与生命周期与配额，
// 已终态的旧代记录保留在代序列中，可通过 History 追溯。
type Orchestrator struct {
	mu    sync.Mutex
	quota int
	now   func() time.Time
	byID  map[string][]*EnvInstance // 每个标识的代序列，末位为当前代
	live  int                       // 非终态实例数，即已占用配额
}

// New 按配置创建编排器。
func New(cfg Config) *Orchestrator {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Orchestrator{
		quota: cfg.Quota,
		now:   now,
		byID:  make(map[string][]*EnvInstance),
	}
}

// snapshot 返回实例的拷贝，调用方拿到的是快照而非内部指针。
func snapshot(inst *EnvInstance) *EnvInstance {
	cp := *inst
	return &cp
}

// currentLocked 返回标识的当前代实例，不存在时 ok=false。调用方须持有锁。
func (o *Orchestrator) currentLocked(id string) (*EnvInstance, bool) {
	gens := o.byID[id]
	if len(gens) == 0 {
		return nil, false
	}
	return gens[len(gens)-1], true
}

// Acquire 申请一个环境实例，lease 为本次租约时长（必须为正）。
//
// 幂等与复用：同一标识当前仍有活跃（非终态）实例时，返回该实例且
// reused=true，不重复占用配额；若该标识上一代已走完生命周期（终态），
// 则创建一个全新实例（新创建时间、新租约、从 PROVISIONING 开始），
// reused=false，旧代记录保留可通过 History 查询。
//
// 失败均可区分且不占用任何配额：空标识 ErrInvalidID、非正租约
// ErrInvalidLease、配额不足 ErrQuotaExceeded。
func (o *Orchestrator) Acquire(id string, lease time.Duration) (inst *EnvInstance, reused bool, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if id == "" {
		return nil, false, fmt.Errorf("acquire: %w", ErrInvalidID)
	}
	if lease <= 0 {
		return nil, false, fmt.Errorf("acquire %q: %w (lease=%s)", id, ErrInvalidLease, lease)
	}
	if cur, ok := o.currentLocked(id); ok && !cur.State.Terminal() {
		// 幂等：同一标识仍有活跃实例，返回一致结果。
		return snapshot(cur), true, nil
	}
	if o.live >= o.quota {
		return nil, false, fmt.Errorf("acquire %q: %w (live=%d quota=%d)", id, ErrQuotaExceeded, o.live, o.quota)
	}
	now := o.now()
	inst = &EnvInstance{
		ID:        id,
		State:     StateProvisioning,
		CreatedAt: now,
		ExpiresAt: now.Add(lease),
		UpdatedAt: now,
	}
	o.byID[id] = append(o.byID[id], inst)
	o.live++
	return snapshot(inst), false, nil
}

// Renew 为 PROVISIONING / READY 实例续租：到期时间延长 extension
// （必须为正），可续多次。续租不额外占用配额。
//
// 失败可区分：标识不存在 ErrInstanceNotFound；实例已终态
// ErrInstanceTerminal（不会把已结束的实例变回活跃）；租约已经过期
// ErrLeaseExpired（过期后不允许再续回来）；非正时长 ErrInvalidLease。
func (o *Orchestrator) Renew(id string, extension time.Duration) (*EnvInstance, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	inst, ok := o.currentLocked(id)
	if !ok {
		return nil, fmt.Errorf("renew %q: %w", id, ErrInstanceNotFound)
	}
	if inst.State.Terminal() {
		return snapshot(inst), fmt.Errorf("renew %q: %w (state=%s)", id, ErrInstanceTerminal, inst.State)
	}
	if extension <= 0 {
		return snapshot(inst), fmt.Errorf("renew %q: %w (extension=%s)", id, ErrInvalidLease, extension)
	}
	now := o.now()
	if !now.Before(inst.ExpiresAt) {
		// 租约已真正过期，不允许续回。
		return snapshot(inst), fmt.Errorf("renew %q: %w (expires_at=%s)", id, ErrLeaseExpired, inst.ExpiresAt.Format(time.RFC3339Nano))
	}
	inst.ExpiresAt = inst.ExpiresAt.Add(extension)
	inst.UpdatedAt = now
	return snapshot(inst), nil
}

// ReportReady 将 PROVISIONING 实例标记为 READY。
func (o *Orchestrator) ReportReady(id string) (*EnvInstance, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	inst, ok := o.currentLocked(id)
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

	inst, ok := o.currentLocked(id)
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

	inst, ok := o.currentLocked(id)
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

	inst, ok := o.currentLocked(id)
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

// SweepExpired 一次性扫描并回收所有租约已到期的实例（转为 EXPIRED 并
// 归还配额），返回本次新回收的实例快照。未到期与已终态的实例不受影响；
// 与申请、就绪、释放并发进行时在同一把锁内逐个判定，不重复回收也不漏收。
func (o *Orchestrator) SweepExpired() []*EnvInstance {
	o.mu.Lock()
	defer o.mu.Unlock()

	now := o.now()
	var expired []*EnvInstance
	for _, gens := range o.byID {
		cur := gens[len(gens)-1]
		if cur.State.Terminal() || now.Before(cur.ExpiresAt) {
			continue
		}
		o.transitionLocked(cur, StateExpired)
		expired = append(expired, snapshot(cur))
	}
	return expired
}

// transitionLocked 将实例转入终态并归还配额。调用方须持有锁，
// 且已确认实例处于非终态，因此配额恰好扣减一次。
func (o *Orchestrator) transitionLocked(inst *EnvInstance, to EnvState) {
	inst.State = to
	inst.UpdatedAt = o.now()
	o.live--
}

// Get 返回标识当前代实例的快照；不存在时返回 ErrInstanceNotFound。
func (o *Orchestrator) Get(id string) (*EnvInstance, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	inst, ok := o.currentLocked(id)
	if !ok {
		return nil, fmt.Errorf("get %q: %w", id, ErrInstanceNotFound)
	}
	return snapshot(inst), nil
}

// History 返回标识全部代实例的快照（最旧在前，末位为当前代），
// 用于追溯已终态的旧记录；标识不存在时返回 ErrInstanceNotFound。
func (o *Orchestrator) History(id string) ([]*EnvInstance, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	gens := o.byID[id]
	if len(gens) == 0 {
		return nil, fmt.Errorf("history %q: %w", id, ErrInstanceNotFound)
	}
	out := make([]*EnvInstance, len(gens))
	for i, inst := range gens {
		out[i] = snapshot(inst)
	}
	return out, nil
}

// Stats 返回容量与状态总览。在同一把锁内一次性计算，
// 并发查询时各字段自洽：Live 等于非终态计数之和，Free = Quota - Live。
func (o *Orchestrator) Stats() Stats {
	o.mu.Lock()
	defer o.mu.Unlock()

	s := Stats{
		Quota:   o.quota,
		Live:    o.live,
		Free:    o.quota - o.live,
		ByState: make(map[EnvState]int),
	}
	for _, gens := range o.byID {
		s.Records += len(gens)
		cur := gens[len(gens)-1]
		s.ByState[cur.State]++
	}
	return s
}

// LiveCount 返回当前占用配额的非终态实例数。
func (o *Orchestrator) LiveCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.live
}
