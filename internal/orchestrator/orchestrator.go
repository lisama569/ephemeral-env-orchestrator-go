package orchestrator

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// entry 记录同一标识的当前一代实例以及已走完生命周期的历史实例，
// 以便同标识重新申请后仍可追溯上一次的终态记录。
type entry struct {
	current *EnvInstance
	history []*EnvInstance
}

// Orchestrator 管理本地环境实例的生命周期与配额。
// 所有方法并发安全；配额计数与实例状态在同一把互斥锁下变更，
// 保证并发下实例数与配额占用守恒、互不覆盖。
type Orchestrator struct {
	mu        sync.Mutex
	quota     int
	now       func() time.Time
	instances map[string]*entry
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
		now:       now,
		instances: make(map[string]*entry),
	}
}

// snapshot 返回实例的拷贝，调用方拿到的是快照而非内部指针。
func snapshot(inst *EnvInstance) *EnvInstance {
	cp := *inst
	return &cp
}

// Acquire 申请一个环境实例。
//   - 入参非法（空 id 或非正 ttl）时分别返回 ErrEmptyID / ErrInvalidLease，
//     不创建记录、不占用任何配额；
//   - 同一 id 仍有活跃（非终态）实例时，幂等返回该实例，Reused=true，
//     不重复占用配额，传入的 ttl 被忽略；
//   - 同一 id 的上一代实例已走完生命周期时，上一代进入历史并可经 History
//     追溯，本次创建全新一代实例（新的创建时间与租约，从 PROVISIONING 开始），
//     Reused=false，并按新实例占用一份容量；容量不足时返回 ErrQuotaExceeded。
func (o *Orchestrator) Acquire(id string, ttl time.Duration) (*AcquireResult, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if id == "" {
		return nil, fmt.Errorf("acquire: %w", ErrEmptyID)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("acquire %q ttl=%s: %w", id, ttl, ErrInvalidLease)
	}

	if e, ok := o.instances[id]; ok && e.current != nil {
		if !e.current.State.Terminal() {
			// 活跃实例命中：沿用既有幂等语义，不重复占用配额。
			return &AcquireResult{Instance: snapshot(e.current), Reused: true}, nil
		}
		// 上一代已终态：先按新实例做容量判定，通过后再归档并新建，
		// 保证被拒绝时旧记录原地保留、不产生任何代际变更。
	}
	if o.live >= o.quota {
		return nil, fmt.Errorf("acquire %q: %w (live=%d quota=%d)", id, ErrQuotaExceeded, o.live, o.quota)
	}

	now := o.now()
	inst := &EnvInstance{
		ID:        id,
		State:     StateProvisioning,
		CreatedAt: now,
		ExpiresAt: now.Add(ttl),
		UpdatedAt: now,
	}
	e, ok := o.instances[id]
	if !ok {
		e = &entry{}
		o.instances[id] = e
	} else if e.current != nil {
		// 容量判定通过后才把上一代终态实例归档，保留历史以便追溯。
		e.history = append(e.history, e.current)
	}
	e.current = inst
	o.live++
	return &AcquireResult{Instance: snapshot(inst), Reused: false}, nil
}

// Renew 在租约到期前延长活跃（PROVISIONING / READY）实例的租约，
// 可多次调用；续租只后移 ExpiresAt，不改变状态、不额外占用容量。
//   - id 为空：ErrEmptyID；extend 非正：ErrInvalidLease；
//   - 实例从未申请（或无任何记录）：ErrInstanceNotFound；
//   - 实例已走完生命周期：ErrInstanceTerminal，终态不会被复活；
//   - 租约已实际到期（now >= ExpiresAt）：ErrLeaseExpired，
//     即使实例尚未被扫描回收，也不允许再续回来。
func (o *Orchestrator) Renew(id string, extend time.Duration) (*EnvInstance, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if id == "" {
		return nil, fmt.Errorf("renew: %w", ErrEmptyID)
	}
	if extend <= 0 {
		return nil, fmt.Errorf("renew %q extend=%s: %w", id, extend, ErrInvalidLease)
	}
	inst, ok := o.lookupLocked(id)
	if !ok {
		return nil, fmt.Errorf("renew %q: %w", id, ErrInstanceNotFound)
	}
	if inst.State.Terminal() {
		return snapshot(inst), fmt.Errorf("renew %q: %w (state=%s)", id, ErrInstanceTerminal, inst.State)
	}
	now := o.now()
	if !now.Before(inst.ExpiresAt) {
		return snapshot(inst), fmt.Errorf("renew %q: %w (expires_at=%s now=%s)", id, ErrLeaseExpired, inst.ExpiresAt, now)
	}
	inst.ExpiresAt = inst.ExpiresAt.Add(extend)
	inst.UpdatedAt = now
	return snapshot(inst), nil
}

// SweepExpired 一次性扫描全部实例，回收所有租约已到期的活跃实例
// （now >= ExpiresAt），逐个转为 EXPIRED 并归还容量，返回本次回收实例的
// 快照（按标识字典序，结果稳定）。没有到期实例时返回空切片。
//
// 扫描与申请、就绪、释放等操作共用同一把互斥锁：每个实例的到期判定与回收
// 在同一个临界区内完成，因此并发下既不会重复回收（已是终态的实例直接跳过），
// 也不会漏掉当时已到期的实例，容量占用始终守恒。
func (o *Orchestrator) SweepExpired() []*EnvInstance {
	o.mu.Lock()
	defer o.mu.Unlock()

	now := o.now()
	ids := make([]string, 0, len(o.instances))
	for id := range o.instances {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	swept := make([]*EnvInstance, 0)
	for _, id := range ids {
		inst := o.instances[id].current
		if inst == nil || inst.State.Terminal() {
			continue
		}
		if now.Before(inst.ExpiresAt) {
			continue
		}
		o.transitionLocked(inst, StateExpired)
		swept = append(swept, snapshot(inst))
	}
	return swept
}

// Stats 返回当前容量与状态分布总览快照。所有字段在同一临界区内采样，
// 并发查询得到的结论自洽：ByState 中各状态数量之和等于曾申请过标识的
// 当前一代实例数，其中非终态状态数量之和恰好等于 Live。
func (o *Orchestrator) Stats() Stats {
	o.mu.Lock()
	defer o.mu.Unlock()

	byState := map[EnvState]int{
		StateProvisioning: 0,
		StateReady:        0,
		StateReleased:     0,
		StateExpired:      0,
		StateFailed:       0,
	}
	for _, e := range o.instances {
		if e.current != nil {
			byState[e.current.State]++
		}
	}
	return Stats{
		Quota:     o.quota,
		Live:      o.live,
		Available: o.quota - o.live,
		ByState:   byState,
	}
}

// History 返回指定标识已走完生命周期的历史实例快照，按代数从旧到新排列，
// 不含当前一代（活跃或尚未被取代的终态）实例。标识从未申请过时返回
// ErrInstanceNotFound；无历史时返回空切片与 nil 错误。
func (o *Orchestrator) History(id string) ([]*EnvInstance, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	e, ok := o.instances[id]
	if !ok {
		return nil, fmt.Errorf("history %q: %w", id, ErrInstanceNotFound)
	}
	out := make([]*EnvInstance, 0, len(e.history))
	for _, inst := range e.history {
		out = append(out, snapshot(inst))
	}
	return out, nil
}

// ReportReady 将 PROVISIONING 实例标记为 READY。
func (o *Orchestrator) ReportReady(id string) (*EnvInstance, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	inst, ok := o.lookupLocked(id)
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

	inst, ok := o.lookupLocked(id)
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

	inst, ok := o.lookupLocked(id)
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

	inst, ok := o.lookupLocked(id)
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

	inst, ok := o.lookupLocked(id)
	if !ok {
		return nil, fmt.Errorf("get %q: %w", id, ErrInstanceNotFound)
	}
	return snapshot(inst), nil
}

// lookupLocked 返回标识当前一代实例；调用方须持有锁。
func (o *Orchestrator) lookupLocked(id string) (*EnvInstance, bool) {
	e, ok := o.instances[id]
	if !ok || e.current == nil {
		return nil, false
	}
	return e.current, true
}

// LiveCount 返回当前占用配额的非终态实例数。
func (o *Orchestrator) LiveCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.live
}
