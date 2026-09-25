package orchestrator

import (
	"errors"
	"strconv"
	"sync"
	"time"
)

// ErrNotFound 表示指定 ID 的实例不存在。
var ErrNotFound = errors.New("instance not found")

// ErrInvalidState 表示当前状态不允许该转换。
var ErrInvalidState = errors.New("invalid state transition")

// Clock 抽象时间来源，便于测试租约到期。
type Clock func() time.Time

// Orchestrator 用本地内存资源模拟临时环境编排器。
//
// 配额计数 active 只统计 Preparing/Ready 两个占用态；
// 进入任何终态(Failed/Released/Expired)都必须且只能扣减一次。
type Orchestrator struct {
	mu        sync.Mutex
	instances map[string]*Instance
	maxActive int
	now       Clock
	active    int
}

// Option 用于构造 Orchestrator。
type Option func(*Orchestrator)

// WithClock 注入自定义时钟。
func WithClock(c Clock) Option {
	return func(o *Orchestrator) { o.now = c }
}

// New 创建一个最大并发实例数为 maxActive 的编排器。
func New(maxActive int, opts ...Option) *Orchestrator {
	o := &Orchestrator{
		instances: make(map[string]*Instance),
		maxActive: maxActive,
		now:       time.Now,
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// Acquire 申请（或复用）同一 ID 的环境实例。
//
//   - ID 为空或 TTL 非正：返回 *RejectError{RejectInvalidArg}，不占配额；
//   - 配额不足：返回 *RejectError{RejectQuotaExceeded}，不占配额；
//   - 已存在占用态实例：原样返回，Reused=true，不重复创建、不重复扣配额；
//   - 已存在终态实例：视为新一轮申请，重新创建。
func (o *Orchestrator) Acquire(id string, leaseTTL time.Duration) (*AcquireResult, error) {
	if id == "" {
		return nil, &RejectError{Reason: RejectInvalidArg, Msg: "instance id must not be empty"}
	}
	if leaseTTL <= 0 {
		return nil, &RejectError{Reason: RejectInvalidArg, Msg: "lease ttl must be positive"}
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if existing, ok := o.instances[id]; ok {
		if isActive(existing.State) {
			return &AcquireResult{Instance: clone(existing), Reused: true}, nil
		}
	}

	if o.active >= o.maxActive {
		return nil, &RejectError{
			Reason: RejectQuotaExceeded,
			Msg:    "quota exceeded: active=" + strconv.Itoa(o.active) + " max=" + strconv.Itoa(o.maxActive),
		}
	}

	now := o.now()
	inst := &Instance{
		ID:        id,
		State:     StatePreparing,
		LeaseTTL:  leaseTTL,
		CreatedAt: now,
	}
	o.instances[id] = inst
	o.active++
	return &AcquireResult{Instance: clone(inst), Reused: false}, nil
}

// MarkReady 将 Preparing 实例标记为 Ready，并据此计算租约到期时刻。
func (o *Orchestrator) MarkReady(id string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	inst, ok := o.instances[id]
	if !ok {
		return ErrNotFound
	}
	if inst.State != StatePreparing {
		return ErrInvalidState
	}
	now := o.now()
	inst.State = StateReady
	inst.ExpiresAt = now.Add(inst.LeaseTTL)
	return nil
}

// FailPrepare 将 Preparing 实例转为 Failed 终态并归还配额。
// 对非 Preparing 状态调用返回 ErrInvalidState，绝不重复扣减。
func (o *Orchestrator) FailPrepare(id string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	inst, ok := o.instances[id]
	if !ok {
		return ErrNotFound
	}
	if inst.State != StatePreparing {
		return ErrInvalidState
	}
	inst.State = StateFailed
	inst.EndedAt = o.now()
	o.active--
	return nil
}

// Release 显式释放 Preparing/Ready 实例并归还配额。
// 对已处于终态的实例重复回收：Reclaimed=false，不再次扣减；
// 调用方可通过返回快照的 State 区分此前是 Released/Expired/Failed。
func (o *Orchestrator) Release(id string) (*ReleaseResult, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	inst, ok := o.instances[id]
	if !ok {
		return nil, ErrNotFound
	}
	if !isActive(inst.State) {
		return &ReleaseResult{Instance: clone(inst), Reclaimed: false}, nil
	}
	inst.State = StateReleased
	inst.EndedAt = o.now()
	o.active--
	return &ReleaseResult{Instance: clone(inst), Reclaimed: true}, nil
}

// Sweep 回收所有租约已到期的 Ready 实例，返回本次回收数量。
func (o *Orchestrator) Sweep() int {
	o.mu.Lock()
	defer o.mu.Unlock()

	now := o.now()
	n := 0
	for _, inst := range o.instances {
		if inst.State == StateReady && !inst.ExpiresAt.IsZero() && !inst.ExpiresAt.After(now) {
			inst.State = StateExpired
			inst.EndedAt = now
			o.active--
			n++
		}
	}
	return n
}

// Get 查询实例快照（不存在返回 nil, false）。
func (o *Orchestrator) Get(id string) (*Instance, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	inst, ok := o.instances[id]
	if !ok {
		return nil, false
	}
	return clone(inst), true
}

// Stats 返回当前配额占用与各状态实例数量。
func (o *Orchestrator) Stats() Stats {
	o.mu.Lock()
	defer o.mu.Unlock()

	byState := make(map[State]int, len(stateNames))
	for _, inst := range o.instances {
		byState[inst.State]++
	}
	return Stats{Active: o.active, Max: o.maxActive, ByState: byState}
}

// isActive 判断状态是否占用配额。
func isActive(s State) bool {
	return s == StatePreparing || s == StateReady
}

// clone 返回实例副本，避免调用方在锁外修改内部状态。
func clone(in *Instance) *Instance {
	cp := *in
	return &cp
}
