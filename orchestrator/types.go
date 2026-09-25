package orchestrator

import "time"

// State 是环境实例在生命周期中的状态。
type State int

const (
	// StateUnknown 零值占位，不应出现。
	StateUnknown State = iota
	// StatePreparing 已受理申请、正在准备本地资源。
	StatePreparing
	// StateReady 资源已就绪，可被使用。
	StateReady
	// StateFailed 准备失败，配额已归还。
	StateFailed
	// StateReleased 被显式释放，配额已归还。
	StateReleased
	// StateExpired 租约到期，配额已归还。
	StateExpired
)

// String 返回状态的可读名称。
func (s State) String() string {
	return stateNames[s]
}

// RejectReason 用于区分申请被拒绝的具体原因。
type RejectReason int

const (
	// RejectNone 未被拒绝。
	RejectNone RejectReason = iota
	// RejectQuotaExceeded 超出容量配额。
	RejectQuotaExceeded
	// RejectInvalidArg 申请参数非法。
	RejectInvalidArg
)

// String 返回拒绝原因的可读名称。
func (r RejectReason) String() string {
	return rejectNames[r]
}

// RejectError 携带可区分的拒绝原因。
type RejectError struct {
	Reason RejectReason
	Msg    string
}

func (e *RejectError) Error() string { return e.Msg }

// Instance 描述一个本地模拟的环境实例。
type Instance struct {
	ID        string
	State     State
	LeaseTTL  time.Duration
	CreatedAt time.Time
	ExpiresAt time.Time
	EndedAt   time.Time
}

// AcquireResult 是申请接口的返回：复用已有实例时 Reused=true。
type AcquireResult struct {
	Instance *Instance
	Reused   bool
}

// ReleaseResult 是回收接口的返回：重复回收时 Reclaimed=false。
type ReleaseResult struct {
	Instance  *Instance
	Reclaimed bool
}

var stateNames = [...]string{
	StateUnknown:   "unknown",
	StatePreparing: "preparing",
	StateReady:     "ready",
	StateFailed:    "failed",
	StateReleased:  "released",
	StateExpired:   "expired",
}

var rejectNames = [...]string{
	RejectNone:          "none",
	RejectQuotaExceeded: "quota_exceeded",
	RejectInvalidArg:    "invalid_arg",
}

// Stats 描述编排器当前的资源占用。
type Stats struct {
	Active  int
	Max     int
	ByState map[State]int
}
