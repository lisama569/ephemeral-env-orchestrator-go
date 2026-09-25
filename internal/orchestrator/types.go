// Package orchestrator 提供本地临时环境实例的编排能力：
// 申请、就绪、释放、租约到期与准备失败等生命周期管理，
// 以及容量配额控制。
package orchestrator

import (
	"errors"
	"time"
)

// EnvState 表示环境实例的生命周期状态。
type EnvState string

const (
	// StateProvisioning 实例已占用配额，正在准备中。
	StateProvisioning EnvState = "PROVISIONING"
	// StateReady 实例准备完成，可被使用。
	StateReady EnvState = "READY"
	// StateReleased 实例已被显式释放（终态）。
	StateReleased EnvState = "RELEASED"
	// StateExpired 实例因租约到期被回收（终态）。
	StateExpired EnvState = "EXPIRED"
	// StateFailed 实例准备失败被回收（终态）。
	StateFailed EnvState = "FAILED"
)

// Terminal 报告状态是否为终态（不再发生转换）。
func (s EnvState) Terminal() bool {
	switch s {
	case StateReleased, StateExpired, StateFailed:
		return true
	}
	return false
}

// 可区分的失败原因，调用方可用 errors.Is 判定。
var (
	// ErrQuotaExceeded 配额已满，申请被拒绝。
	ErrQuotaExceeded = errors.New("quota exceeded")
	// ErrInstanceNotFound 指定标识的实例不存在。
	ErrInstanceNotFound = errors.New("instance not found")
	// ErrInstanceTerminal 实例已处于终态，操作被拒绝。
	ErrInstanceTerminal = errors.New("instance already in terminal state")
)

// Config 编排器配置。
type Config struct {
	// Quota 允许同时存活（非终态）的实例上限。
	Quota int
	// LeaseTTL 租约时长，到期后实例可被回收为 EXPIRED。
	LeaseTTL time.Duration
	// Now 可注入的时钟，便于测试；为 nil 时使用 time.Now。
	Now func() time.Time
}

// EnvInstance 一个环境实例的当前快照。
type EnvInstance struct {
	// ID 申请方提供的标识，同一标识的申请具有幂等性。
	ID string
	// State 当前生命周期状态。
	State EnvState
	// CreatedAt 创建时间。
	CreatedAt time.Time
	// ExpiresAt 租约到期时间。
	ExpiresAt time.Time
	// UpdatedAt 最近一次状态变更时间。
	UpdatedAt time.Time
}
