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
	// ErrInvalidID 申请标识为空，申请被拒绝。
	ErrInvalidID = errors.New("invalid (empty) instance id")
	// ErrInvalidLease 租约时长无效（非正），申请或续租被拒绝。
	ErrInvalidLease = errors.New("invalid (non-positive) lease duration")
	// ErrLeaseExpired 租约已经过期，不允许再续租。
	ErrLeaseExpired = errors.New("lease already expired")
)

// Config 编排器配置。
type Config struct {
	// Quota 允许同时存活（非终态）的实例上限。
	Quota int
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

// Stats 容量与状态总览：某一时刻各状态实例分布与配额使用情况。
// 在同一把锁内一次性计算，多个调用方并发查询时结果自洽。
type Stats struct {
	// Quota 允许同时存活（非终态）的实例上限。
	Quota int
	// Live 当前非终态实例数，即已占用配额。
	Live int
	// Free 剩余可用配额（Quota - Live）。
	Free int
	// ByState 各生命周期状态下的当前实例数（仅统计每标识的最新一代）。
	ByState map[EnvState]int
	// Records 保留的实例记录总数（含各标识的历史终态记录）。
	Records int
}
