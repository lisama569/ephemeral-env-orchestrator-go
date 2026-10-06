package orchestrator

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// logDecision 按要求在单测日志中打印申请标识、判定依据与最终状态。
func logDecision(t *testing.T, id, basis string, inst *EnvInstance, err error) {
	t.Helper()
	state := "<none>"
	if inst != nil {
		state = string(inst.State)
	}
	t.Logf("request_id=%s basis=%s final_state=%s err=%v", id, basis, state, err)
}

// resultInstance 从申请结果中取出实例快照，便于复用日志辅助函数。
func resultInstance(r *AcquireResult) *EnvInstance {
	if r == nil {
		return nil
	}
	return r.Instance
}

// newTestOrchestrator 返回使用可控时钟的编排器与推进时钟的函数。
func newTestOrchestrator(quota int) (*Orchestrator, func(d time.Duration)) {
	var mu sync.Mutex
	now := time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC)
	orch := New(Config{
		Quota: quota,
		Now: func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			return now
		},
	})
	advance := func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		now = now.Add(d)
	}
	return orch, advance
}

func TestAcquireIdempotent(t *testing.T) {
	orch, _ := newTestOrchestrator(2)

	first, err := orch.Acquire("env-1", time.Minute)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	logDecision(t, "env-1", "首次申请，占用配额", first.Instance, err)

	second, err := orch.Acquire("env-1", time.Minute)
	if err != nil {
		t.Fatalf("duplicate acquire should be idempotent: %v", err)
	}
	logDecision(t, "env-1", "重复申请，幂等返回已有实例", second.Instance, err)

	if first.Reused {
		t.Fatalf("first acquire must report Reused=false")
	}
	if !second.Reused {
		t.Fatalf("duplicate acquire must report Reused=true")
	}
	if first.Instance.State != second.Instance.State || first.Instance.CreatedAt != second.Instance.CreatedAt {
		t.Fatalf("duplicate acquire returned inconsistent result: %+v vs %+v", first.Instance, second.Instance)
	}
	if got := orch.LiveCount(); got != 1 {
		t.Fatalf("duplicate acquire must not consume extra quota, live=%d want 1", got)
	}
}

func TestAcquireQuotaExceeded(t *testing.T) {
	orch, _ := newTestOrchestrator(1)

	if _, err := orch.Acquire("env-1", time.Minute); err != nil {
		t.Fatalf("acquire env-1: %v", err)
	}
	res, err := orch.Acquire("env-2", time.Minute)
	logDecision(t, "env-2", "配额已满(live=1 quota=1)，拒绝", resultInstance(res), err)
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("want ErrQuotaExceeded, got %v", err)
	}
	if res != nil {
		t.Fatalf("rejected acquire must not return a result, got %+v", res)
	}
	if got := orch.LiveCount(); got != 1 {
		t.Fatalf("rejected acquire must not consume quota, live=%d want 1", got)
	}
	// 被拒绝的标识不得留下悬挂实例。
	if _, err := orch.Get("env-2"); !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("rejected id must not linger, got %v", err)
	}
}

func TestConcurrentAcquireConservation(t *testing.T) {
	const quota = 8
	const requesters = 64
	orch, _ := newTestOrchestrator(quota)

	var wg sync.WaitGroup
	results := make([]error, requesters)
	for i := 0; i < requesters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("env-%d", i)
			_, err := orch.Acquire(id, time.Minute)
			results[i] = err
		}(i)
	}
	wg.Wait()

	succeeded, rejected := 0, 0
	for i, err := range results {
		id := fmt.Sprintf("env-%d", i)
		switch {
		case err == nil:
			succeeded++
			logDecision(t, id, "并发申请成功，占用一份配额", nil, nil)
		case errors.Is(err, ErrQuotaExceeded):
			rejected++
			logDecision(t, id, "并发申请因配额超限被拒绝", nil, err)
		default:
			t.Fatalf("unexpected error for %s: %v", id, err)
		}
	}
	if succeeded != quota {
		t.Fatalf("succeeded=%d want exactly quota=%d", succeeded, quota)
	}
	if rejected != requesters-quota {
		t.Fatalf("rejected=%d want %d", rejected, requesters-quota)
	}
	if got := orch.LiveCount(); got != quota {
		t.Fatalf("live=%d want %d：并发下配额必须守恒", got, quota)
	}
}

func TestConcurrentDuplicateAcquireSameID(t *testing.T) {
	orch, _ := newTestOrchestrator(4)

	const goroutines = 32
	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = orch.Acquire("shared-env", time.Minute)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: duplicate acquire must succeed idempotently: %v", i, err)
		}
	}
	if got := orch.LiveCount(); got != 1 {
		t.Fatalf("concurrent duplicate acquire must occupy exactly one slot, live=%d", got)
	}
	logDecision(t, "shared-env", "并发重复申请同一标识，仅占用一份配额", mustGet(t, orch, "shared-env"), nil)
}

func mustGet(t *testing.T, orch *Orchestrator, id string) *EnvInstance {
	t.Helper()
	inst, err := orch.Get(id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	return inst
}

func TestReleaseAndDoubleRelease(t *testing.T) {
	orch, _ := newTestOrchestrator(2)

	if _, err := orch.Acquire("env-1", time.Minute); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	inst, err := orch.Release("env-1")
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	logDecision(t, "env-1", "显式释放，归还配额", inst, err)
	if inst.State != StateReleased {
		t.Fatalf("state=%s want RELEASED", inst.State)
	}
	if got := orch.LiveCount(); got != 0 {
		t.Fatalf("live=%d want 0 after release", got)
	}

	// 重复回收：结论可区分，且不重复扣减。
	inst2, err2 := orch.Release("env-1")
	logDecision(t, "env-1", "重复释放，已终态，拒绝且不重复扣减", inst2, err2)
	if !errors.Is(err2, ErrInstanceTerminal) {
		t.Fatalf("double release: want ErrInstanceTerminal, got %v", err2)
	}
	if inst2 == nil || inst2.State != StateReleased {
		t.Fatalf("double release must return original terminal instance, got %+v", inst2)
	}
	if got := orch.LiveCount(); got != 0 {
		t.Fatalf("double release must not decrement again, live=%d", got)
	}

	// 释放后配额可复用。
	if _, err := orch.Acquire("env-2", time.Minute); err != nil {
		t.Fatalf("quota must be reusable after release: %v", err)
	}
}

func TestLeaseExpiry(t *testing.T) {
	orch, advance := newTestOrchestrator(2)

	if _, err := orch.Acquire("env-1", time.Minute); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	// 未到期：Expire 不转换状态。
	inst, err := orch.Expire("env-1")
	if err != nil {
		t.Fatalf("expire before deadline must be a no-op, got %v", err)
	}
	logDecision(t, "env-1", "租约未到期，不回收", inst, nil)
	if inst.State != StateProvisioning {
		t.Fatalf("state=%s want PROVISIONING before deadline", inst.State)
	}

	// 到期后回收为 EXPIRED 并归还配额。
	advance(2 * time.Minute)
	inst, err = orch.Expire("env-1")
	if err != nil {
		t.Fatalf("expire after deadline: %v", err)
	}
	logDecision(t, "env-1", "租约到期，回收为 EXPIRED", inst, err)
	if inst.State != StateExpired {
		t.Fatalf("state=%s want EXPIRED", inst.State)
	}
	if got := orch.LiveCount(); got != 0 {
		t.Fatalf("live=%d want 0 after expiry", got)
	}

	// 重复到期回收：可区分的结论，不重复扣减。
	inst2, err2 := orch.Expire("env-1")
	logDecision(t, "env-1", "重复到期回收，已终态，拒绝且不重复扣减", inst2, err2)
	if !errors.Is(err2, ErrInstanceTerminal) {
		t.Fatalf("double expire: want ErrInstanceTerminal, got %v", err2)
	}
	if got := orch.LiveCount(); got != 0 {
		t.Fatalf("double expire must not decrement again, live=%d", got)
	}
}

func TestProvisionFailure(t *testing.T) {
	orch, _ := newTestOrchestrator(1)

	if _, err := orch.Acquire("env-1", time.Minute); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	inst, err := orch.ReportFailed("env-1")
	if err != nil {
		t.Fatalf("report failed: %v", err)
	}
	logDecision(t, "env-1", "准备失败，转入 FAILED 并归还配额", inst, err)
	if inst.State != StateFailed {
		t.Fatalf("state=%s want FAILED", inst.State)
	}
	if got := orch.LiveCount(); got != 0 {
		t.Fatalf("failed instance must not leak quota, live=%d", got)
	}

	// 失败不阻塞后续申请（无悬挂配额）。
	if _, err := orch.Acquire("env-2", time.Minute); err != nil {
		t.Fatalf("quota must be reusable after failure: %v", err)
	}

	// 失败实例不可再就绪。
	if _, err := orch.ReportReady("env-1"); !errors.Is(err, ErrInstanceTerminal) {
		t.Fatalf("ready on failed instance: want ErrInstanceTerminal, got %v", err)
	}
}

func TestReadyThenRelease(t *testing.T) {
	orch, _ := newTestOrchestrator(1)

	if _, err := orch.Acquire("env-1", time.Minute); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	inst, err := orch.ReportReady("env-1")
	if err != nil {
		t.Fatalf("report ready: %v", err)
	}
	logDecision(t, "env-1", "准备完成，PROVISIONING -> READY", inst, err)
	if inst.State != StateReady {
		t.Fatalf("state=%s want READY", inst.State)
	}
	inst, err = orch.Release("env-1")
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	logDecision(t, "env-1", "就绪后显式释放，READY -> RELEASED", inst, err)
	if inst.State != StateReleased {
		t.Fatalf("state=%s want RELEASED", inst.State)
	}
}

func TestConcurrentReleaseConservation(t *testing.T) {
	const quota = 16
	orch, _ := newTestOrchestrator(quota)
	for i := 0; i < quota; i++ {
		if _, err := orch.Acquire(fmt.Sprintf("env-%d", i), time.Minute); err != nil {
			t.Fatalf("acquire env-%d: %v", i, err)
		}
	}

	// 并发重复释放同一批实例：每个实例只应扣减一次配额。
	var wg sync.WaitGroup
	for i := 0; i < quota; i++ {
		for r := 0; r < 4; r++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, _ = orch.Release(fmt.Sprintf("env-%d", i))
			}(i)
		}
	}
	wg.Wait()

	if got := orch.LiveCount(); got != 0 {
		t.Fatalf("live=%d want 0：并发重复释放不得出现负扣减或泄漏", got)
	}
	for i := 0; i < quota; i++ {
		id := fmt.Sprintf("env-%d", i)
		inst := mustGet(t, orch, id)
		if inst.State != StateReleased {
			t.Fatalf("%s state=%s want RELEASED", id, inst.State)
		}
	}
	t.Logf("request_id=all basis=并发重复释放 %d 个实例 final_state=RELEASED live=%d", quota, orch.LiveCount())
}

func TestUnknownInstance(t *testing.T) {
	orch, _ := newTestOrchestrator(1)
	for _, op := range []struct {
		name string
		fn   func(string) (*EnvInstance, error)
	}{
		{"Get", orch.Get},
		{"Release", orch.Release},
		{"Expire", orch.Expire},
		{"ReportReady", orch.ReportReady},
		{"ReportFailed", orch.ReportFailed},
	} {
		if _, err := op.fn("ghost"); !errors.Is(err, ErrInstanceNotFound) {
			t.Fatalf("%s on unknown id: want ErrInstanceNotFound, got %v", op.name, err)
		}
	}
}

func TestRenewExtendsLease(t *testing.T) {
	orch, advance := newTestOrchestrator(2)

	res, err := orch.Acquire("env-1", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	originalExpiry := res.Instance.ExpiresAt

	// 到期前续租一次：到期时间后移，状态与容量不变。
	advance(50 * time.Second)
	renewed, err := orch.Renew("env-1", 30*time.Second)
	if err != nil {
		t.Fatalf("renew before expiry: %v", err)
	}
	logDecision(t, "env-1", "到期前续租 30s，ExpiresAt 后移", renewed, err)
	if !renewed.ExpiresAt.Equal(originalExpiry.Add(30 * time.Second)) {
		t.Fatalf("expires_at=%s want %s", renewed.ExpiresAt, originalExpiry.Add(30*time.Second))
	}
	if renewed.State != StateProvisioning {
		t.Fatalf("renew must not change state, got %s", renewed.State)
	}
	if got := orch.LiveCount(); got != 1 {
		t.Fatalf("renew must not consume quota, live=%d want 1", got)
	}

	// 原租约时刻（now=60s）在新到期时间（90s）之前：不会被回收。
	advance(10 * time.Second) // now=60s，等于原到期时间
	inst, err := orch.Expire("env-1")
	if err != nil {
		t.Fatalf("expire at original deadline after renew: %v", err)
	}
	logDecision(t, "env-1", "到达原到期时间，但续租后尚未到期，不回收", inst, nil)
	if inst.State != StateProvisioning {
		t.Fatalf("state=%s want PROVISIONING：续租后按新到期时间判定", inst.State)
	}

	// 越过新到期时间后才被回收。
	advance(31 * time.Second) // now=91s > 90s
	inst, err = orch.Expire("env-1")
	if err != nil {
		t.Fatalf("expire after renewed deadline: %v", err)
	}
	logDecision(t, "env-1", "越过续租后的到期时间，回收为 EXPIRED", inst, err)
	if inst.State != StateExpired {
		t.Fatalf("state=%s want EXPIRED", inst.State)
	}
	if got := orch.LiveCount(); got != 0 {
		t.Fatalf("live=%d want 0 after expiry", got)
	}
}

func TestRenewMultipleTimesAndReadyState(t *testing.T) {
	orch, advance := newTestOrchestrator(2)

	if _, err := orch.Acquire("env-1", time.Minute); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := orch.ReportReady("env-1"); err != nil {
		t.Fatalf("ready: %v", err)
	}

	advance(55 * time.Second)
	first, err := orch.Renew("env-1", time.Minute)
	if err != nil {
		t.Fatalf("first renew: %v", err)
	}
	// 可多次续租，每次都基于当前到期时间继续后移。
	advance(55 * time.Second)
	second, err := orch.Renew("env-1", time.Minute)
	if err != nil {
		t.Fatalf("second renew: %v", err)
	}
	logDecision(t, "env-1", "READY 实例连续两次续租各 1m", second, err)
	if !second.ExpiresAt.Equal(first.ExpiresAt.Add(time.Minute)) {
		t.Fatalf("second expiry=%s want %s", second.ExpiresAt, first.ExpiresAt.Add(time.Minute))
	}
	if second.State != StateReady {
		t.Fatalf("state=%s want READY", second.State)
	}

	// now=110s，首次到期(115s)未到：仍然存活。
	inst, err := orch.Expire("env-1")
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	if inst.State != StateReady {
		t.Fatalf("state=%s want READY before renewed deadline", inst.State)
	}
}

func TestRenewFailuresAreDistinguishable(t *testing.T) {
	orch, advance := newTestOrchestrator(2)
	if _, err := orch.Acquire("env-1", time.Minute); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := orch.Acquire("env-2", time.Minute); err != nil {
		t.Fatalf("acquire env-2: %v", err)
	}
	if _, err := orch.Release("env-2"); err != nil {
		t.Fatalf("release env-2: %v", err)
	}

	// 从未申请的标识：not found。
	_, err := orch.Renew("ghost", time.Minute)
	logDecision(t, "ghost", "续租不存在的实例，拒绝", nil, err)
	if !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("renew unknown: want ErrInstanceNotFound, got %v", err)
	}

	// 已走完生命周期的实例：terminal，不能被复活。
	inst, err := orch.Renew("env-2", time.Minute)
	logDecision(t, "env-2", "续租已释放实例，拒绝且保持终态", inst, err)
	if !errors.Is(err, ErrInstanceTerminal) {
		t.Fatalf("renew released: want ErrInstanceTerminal, got %v", err)
	}
	if inst == nil || inst.State != StateReleased {
		t.Fatalf("terminal instance must be returned unchanged, got %+v", inst)
	}

	// 租约实际到期：lease expired，即使尚未被扫描回收也不允许续回。
	advance(61 * time.Second)
	inst, err = orch.Renew("env-1", time.Minute)
	logDecision(t, "env-1", "租约已实际到期，续租拒绝（不可续回）", inst, err)
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("renew expired lease: want ErrLeaseExpired, got %v", err)
	}
	if inst.State != StateProvisioning {
		t.Fatalf("failed renew must not mutate state, got %s", inst.State)
	}
	if !inst.ExpiresAt.Equal(time.Date(2026, 9, 25, 0, 1, 0, 0, time.UTC)) {
		t.Fatalf("failed renew must not move expiry, got %s", inst.ExpiresAt)
	}
	if got := orch.LiveCount(); got != 1 {
		t.Fatalf("failed renew must not touch quota, live=%d want 1", got)
	}

	// 非法入参：空标识与非正延长时长，与上述失败可区分。
	if _, err := orch.Renew("", time.Minute); !errors.Is(err, ErrEmptyID) {
		t.Fatalf("renew empty id: want ErrEmptyID, got %v", err)
	}
	for _, d := range []time.Duration{0, -time.Second} {
		if _, err := orch.Renew("env-1", d); !errors.Is(err, ErrInvalidLease) {
			t.Fatalf("renew with %s: want ErrInvalidLease, got %v", d, err)
		}
	}
}

func TestReacquireAfterTerminalCreatesNewGeneration(t *testing.T) {
	orch, advance := newTestOrchestrator(1)

	first, err := orch.Acquire("env-1", time.Minute)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	firstCreated := first.Instance.CreatedAt
	if _, err := orch.ReportReady("env-1"); err != nil {
		t.Fatalf("ready: %v", err)
	}
	advance(2 * time.Minute)
	expired, err := orch.Expire("env-1")
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	logDecision(t, "env-1", "第一代租约到期 EXPIRED，容量归还", expired, err)
	if got := orch.LiveCount(); got != 0 {
		t.Fatalf("live=%d want 0", got)
	}

	// 同一标识重新申请：全新实例，新创建时间/租约，从 PROVISIONING 开始。
	second, err := orch.Acquire("env-1", 2*time.Minute)
	if err != nil {
		t.Fatalf("reacquire after terminal: %v", err)
	}
	logDecision(t, "env-1", "终态后同标识重新申请，新建一代并占用容量", second.Instance, err)
	if second.Reused {
		t.Fatalf("reacquire after terminal must report Reused=false")
	}
	if !second.Instance.CreatedAt.After(firstCreated) {
		t.Fatalf("new generation CreatedAt=%s must be after first=%s", second.Instance.CreatedAt, firstCreated)
	}
	if second.Instance.State != StateProvisioning {
		t.Fatalf("new generation must start at PROVISIONING, got %s", second.Instance.State)
	}
	if !second.Instance.ExpiresAt.Equal(second.Instance.CreatedAt.Add(2 * time.Minute)) {
		t.Fatalf("new generation must use new lease, expires=%s", second.Instance.ExpiresAt)
	}
	if got := orch.LiveCount(); got != 1 {
		t.Fatalf("reacquire must occupy one slot, live=%d want 1", got)
	}

	// 旧记录仍可追溯：历史含第一代终态实例，Get 看到的是新一代。
	history, err := orch.History("env-1")
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) != 1 || history[0].State != StateExpired || !history[0].CreatedAt.Equal(firstCreated) {
		t.Fatalf("history=%+v want one EXPIRED generation created at %s", history, firstCreated)
	}
	current := mustGet(t, orch, "env-1")
	if current.CreatedAt != second.Instance.CreatedAt {
		t.Fatalf("Get must return new generation, got %s", current.CreatedAt)
	}

	// 再走完一代后重新申请，历史按代数累积。
	if _, err := orch.Release("env-1"); err != nil {
		t.Fatalf("release second generation: %v", err)
	}
	if _, err := orch.Acquire("env-1", time.Minute); err != nil {
		t.Fatalf("third acquire: %v", err)
	}
	history, _ = orch.History("env-1")
	if len(history) != 2 {
		t.Fatalf("history len=%d want 2 generations", len(history))
	}
	if history[0].State != StateExpired || history[1].State != StateReleased {
		t.Fatalf("history order wrong: %s, %s", history[0].State, history[1].State)
	}
}

func TestReacquireRejectedWhenQuotaFull(t *testing.T) {
	orch, _ := newTestOrchestrator(1)

	if _, err := orch.Acquire("env-1", time.Minute); err != nil {
		t.Fatalf("acquire env-1: %v", err)
	}
	if _, err := orch.Release("env-1"); err != nil {
		t.Fatalf("release: %v", err)
	}
	// 用另一个活跃实例占满容量。
	if _, err := orch.Acquire("env-2", time.Minute); err != nil {
		t.Fatalf("acquire env-2: %v", err)
	}

	// 终态后同标识重新申请按新实例占容量，容量不足按原规则拒绝。
	res, err := orch.Acquire("env-1", time.Minute)
	logDecision(t, "env-1", "终态后重新申请但容量被占，拒绝", resultInstance(res), err)
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("want ErrQuotaExceeded, got %v", err)
	}
	// 被拒绝时不得新建一代：当前仍是上一次的 RELEASED 终态，历史为空。
	current, gerr := orch.Get("env-1")
	if gerr != nil || current.State != StateReleased {
		t.Fatalf("rejected reacquire must keep old terminal record, got %+v err=%v", current, gerr)
	}
	history, _ := orch.History("env-1")
	if len(history) != 0 {
		t.Fatalf("rejected reacquire must not archive or create generation, history=%d", len(history))
	}
}

func TestAcquireInvalidInputsRejected(t *testing.T) {
	orch, _ := newTestOrchestrator(1)

	// 空标识：与容量不足、不存在、已结束均不同。
	res, err := orch.Acquire("", time.Minute)
	logDecision(t, "<empty>", "空标识申请，直接拒绝", resultInstance(res), err)
	if !errors.Is(err, ErrEmptyID) {
		t.Fatalf("empty id: want ErrEmptyID, got %v", err)
	}

	// 非正租约：0 与负值都拒绝。
	for _, ttl := range []time.Duration{0, -time.Minute} {
		res, err := orch.Acquire("env-x", ttl)
		logDecision(t, "env-x", fmt.Sprintf("非法租约 %s，直接拒绝", ttl), resultInstance(res), err)
		if !errors.Is(err, ErrInvalidLease) {
			t.Fatalf("ttl=%s: want ErrInvalidLease, got %v", ttl, err)
		}
	}

	// 非法入参不得占用容量、不得留下记录。
	if got := orch.LiveCount(); got != 0 {
		t.Fatalf("invalid acquires must not consume quota, live=%d", got)
	}
	if _, err := orch.Get("env-x"); !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("invalid acquire must not leave records, got %v", err)
	}
	if stats := orch.Stats(); stats.Live != 0 || stats.ByState[StateProvisioning] != 0 {
		t.Fatalf("invalid acquires must not appear in stats: %+v", stats)
	}

	// 非法入参优先于容量判定：把容量占满后，非法入参仍返回入参错误而非配额错误。
	if _, err := orch.Acquire("env-ok", time.Minute); err != nil {
		t.Fatalf("acquire env-ok: %v", err)
	}
	if _, err := orch.Acquire("env-full", time.Minute); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("sanity: want ErrQuotaExceeded, got %v", err)
	}
	if _, err := orch.Acquire("", time.Minute); !errors.Is(err, ErrEmptyID) {
		t.Fatalf("empty id under full quota: want ErrEmptyID, got %v", err)
	}
	if _, err := orch.Acquire("env-y", 0); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("zero ttl under full quota: want ErrInvalidLease, got %v", err)
	}
}

func TestHistoryUnknownID(t *testing.T) {
	orch, _ := newTestOrchestrator(1)
	if _, err := orch.History("ghost"); !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("history unknown id: want ErrInstanceNotFound, got %v", err)
	}
	// 只有活跃一代、尚无历史时返回空切片而非错误。
	if _, err := orch.Acquire("env-1", time.Minute); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	history, err := orch.History("env-1")
	if err != nil || len(history) != 0 {
		t.Fatalf("active-only history: want empty, got %+v err=%v", history, err)
	}
}

func TestSweepExpiredBatch(t *testing.T) {
	orch, advance := newTestOrchestrator(5)

	// env-1/env-2：1 分钟租约；env-3：10 分钟；env-4 释放（终态）。
	if _, err := orch.Acquire("env-1", time.Minute); err != nil {
		t.Fatalf("acquire env-1: %v", err)
	}
	if _, err := orch.Acquire("env-2", time.Minute); err != nil {
		t.Fatalf("acquire env-2: %v", err)
	}
	if _, err := orch.Acquire("env-3", 10*time.Minute); err != nil {
		t.Fatalf("acquire env-3: %v", err)
	}
	if _, err := orch.Acquire("env-4", time.Minute); err != nil {
		t.Fatalf("acquire env-4: %v", err)
	}
	if _, err := orch.Release("env-4"); err != nil {
		t.Fatalf("release env-4: %v", err)
	}
	if _, err := orch.ReportReady("env-2"); err != nil {
		t.Fatalf("ready env-2: %v", err)
	}

	// 未到期：扫描无回收。
	if swept := orch.SweepExpired(); len(swept) != 0 {
		t.Fatalf("sweep before deadline: want 0, got %d", len(swept))
	}

	advance(2 * time.Minute)
	swept := orch.SweepExpired()
	t.Logf("request_id=env-1,env-2 basis=批量扫描到期实例(now 越过 1m) final_state=EXPIRED swept=%d err=<nil>", len(swept))
	if len(swept) != 2 {
		t.Fatalf("swept=%d want 2", len(swept))
	}
	// 结果按标识字典序稳定。
	if swept[0].ID != "env-1" || swept[1].ID != "env-2" {
		t.Fatalf("swept order: %s, %s", swept[0].ID, swept[1].ID)
	}
	for _, inst := range swept {
		if inst.State != StateExpired {
			t.Fatalf("%s state=%s want EXPIRED", inst.ID, inst.State)
		}
	}
	// env-3 未到期保留；env-4 已终态不被重复回收；容量恰好归还两份。
	if got := mustGet(t, orch, "env-3").State; got != StateProvisioning {
		t.Fatalf("env-3 state=%s want PROVISIONING", got)
	}
	if got := mustGet(t, orch, "env-4").State; got != StateReleased {
		t.Fatalf("env-4 state=%s want RELEASED（终态不重复回收）", got)
	}
	if got := orch.LiveCount(); got != 1 {
		t.Fatalf("live=%d want 1 after sweep", got)
	}

	// 再次扫描：没有新增到期实例，不重复回收。
	if swept2 := orch.SweepExpired(); len(swept2) != 0 {
		t.Fatalf("second sweep: want empty, got %d", len(swept2))
	}

	// 批量回收后，同标识可重新申请为新实例。
	res, err := orch.Acquire("env-1", time.Minute)
	if err != nil {
		t.Fatalf("reacquire after sweep: %v", err)
	}
	if res.Reused || res.Instance.State != StateProvisioning {
		t.Fatalf("reacquire must create new generation, got %+v reused=%v", res.Instance, res.Reused)
	}
	history, _ := orch.History("env-1")
	if len(history) != 1 || history[0].State != StateExpired {
		t.Fatalf("env-1 history wrong: %+v", history)
	}
}

func TestStatsOverview(t *testing.T) {
	orch, _ := newTestOrchestrator(4)

	stats := orch.Stats()
	if stats.Quota != 4 || stats.Live != 0 || stats.Available != 4 {
		t.Fatalf("empty stats wrong: %+v", stats)
	}

	if _, err := orch.Acquire("p1", time.Minute); err != nil {
		t.Fatalf("acquire p1: %v", err)
	}
	if _, err := orch.Acquire("p2", time.Minute); err != nil {
		t.Fatalf("acquire p2: %v", err)
	}
	if _, err := orch.Acquire("r1", time.Minute); err != nil {
		t.Fatalf("acquire r1: %v", err)
	}
	if _, err := orch.ReportReady("r1"); err != nil {
		t.Fatalf("ready r1: %v", err)
	}
	if _, err := orch.Acquire("x1", time.Minute); err != nil {
		t.Fatalf("acquire x1: %v", err)
	}
	if _, err := orch.Release("x1"); err != nil {
		t.Fatalf("release x1: %v", err)
	}

	stats = orch.Stats()
	t.Logf("request_id=<all> basis=容量与状态总览 final_state=P%d/R%d/REL%d live=%d/%d",
		stats.ByState[StateProvisioning], stats.ByState[StateReady], stats.ByState[StateReleased], stats.Live, stats.Quota)
	if stats.Live != 3 || stats.Available != 1 || stats.Quota != 4 {
		t.Fatalf("capacity stats wrong: %+v", stats)
	}
	if stats.ByState[StateProvisioning] != 2 || stats.ByState[StateReady] != 1 || stats.ByState[StateReleased] != 1 {
		t.Fatalf("state distribution wrong: %+v", stats.ByState)
	}
	if stats.ByState[StateExpired] != 0 || stats.ByState[StateFailed] != 0 {
		t.Fatalf("zero states must still appear: %+v", stats.ByState)
	}
	// 非终态分布之和必须恰好等于 Live（查询自洽）。
	if stats.ByState[StateProvisioning]+stats.ByState[StateReady] != stats.Live {
		t.Fatalf("non-terminal counts must sum to live: %+v", stats)
	}
}

func TestConcurrentSweepAcquireReleaseConservation(t *testing.T) {
	const quota = 40 // 大于标识池，拒绝主要来自同标识活跃幂等，容量竞争仍由断言覆盖
	const idCount = 20
	orch, advance := newTestOrchestrator(quota)

	const workers = 8
	const rounds = 150
	ticks := workers * rounds
	var wg sync.WaitGroup

	// 时钟推进者：不断推进，使短租约实例陆续到期。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < ticks; i++ {
			advance(3 * time.Millisecond)
		}
	}()

	// 后台批量扫描者：持续回收所有到期实例。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < ticks; i++ {
			_ = orch.SweepExpired()
		}
	}()

	// 工作者：在固定标识池上交错进行 申请/就绪/续租/释放/扫描。
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				id := fmt.Sprintf("env-%d", (w*rounds+r)%idCount)
				res, err := orch.Acquire(id, 20*time.Millisecond)
				if err != nil {
					if !errors.Is(err, ErrQuotaExceeded) {
						t.Errorf("unexpected acquire error: %v", err)
					}
					continue
				}
				if res.Reused {
					continue // 命中他人仍活跃的实例：幂等，不重复占用
				}
				if r%2 == 0 {
					_, _ = orch.ReportReady(id)
				}
				if r%3 == 0 {
					_, _ = orch.Renew(id, 10*time.Millisecond)
				}
				_ = orch.SweepExpired()
				_, _ = orch.Release(id)
			}
		}(w)
	}
	wg.Wait()

	// 越过所有剩余租约后做最终扫描：到期实例一个不漏地回收。
	advance(time.Hour)
	swept := orch.SweepExpired()
	t.Logf("request_id=<all> basis=并发申请/续租/释放/扫描结束，推进时钟后最终批量回收 final_state=EXPIRED swept=%d", len(swept))
	if swept2 := orch.SweepExpired(); len(swept2) != 0 {
		t.Fatalf("second final sweep must find nothing, got %d（重复回收或漏判）", len(swept2))
	}

	stats := orch.Stats()
	t.Logf("request_id=<all> basis=并发结束后容量与状态守恒核对 final_state=live=%d available=%d/%d byState=%v",
		stats.Live, stats.Available, stats.Quota, stats.ByState)

	// 时钟已推进一小时，所有租约都到期，扫描必须把容量清零：不重不漏。
	if stats.Live != 0 {
		t.Fatalf("live=%d want 0：所有租约到期后扫描必须清零，不重不漏", stats.Live)
	}
	if stats.Available != quota {
		t.Fatalf("available=%d want %d", stats.Available, quota)
	}
	if stats.ByState[StateProvisioning] != 0 || stats.ByState[StateReady] != 0 {
		t.Fatalf("no active instance may remain: %+v", stats.ByState)
	}
	// 每个标识当前一代恰为一条终态记录（RELEASED 或 EXPIRED）。
	terminal := stats.ByState[StateReleased] + stats.ByState[StateExpired] + stats.ByState[StateFailed]
	if terminal != idCount {
		t.Fatalf("terminal current generations=%d want %d", terminal, idCount)
	}
	if got := orch.LiveCount(); got != stats.Live {
		t.Fatalf("LiveCount=%d inconsistent with stats live=%d", got, stats.Live)
	}
}

func TestConcurrentStatsConsistency(t *testing.T) {
	const quota = 8
	orch, _ := newTestOrchestrator(quota)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// 后台持续制造状态变化：申请 -> 就绪/释放。
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				id := fmt.Sprintf("env-%d", (w*1000+i)%6)
				if _, err := orch.Acquire(id, time.Minute); err == nil {
					if i%2 == 0 {
						_, _ = orch.ReportReady(id)
					} else {
						_, _ = orch.Release(id)
					}
				}
			}
		}(w)
	}

	// 并发查询：每次拿到的总览都必须自洽。
	const iterations = 2000
	for q := 0; q < 4; q++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				select {
				case <-stop:
					return
				default:
				}
				s := orch.Stats()
				if s.Live < 0 || s.Live > s.Quota {
					t.Errorf("live out of range: %+v", s)
					return
				}
				if s.Available != s.Quota-s.Live {
					t.Errorf("available inconsistent: %+v", s)
					return
				}
				nonTerminal := s.ByState[StateProvisioning] + s.ByState[StateReady]
				if nonTerminal != s.Live {
					t.Errorf("non-terminal sum=%d != live=%d: %+v", nonTerminal, s.Live, s)
					return
				}
			}
		}()
	}

	// 让变更与查询并发跑一小段时间后发出停止信号，再统一收尾。
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()

	t.Logf("request_id=<all> basis=并发变更下多次 Stats 查询 final_state=每次快照 live 与状态分布均自洽 err=<nil>")
}
