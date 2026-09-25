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

// newTestOrchestrator 返回使用可控时钟的编排器与推进时钟的函数。
func newTestOrchestrator(quota int, ttl time.Duration) (*Orchestrator, func(d time.Duration)) {
	var mu sync.Mutex
	now := time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC)
	orch := New(Config{
		Quota:    quota,
		LeaseTTL: ttl,
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
	orch, _ := newTestOrchestrator(2, time.Minute)

	first, err := orch.Acquire("env-1")
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	logDecision(t, "env-1", "首次申请，占用配额", first, err)

	second, err := orch.Acquire("env-1")
	if err != nil {
		t.Fatalf("duplicate acquire should be idempotent: %v", err)
	}
	logDecision(t, "env-1", "重复申请，幂等返回已有实例", second, err)

	if first.State != second.State || first.CreatedAt != second.CreatedAt {
		t.Fatalf("duplicate acquire returned inconsistent result: %+v vs %+v", first, second)
	}
	if got := orch.LiveCount(); got != 1 {
		t.Fatalf("duplicate acquire must not consume extra quota, live=%d want 1", got)
	}
}

func TestAcquireQuotaExceeded(t *testing.T) {
	orch, _ := newTestOrchestrator(1, time.Minute)

	if _, err := orch.Acquire("env-1"); err != nil {
		t.Fatalf("acquire env-1: %v", err)
	}
	inst, err := orch.Acquire("env-2")
	logDecision(t, "env-2", "配额已满(live=1 quota=1)，拒绝", inst, err)
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("want ErrQuotaExceeded, got %v", err)
	}
	if inst != nil {
		t.Fatalf("rejected acquire must not return an instance, got %+v", inst)
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
	orch, _ := newTestOrchestrator(quota, time.Minute)

	var wg sync.WaitGroup
	results := make([]error, requesters)
	for i := 0; i < requesters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("env-%d", i)
			_, err := orch.Acquire(id)
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
	orch, _ := newTestOrchestrator(4, time.Minute)

	const goroutines = 32
	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = orch.Acquire("shared-env")
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
	orch, _ := newTestOrchestrator(2, time.Minute)

	if _, err := orch.Acquire("env-1"); err != nil {
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
	if _, err := orch.Acquire("env-2"); err != nil {
		t.Fatalf("quota must be reusable after release: %v", err)
	}
}

func TestLeaseExpiry(t *testing.T) {
	orch, advance := newTestOrchestrator(2, time.Minute)

	if _, err := orch.Acquire("env-1"); err != nil {
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
	orch, _ := newTestOrchestrator(1, time.Minute)

	if _, err := orch.Acquire("env-1"); err != nil {
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
	if _, err := orch.Acquire("env-2"); err != nil {
		t.Fatalf("quota must be reusable after failure: %v", err)
	}

	// 失败实例不可再就绪。
	if _, err := orch.ReportReady("env-1"); !errors.Is(err, ErrInstanceTerminal) {
		t.Fatalf("ready on failed instance: want ErrInstanceTerminal, got %v", err)
	}
}

func TestReadyThenRelease(t *testing.T) {
	orch, _ := newTestOrchestrator(1, time.Minute)

	if _, err := orch.Acquire("env-1"); err != nil {
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
	orch, _ := newTestOrchestrator(quota, time.Minute)
	for i := 0; i < quota; i++ {
		if _, err := orch.Acquire(fmt.Sprintf("env-%d", i)); err != nil {
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
	orch, _ := newTestOrchestrator(1, time.Minute)
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
