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

// testLease 是测试中使用的标准租约时长。
const testLease = time.Minute

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

	first, _, err := orch.Acquire("env-1", testLease)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	logDecision(t, "env-1", "首次申请，占用配额", first, err)

	second, _, err := orch.Acquire("env-1", testLease)
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
	orch, _ := newTestOrchestrator(1)

	if _, _, err := orch.Acquire("env-1", testLease); err != nil {
		t.Fatalf("acquire env-1: %v", err)
	}
	inst, _, err := orch.Acquire("env-2", testLease)
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
	orch, _ := newTestOrchestrator(quota)

	var wg sync.WaitGroup
	results := make([]error, requesters)
	for i := 0; i < requesters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("env-%d", i)
			_, _, err := orch.Acquire(id, testLease)
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
			_, _, errs[i] = orch.Acquire("shared-env", testLease)
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

	if _, _, err := orch.Acquire("env-1", testLease); err != nil {
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
	if _, _, err := orch.Acquire("env-2", testLease); err != nil {
		t.Fatalf("quota must be reusable after release: %v", err)
	}
}

func TestLeaseExpiry(t *testing.T) {
	orch, advance := newTestOrchestrator(2)

	if _, _, err := orch.Acquire("env-1", testLease); err != nil {
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

	if _, _, err := orch.Acquire("env-1", testLease); err != nil {
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
	if _, _, err := orch.Acquire("env-2", testLease); err != nil {
		t.Fatalf("quota must be reusable after failure: %v", err)
	}

	// 失败实例不可再就绪。
	if _, err := orch.ReportReady("env-1"); !errors.Is(err, ErrInstanceTerminal) {
		t.Fatalf("ready on failed instance: want ErrInstanceTerminal, got %v", err)
	}
}

func TestReadyThenRelease(t *testing.T) {
	orch, _ := newTestOrchestrator(1)

	if _, _, err := orch.Acquire("env-1", testLease); err != nil {
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
		if _, _, err := orch.Acquire(fmt.Sprintf("env-%d", i), testLease); err != nil {
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

	inst, _, err := orch.Acquire("env-1", testLease)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	originalExpiry := inst.ExpiresAt

	// 到期前续租 2 分钟：新到期时间 = 原到期时间 + 2m。
	ren, err := orch.Renew("env-1", 2*time.Minute)
	if err != nil {
		t.Fatalf("renew before deadline: %v", err)
	}
	logDecision(t, "env-1", "到期前续租 2m，到期时间顺延", ren, err)
	want := originalExpiry.Add(2 * time.Minute)
	if !ren.ExpiresAt.Equal(want) {
		t.Fatalf("expires_at=%s want %s", ren.ExpiresAt, want)
	}
	if got := orch.LiveCount(); got != 1 {
		t.Fatalf("renew must not consume extra quota, live=%d want 1", got)
	}

	// 越过原到期时间但未到新到期时间：不回收。
	advance(90 * time.Second)
	inst, err = orch.Expire("env-1")
	if err != nil {
		t.Fatalf("expire before renewed deadline: %v", err)
	}
	logDecision(t, "env-1", "已过原到期时间但续租生效，不回收", inst, err)
	if inst.State != StateProvisioning {
		t.Fatalf("state=%s want PROVISIONING：续租后按新到期时间判定", inst.State)
	}

	// 越过新到期时间：回收为 EXPIRED。
	advance(90 * time.Second)
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

func TestRenewReadyAndMultipleTimes(t *testing.T) {
	orch, advance := newTestOrchestrator(2)

	if _, _, err := orch.Acquire("env-1", testLease); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := orch.ReportReady("env-1"); err != nil {
		t.Fatalf("report ready: %v", err)
	}
	// READY 状态也可续租，且可续多次，时长累加。
	if _, err := orch.Renew("env-1", time.Minute); err != nil {
		t.Fatalf("first renew on READY: %v", err)
	}
	ren, err := orch.Renew("env-1", 30*time.Second)
	if err != nil {
		t.Fatalf("second renew: %v", err)
	}
	logDecision(t, "env-1", "READY 实例连续续租两次，时长累加", ren, err)
	want := time.Date(2026, 9, 25, 0, 2, 30, 0, time.UTC)
	if !ren.ExpiresAt.Equal(want) {
		t.Fatalf("expires_at=%s want %s（1m 初始 + 1m + 30s）", ren.ExpiresAt, want)
	}
	if ren.State != StateReady {
		t.Fatalf("renew must not change state, got %s", ren.State)
	}

	// 续租后的到期判定同样适用于 READY 实例。
	advance(3 * time.Minute)
	inst, err := orch.Expire("env-1")
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	logDecision(t, "env-1", "READY 实例租约到期，回收为 EXPIRED", inst, err)
	if inst.State != StateExpired {
		t.Fatalf("state=%s want EXPIRED", inst.State)
	}
}

func TestRenewRejected(t *testing.T) {
	orch, advance := newTestOrchestrator(2)

	// 不存在的标识。
	if _, err := orch.Renew("ghost", time.Minute); !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("renew unknown: want ErrInstanceNotFound, got %v", err)
	}

	if _, _, err := orch.Acquire("env-1", testLease); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	// 非正续租时长：可区分，且不改变到期时间。
	before := mustGet(t, orch, "env-1")
	if _, err := orch.Renew("env-1", 0); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("renew zero extension: want ErrInvalidLease, got %v", err)
	}
	if _, err := orch.Renew("env-1", -time.Second); !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("renew negative extension: want ErrInvalidLease, got %v", err)
	}
	if after := mustGet(t, orch, "env-1"); !after.ExpiresAt.Equal(before.ExpiresAt) {
		t.Fatalf("rejected renew must not change expiry: %s -> %s", before.ExpiresAt, after.ExpiresAt)
	}

	// 租约真正过期后（尚未被回收）：不允许续回，实例保持原状态。
	advance(2 * time.Minute)
	inst, err := orch.Renew("env-1", time.Minute)
	logDecision(t, "env-1", "租约已过期，续租被拒绝", inst, err)
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("renew after lease expiry: want ErrLeaseExpired, got %v", err)
	}
	if inst.State != StateProvisioning {
		t.Fatalf("rejected renew must not resurrect or transition, state=%s", inst.State)
	}
	// 过期未续的实例仍可被正常回收。
	if inst, err := orch.Expire("env-1"); err != nil || inst.State != StateExpired {
		t.Fatalf("expire after rejected renew: state=%s err=%v", inst.State, err)
	}

	// 已终态实例：续租返回 ErrInstanceTerminal，不会变回活跃。
	inst, err = orch.Renew("env-1", time.Minute)
	logDecision(t, "env-1", "已终态实例续租被拒绝", inst, err)
	if !errors.Is(err, ErrInstanceTerminal) {
		t.Fatalf("renew terminal: want ErrInstanceTerminal, got %v", err)
	}
	if inst.State != StateExpired {
		t.Fatalf("terminal instance must stay EXPIRED, got %s", inst.State)
	}
	if got := orch.LiveCount(); got != 0 {
		t.Fatalf("rejected renews must not touch quota, live=%d want 0", got)
	}
}

func TestReacquireAfterTerminalCreatesNewInstance(t *testing.T) {
	orch, advance := newTestOrchestrator(2)

	first, reused, err := orch.Acquire("env-1", testLease)
	if err != nil || reused {
		t.Fatalf("first acquire: reused=%v err=%v", reused, err)
	}
	if _, err := orch.Release("env-1"); err != nil {
		t.Fatalf("release: %v", err)
	}

	// 终态后同标识重新申请：得到全新实例，而非沿用上一次的终态结果。
	advance(10 * time.Second)
	second, reused, err := orch.Acquire("env-1", 2*time.Minute)
	logDecision(t, "env-1", "上一代已终态，重新申请创建新实例", second, err)
	if err != nil {
		t.Fatalf("reacquire after terminal: %v", err)
	}
	if reused {
		t.Fatalf("reacquire after terminal must report reused=false")
	}
	if second.State != StateProvisioning {
		t.Fatalf("new instance must start PROVISIONING, got %s", second.State)
	}
	if !second.CreatedAt.After(first.CreatedAt) {
		t.Fatalf("new instance must have a fresh CreatedAt: first=%s second=%s", first.CreatedAt, second.CreatedAt)
	}
	if !second.ExpiresAt.Equal(second.CreatedAt.Add(2 * time.Minute)) {
		t.Fatalf("new instance must carry the new lease, expires_at=%s", second.ExpiresAt)
	}
	if got := orch.LiveCount(); got != 1 {
		t.Fatalf("new instance occupies exactly one slot, live=%d want 1", got)
	}

	// 旧记录保留可查：History 按代返回，末位为当前代。
	hist, err := orch.History("env-1")
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(hist) != 2 {
		t.Fatalf("history len=%d want 2", len(hist))
	}
	if hist[0].State != StateReleased || !hist[0].CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("first generation must be the released old record, got %+v", hist[0])
	}
	if hist[1].State != StateProvisioning || !hist[1].CreatedAt.Equal(second.CreatedAt) {
		t.Fatalf("second generation must be the new active instance, got %+v", hist[1])
	}
	// Get 返回当前代。
	cur := mustGet(t, orch, "env-1")
	if !cur.CreatedAt.Equal(second.CreatedAt) || cur.State != StateProvisioning {
		t.Fatalf("get must return current generation, got %+v", cur)
	}
	t.Logf("request_id=env-1 basis=历史追溯 final_state=%s generations=%d", cur.State, len(hist))
}

func TestReacquireCountsAgainstQuota(t *testing.T) {
	orch, _ := newTestOrchestrator(1)

	if _, _, err := orch.Acquire("env-1", testLease); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := orch.Release("env-1"); err != nil {
		t.Fatalf("release: %v", err)
	}
	// 同标识重新申请按新实例占用一份容量。
	if _, reused, err := orch.Acquire("env-1", testLease); err != nil || reused {
		t.Fatalf("reacquire: reused=%v err=%v", reused, err)
	}
	// 容量不足时按原有规则拒绝。
	_, _, err := orch.Acquire("env-2", testLease)
	logDecision(t, "env-2", "重新申请已占满配额，新标识被拒绝", nil, err)
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("want ErrQuotaExceeded, got %v", err)
	}
}

func TestAcquireActiveInstanceReused(t *testing.T) {
	orch, _ := newTestOrchestrator(2)

	first, reused, err := orch.Acquire("env-1", testLease)
	if err != nil || reused {
		t.Fatalf("first acquire: reused=%v err=%v", reused, err)
	}
	if _, err := orch.ReportReady("env-1"); err != nil {
		t.Fatalf("report ready: %v", err)
	}
	// 同标识仍有活跃实例：幂等返回同一实例，调用方可区分是复用。
	second, reused, err := orch.Acquire("env-1", testLease)
	logDecision(t, "env-1", "活跃实例同标识重复申请，幂等复用", second, err)
	if err != nil {
		t.Fatalf("duplicate acquire: %v", err)
	}
	if !reused {
		t.Fatalf("active duplicate acquire must report reused=true")
	}
	if !second.CreatedAt.Equal(first.CreatedAt) || second.State != StateReady {
		t.Fatalf("reused acquire must return the same active instance, got %+v", second)
	}
	if got := orch.LiveCount(); got != 1 {
		t.Fatalf("reused acquire must not consume extra quota, live=%d want 1", got)
	}
	// 复用不追加历史记录。
	if hist, _ := orch.History("env-1"); len(hist) != 1 {
		t.Fatalf("reused acquire must not append history, generations=%d", len(hist))
	}
}

func TestAcquireInvalidInput(t *testing.T) {
	orch, _ := newTestOrchestrator(1)

	cases := []struct {
		name string
		id   string
		ttl  time.Duration
		want error
	}{
		{"空标识", "", testLease, ErrInvalidID},
		{"零租约", "env-zero", 0, ErrInvalidLease},
		{"负租约", "env-negative", -time.Second, ErrInvalidLease},
	}
	for _, c := range cases {
		inst, reused, err := orch.Acquire(c.id, c.ttl)
		logDecision(t, c.id, "非法输入："+c.name+"，直接拒绝", inst, err)
		if !errors.Is(err, c.want) {
			t.Fatalf("%s: want %v, got %v", c.name, c.want, err)
		}
		if !errors.Is(err, c.want) || errors.Is(err, ErrQuotaExceeded) ||
			errors.Is(err, ErrInstanceNotFound) || errors.Is(err, ErrInstanceTerminal) {
			t.Fatalf("%s: error must be distinguishable from quota/notfound/terminal: %v", c.name, err)
		}
		if inst != nil || reused {
			t.Fatalf("%s: rejected acquire must not return an instance", c.name)
		}
	}
	// 非法输入不占用任何容量、不留下记录。
	if got := orch.LiveCount(); got != 0 {
		t.Fatalf("invalid input must not consume quota, live=%d want 0", got)
	}
	for _, id := range []string{"env-zero", "env-negative"} {
		if _, err := orch.Get(id); !errors.Is(err, ErrInstanceNotFound) {
			t.Fatalf("rejected id %q must not linger, got %v", id, err)
		}
	}
	// 拒绝后配额仍完整可用。
	if _, _, err := orch.Acquire("env-ok", testLease); err != nil {
		t.Fatalf("quota must remain intact after invalid input: %v", err)
	}
}

func TestSweepExpired(t *testing.T) {
	orch, advance := newTestOrchestrator(4)

	// 三个不同租约的实例，外加一个已显式释放的。
	if _, _, err := orch.Acquire("env-short", testLease); err != nil {
		t.Fatalf("acquire short: %v", err)
	}
	if _, _, err := orch.Acquire("env-long", 10*time.Minute); err != nil {
		t.Fatalf("acquire long: %v", err)
	}
	if _, _, err := orch.Acquire("env-ready", testLease); err != nil {
		t.Fatalf("acquire ready: %v", err)
	}
	if _, err := orch.ReportReady("env-ready"); err != nil {
		t.Fatalf("report ready: %v", err)
	}
	if _, _, err := orch.Acquire("env-gone", testLease); err != nil {
		t.Fatalf("acquire gone: %v", err)
	}
	if _, err := orch.Release("env-gone"); err != nil {
		t.Fatalf("release: %v", err)
	}

	// 未到期时扫描：无回收。
	if got := orch.SweepExpired(); len(got) != 0 {
		t.Fatalf("sweep before deadline must be a no-op, expired=%d", len(got))
	}

	// 短租约到期后一次性批量回收。
	advance(2 * time.Minute)
	expired := orch.SweepExpired()
	if len(expired) != 2 {
		t.Fatalf("sweep expired %d instances, want 2", len(expired))
	}
	for _, inst := range expired {
		logDecision(t, inst.ID, "批量扫描：租约到期，回收为 EXPIRED", inst, nil)
		if inst.State != StateExpired {
			t.Fatalf("%s state=%s want EXPIRED", inst.ID, inst.State)
		}
	}
	if got := orch.LiveCount(); got != 1 {
		t.Fatalf("live=%d want 1：仅未到期实例保留", got)
	}
	if inst := mustGet(t, orch, "env-long"); inst.State != StateProvisioning {
		t.Fatalf("unexpired instance must stay active, state=%s", inst.State)
	}

	// 重复扫描：不重复回收、不重复扣减。
	if got := orch.SweepExpired(); len(got) != 0 {
		t.Fatalf("second sweep must find nothing, expired=%d", len(got))
	}
	if got := orch.LiveCount(); got != 1 {
		t.Fatalf("second sweep must not decrement again, live=%d want 1", got)
	}
	t.Logf("request_id=all basis=重复批量扫描 final_state=EXPIRED live=%d", orch.LiveCount())
}

func TestConcurrentSweepConservation(t *testing.T) {
	const quota = 16
	orch, advance := newTestOrchestrator(quota)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 一路并发申请（租约较短，会陆续到期）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			id := fmt.Sprintf("env-%d", i%64)
			inst, _, err := orch.Acquire(id, testLease)
			if err == nil && inst != nil {
				logDecision(t, id, "并发申请成功", inst, nil)
			}
		}
	}()
	// 一路并发释放。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = orch.Release(fmt.Sprintf("env-%d", i%64))
		}
	}()
	// 一路并发批量清理。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			orch.SweepExpired()
		}
	}()

	// 推进时钟让租约陆续过期，给并发操作制造交错。
	for i := 0; i < 100; i++ {
		advance(10 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	// 守恒校验：live 与实际非终态实例数一致，且不超配额、不为负。
	stats := orch.Stats()
	nonTerminal := stats.ByState[StateProvisioning] + stats.ByState[StateReady]
	if stats.Live != nonTerminal {
		t.Fatalf("live=%d but non-terminal instances=%d：并发下计数不守恒", stats.Live, nonTerminal)
	}
	if stats.Live < 0 || stats.Live > quota {
		t.Fatalf("live=%d out of bounds [0,%d]", stats.Live, quota)
	}
	if stats.Free != quota-stats.Live {
		t.Fatalf("free=%d want %d", stats.Free, quota-stats.Live)
	}
	// 所有已过期的活跃实例都应被清理掉：再扫一次应为空。
	if got := orch.SweepExpired(); len(got) != 0 {
		t.Fatalf("after concurrent phase, sweep must find nothing left, expired=%d", len(got))
	}
	t.Logf("request_id=all basis=并发申请/释放/批量清理后守恒 final_state=mixed live=%d stats=%+v", stats.Live, stats.ByState)
}

func TestStatsOverview(t *testing.T) {
	orch, advance := newTestOrchestrator(4)

	if _, _, err := orch.Acquire("env-prov", testLease); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, _, err := orch.Acquire("env-ready", 10*time.Minute); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := orch.ReportReady("env-ready"); err != nil {
		t.Fatalf("report ready: %v", err)
	}
	if _, _, err := orch.Acquire("env-failed", 10*time.Minute); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := orch.ReportFailed("env-failed"); err != nil {
		t.Fatalf("report failed: %v", err)
	}
	if _, _, err := orch.Acquire("env-cycle", testLease); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := orch.Release("env-cycle"); err != nil {
		t.Fatalf("release: %v", err)
	}
	// 同标识重新申请：历史记录 +1，当前代重新活跃。
	if _, _, err := orch.Acquire("env-cycle", 10*time.Minute); err != nil {
		t.Fatalf("reacquire: %v", err)
	}
	// 让一个实例到期。
	advance(2 * time.Minute)
	if got := orch.SweepExpired(); len(got) != 1 {
		t.Fatalf("sweep expired %d, want 1", len(got))
	}

	s := orch.Stats()
	if s.Quota != 4 || s.Live != 2 || s.Free != 2 {
		t.Fatalf("quota/live/free = %d/%d/%d, want 4/2/2", s.Quota, s.Live, s.Free)
	}
	wantByState := map[EnvState]int{
		StateReady:        1, // env-ready
		StateFailed:       1, // env-failed
		StateProvisioning: 1, // env-cycle 当前代
		StateExpired:      1, // env-prov
	}
	if len(s.ByState) != len(wantByState) {
		t.Fatalf("by_state=%v want %v", s.ByState, wantByState)
	}
	for st, want := range wantByState {
		if s.ByState[st] != want {
			t.Fatalf("by_state[%s]=%d want %d (full: %v)", st, s.ByState[st], want, s.ByState)
		}
	}
	// 记录总数含历史：4 个标识各 1 条当前代 + env-cycle 旧代 1 条。
	if s.Records != 5 {
		t.Fatalf("records=%d want 5（含 env-cycle 的历史代）", s.Records)
	}
	t.Logf("request_id=all basis=容量与状态总览 final_state=mixed live=%d free=%d by_state=%v records=%d",
		s.Live, s.Free, s.ByState, s.Records)
}

func TestConcurrentStatsConsistent(t *testing.T) {
	const quota = 8
	orch, _ := newTestOrchestrator(quota)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	// 后台不断变化：申请、释放、批量清理。
	for _, fn := range []func(){
		func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				_, _, _ = orch.Acquire(fmt.Sprintf("env-%d", i%32), testLease)
			}
		},
		func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = orch.Release(fmt.Sprintf("env-%d", i%32))
			}
		},
		func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				orch.SweepExpired()
			}
		},
	} {
		wg.Add(1)
		go func(f func()) { defer wg.Done(); f() }(fn)
	}

	// 并发查询总览：每次结果内部必须自洽。
	for i := 0; i < 200; i++ {
		s := orch.Stats()
		nonTerminal := s.ByState[StateProvisioning] + s.ByState[StateReady]
		if s.Live != nonTerminal {
			t.Fatalf("inconsistent stats: live=%d non-terminal=%d by_state=%v", s.Live, nonTerminal, s.ByState)
		}
		if s.Free != s.Quota-s.Live {
			t.Fatalf("inconsistent stats: free=%d quota=%d live=%d", s.Free, s.Quota, s.Live)
		}
		if s.Live < 0 || s.Live > s.Quota {
			t.Fatalf("inconsistent stats: live=%d out of [0,%d]", s.Live, s.Quota)
		}
	}
	close(stop)
	wg.Wait()
}
