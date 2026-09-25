package orchestrator

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeClock 是测试用的可控时钟。
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)}
}

func rejectReason(err error) RejectReason {
	var re *RejectError
	if errors.As(err, &re) {
		return re.Reason
	}
	return RejectNone
}

// TestLifecycleReadyThenRelease 验证 申请→就绪→显式释放 主链路。
func TestLifecycleReadyThenRelease(t *testing.T) {
	oc := New(4)

	res, err := oc.Acquire("env-a", time.Minute)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if res.Reused || res.Instance.State != StatePreparing {
		t.Fatalf("unexpected result: reused=%v state=%v", res.Reused, res.Instance.State)
	}
	t.Logf("申请标识=%s 判定依据=首次申请未占配额 最终状态=%s", res.Instance.ID, res.Instance.State)

	if err := oc.MarkReady("env-a"); err != nil {
		t.Fatalf("MarkReady: %v", err)
	}
	got, _ := oc.Get("env-a")
	t.Logf("申请标识=env-a 判定依据=MarkReady 后应进入 Ready 最终状态=%s", got.State)
	if got.State != StateReady || !got.ExpiresAt.After(got.CreatedAt) {
		t.Fatalf("ready instance bad: %+v", got)
	}

	rel, err := oc.Release("env-a")
	if err != nil || !rel.Reclaimed || rel.Instance.State != StateReleased {
		t.Fatalf("Release: %+v err=%v", rel, err)
	}
	t.Logf("申请标识=env-a 判定依据=显式释放扣减一次配额 最终状态=%s", rel.Instance.State)
	if s := oc.Stats(); s.Active != 0 {
		t.Fatalf("quota leaked, active=%d", s.Active)
	}
}

// TestDuplicateAcquireIsIdempotent 同一标识重复申请必须返回一致结果。
func TestDuplicateAcquireIsIdempotent(t *testing.T) {
	oc := New(2)

	first, err := oc.Acquire("dup", 30*time.Second)
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	second, err := oc.Acquire("dup", 30*time.Second)
	if err != nil {
		t.Fatalf("second Acquire: %v", err)
	}
	if !second.Reused {
		t.Fatal("duplicate acquire should be marked Reused")
	}
	if first.Instance.ID != second.Instance.ID ||
		first.Instance.State != second.Instance.State ||
		!first.Instance.CreatedAt.Equal(second.Instance.CreatedAt) {
		t.Fatalf("inconsistent duplicate result: %+v vs %+v", first.Instance, second.Instance)
	}
	t.Logf("申请标识=dup 判定依据=占用态实例已存在,复用而非重建 最终状态=%s 占用配额=%d",
		second.Instance.State, oc.Stats().Active)
	if oc.Stats().Active != 1 {
		t.Fatal("duplicate acquire leaked quota")
	}
}

// TestQuotaExceededDoesNotLeak 超出配额必须拒绝，且不占用配额。
func TestQuotaExceededDoesNotLeak(t *testing.T) {
	oc := New(2)

	for _, id := range []string{"q1", "q2"} {
		if _, err := oc.Acquire(id, time.Minute); err != nil {
			t.Fatalf("Acquire %s: %v", id, err)
		}
	}
	_, err := oc.Acquire("q3", time.Minute)
	if rejectReason(err) != RejectQuotaExceeded {
		t.Fatalf("expect RejectQuotaExceeded, got %v", err)
	}
	t.Logf("申请标识=q3 判定依据=active=2,max=2,容量已满 拒绝原因=%s", rejectReason(err))

	// 被拒申请不得占用配额：释放一个后应能立即申请成功。
	if _, err := oc.Release("q1"); err != nil {
		t.Fatalf("Release q1: %v", err)
	}
	res, err := oc.Acquire("q3", time.Minute)
	if err != nil || res.Reused {
		t.Fatalf("retry after release should succeed: %+v %v", res, err)
	}
	t.Logf("申请标识=q3 判定依据=释放 q1 后 active=1<2,可再创建 最终状态=%s", res.Instance.State)
}

// TestInvalidArgsRejected 非法参数的拒绝原因需与配额超限可区分。
func TestInvalidArgsRejected(t *testing.T) {
	oc := New(1)

	if _, err := oc.Acquire("", time.Minute); rejectReason(err) != RejectInvalidArg {
		t.Fatalf("empty id: %v", err)
	}
	if _, err := oc.Acquire("bad", 0); rejectReason(err) != RejectInvalidArg {
		t.Fatalf("zero ttl: %v", err)
	}
	if s := oc.Stats(); s.Active != 0 {
		t.Fatalf("invalid acquire leaked quota, active=%d", s.Active)
	}
	t.Logf("申请标识=<empty>/bad 判定依据=id 为空或 ttl<=0 拒绝原因=%s", RejectInvalidArg)
}

// TestPrepareFailureFreesQuota 准备失败必须归还配额且无悬挂实例。
func TestPrepareFailureFreesQuota(t *testing.T) {
	oc := New(1)

	if _, err := oc.Acquire("fail-me", time.Minute); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := oc.FailPrepare("fail-me"); err != nil {
		t.Fatalf("FailPrepare: %v", err)
	}
	got, _ := oc.Get("fail-me")
	t.Logf("申请标识=fail-me 判定依据=准备失败进入终态并归还配额 最终状态=%s", got.State)
	if got.State != StateFailed || oc.Stats().Active != 0 {
		t.Fatalf("failure did not free quota: state=%v active=%d", got.State, oc.Stats().Active)
	}

	// 对已失败实例重复 FailPrepare 不得再扣减。
	if err := oc.FailPrepare("fail-me"); err != ErrInvalidState {
		t.Fatalf("expect ErrInvalidState, got %v", err)
	}
	if oc.Stats().Active != 0 {
		t.Fatal("repeated FailPrepare double-decremented quota")
	}

	// 终态实例允许以同一 ID 重新申请。
	res, err := oc.Acquire("fail-me", time.Minute)
	if err != nil || res.Reused {
		t.Fatalf("re-acquire after failure should be fresh: %+v %v", res, err)
	}
	t.Logf("申请标识=fail-me 判定依据=旧实例为 Failed 终态,重新创建 最终状态=%s", res.Instance.State)
}

// TestLeaseExpiry 租约到期由 Sweep 回收，与显式释放殊途同归。
func TestLeaseExpiry(t *testing.T) {
	clk := newFakeClock()
	oc := New(2, WithClock(clk.now))

	if _, err := oc.Acquire("lease", 10*time.Second); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := oc.MarkReady("lease"); err != nil {
		t.Fatalf("MarkReady: %v", err)
	}

	clk.advance(5 * time.Second)
	if n := oc.Sweep(); n != 0 {
		t.Fatalf("premature expiry: swept %d", n)
	}

	clk.advance(6 * time.Second)
	if n := oc.Sweep(); n != 1 {
		t.Fatalf("expect 1 expired, got %d", n)
	}
	got, _ := oc.Get("lease")
	t.Logf("申请标识=lease 判定依据=now 越过 ExpiresAt,Sweep 置为过期 最终状态=%s", got.State)
	if got.State != StateExpired || oc.Stats().Active != 0 {
		t.Fatalf("expiry failed: state=%v active=%d", got.State, oc.Stats().Active)
	}

	// 到期后再显式释放：不得重复扣减，且能区分此前已是 expired。
	rel, err := oc.Release("lease")
	if err != nil || rel.Reclaimed {
		t.Fatalf("release after expiry must not reclaim: %+v %v", rel, err)
	}
	t.Logf("申请标识=lease 判定依据=实例已是终态,重复回收幂等 最终状态=%s 配额占用=%d",
		rel.Instance.State, oc.Stats().Active)
	if oc.Stats().Active != 0 {
		t.Fatal("double reclaim decremented quota")
	}
}

// TestDuplicateReleaseIsIdempotent 重复回收结论可区分且不重复扣减。
func TestDuplicateReleaseIsIdempotent(t *testing.T) {
	oc := New(2)

	if _, err := oc.Acquire("rel", time.Minute); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	first, err := oc.Release("rel")
	if err != nil || !first.Reclaimed {
		t.Fatalf("first release: %+v %v", first, err)
	}
	second, err := oc.Release("rel")
	if err != nil {
		t.Fatalf("second release should not error: %v", err)
	}
	if second.Reclaimed || second.Instance.State != StateReleased {
		t.Fatalf("duplicate release indistinguishable: %+v", second)
	}
	t.Logf("申请标识=rel 判定依据=首次 Reclaimed=true,重复 Reclaimed=false 最终状态=%s 配额占用=%d",
		second.Instance.State, oc.Stats().Active)
	if oc.Stats().Active != 0 {
		t.Fatal("duplicate release double-decremented quota")
	}

	if _, err := oc.Release("ghost"); err != ErrNotFound {
		t.Fatalf("unknown id should be ErrNotFound, got %v", err)
	}
}

// TestConcurrentAcquireQuotaConservation 并发申请下实例数与配额守恒。
func TestConcurrentAcquireQuotaConservation(t *testing.T) {
	const max = 8
	const goroutines = 64
	oc := New(max)

	var wg sync.WaitGroup
	var mu sync.Mutex
	created, reused, quotaRejected := 0, 0, 0

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("env-%d", i%12) // 12 个不同标识，制造复用与竞争
			res, err := oc.Acquire(id, time.Minute)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err != nil && rejectReason(err) == RejectQuotaExceeded:
				quotaRejected++
			case err != nil:
				t.Errorf("unexpected error for %s: %v", id, err)
			case res.Reused:
				reused++
			default:
				created++
			}
		}(i)
	}
	wg.Wait()

	s := oc.Stats()
	t.Logf("判定依据=并发 %d 次申请,max=%d 新建=%d 复用=%d 配额拒绝=%d 最终配额占用=%d",
		goroutines, max, created, reused, quotaRejected, s.Active)

	if created > max {
		t.Fatalf("created %d exceeds max %d", created, max)
	}
	if s.Active != created {
		t.Fatalf("quota not conserved: active=%d created=%d", s.Active, created)
	}
	if created+reused+quotaRejected != goroutines {
		t.Fatal("some acquires unaccounted for")
	}
	if s.ByState[StatePreparing] != created {
		t.Fatalf("instance count mismatch: preparing=%d created=%d", s.ByState[StatePreparing], created)
	}
}

// TestConcurrentAcquireSameID 同一标识并发申请：只允许一个新建，其余全部复用。
func TestConcurrentAcquireSameID(t *testing.T) {
	oc := New(4)
	const goroutines = 32

	var wg sync.WaitGroup
	var mu sync.Mutex
	created := 0
	var firstCreatedAt time.Time

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := oc.Acquire("same", time.Minute)
			if err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if !res.Reused {
				created++
				firstCreatedAt = res.Instance.CreatedAt
			} else if !res.Instance.CreatedAt.Equal(firstCreatedAt) && !firstCreatedAt.IsZero() {
				t.Errorf("reused instance has different identity: %+v", res.Instance)
			}
		}()
	}
	wg.Wait()

	t.Logf("申请标识=same 判定依据=并发 %d 次同标识申请仅允许 1 次新建 新建次数=%d 配额占用=%d",
		goroutines, created, oc.Stats().Active)
	if created != 1 || oc.Stats().Active != 1 {
		t.Fatalf("duplicate creation under concurrency: created=%d active=%d", created, oc.Stats().Active)
	}
}

// TestConcurrentReleaseAndExpiry 并发释放/到期/失败混合回收：每条路径只扣减一次。
func TestConcurrentReleaseAndExpiry(t *testing.T) {
	clk := newFakeClock()
	oc := New(16, WithClock(clk.now))

	const n = 12
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("mix-%d", i)
		if _, err := oc.Acquire(id, time.Second); err != nil {
			t.Fatalf("Acquire %s: %v", id, err)
		}
		if err := oc.MarkReady(id); err != nil {
			t.Fatalf("MarkReady %s: %v", id, err)
		}
		ids = append(ids, id)
	}
	clk.advance(2 * time.Second) // 全部租约到期

	var wg sync.WaitGroup
	var mu sync.Mutex
	reclaimed := 0
	for _, id := range ids {
		for _, op := range []string{"release", "release", "fail"} {
			wg.Add(1)
			go func(id, op string) {
				defer wg.Done()
				var ok bool
				switch op {
				case "release":
					rel, err := oc.Release(id)
					if err != nil && err != ErrNotFound {
						t.Errorf("Release %s: %v", id, err)
					}
					ok = err == nil && rel.Reclaimed
				case "fail":
					ok = oc.FailPrepare(id) == nil // Ready 态下应失败
				}
				mu.Lock()
				if ok {
					reclaimed++
				}
				mu.Unlock()
			}(id, op)
		}
	}
	wg.Wait()
	expired := oc.Sweep()

	s := oc.Stats()
	t.Logf("判定依据=%d 个实例并发混合回收,首次回收+到期总数应等于实例数 显式回收=%d 到期回收=%d 最终配额占用=%d",
		n, reclaimed, expired, s.Active)

	if reclaimed+expired != n {
		t.Fatalf("reclaim count mismatch: reclaimed=%d expired=%d want=%d", reclaimed, expired, n)
	}
	if s.Active != 0 {
		t.Fatalf("quota leaked under concurrent reclaim: active=%d", s.Active)
	}
	// 无悬挂：所有实例必须处于终态。
	for _, id := range ids {
		got, _ := oc.Get(id)
		if got.State == StatePreparing || got.State == StateReady {
			t.Fatalf("dangling instance %s in state %s", id, got.State)
		}
		t.Logf("申请标识=%s 判定依据=混合回收收敛 最终状态=%s", id, got.State)
	}
}
