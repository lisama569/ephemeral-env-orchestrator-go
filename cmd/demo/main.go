// demo 在本地演示临时环境编排器的完整生命周期：
// 申请 → 就绪 → 租约到期/显式释放/准备失败，以及配额超限拒绝。
package main

import (
	"errors"
	"fmt"
	"time"

	orch "github.com/lisama569/ephemeral-env-orchestrator-go/orchestrator"
)

func main() {
	oc := orch.New(2)

	// 1. 申请并就绪
	res, err := oc.Acquire("web-preview", 2*time.Second)
	must(err)
	must(oc.MarkReady("web-preview"))
	fmt.Printf("[acquire] id=%s state=%s reused=%v\n", res.Instance.ID, res.Instance.State, res.Reused)

	// 2. 同一标识重复申请 → 复用
	again, err := oc.Acquire("web-preview", 2*time.Second)
	must(err)
	fmt.Printf("[acquire] id=%s state=%s reused=%v (重复申请返回一致结果)\n",
		again.Instance.ID, again.Instance.State, again.Reused)

	// 3. 占满配额后再次申请 → 可区分的拒绝原因
	res2, err := oc.Acquire("api-preview", time.Minute)
	must(err)
	_ = res2
	if _, err := oc.Acquire("overflow", time.Minute); err != nil {
		var re *orch.RejectError
		if errors.As(err, &re) {
			fmt.Printf("[reject ] id=overflow reason=%s msg=%s\n", re.Reason, re.Msg)
		}
	}

	// 4. 租约到期回收
	time.Sleep(2100 * time.Millisecond)
	fmt.Printf("[sweep  ] expired=%d\n", oc.Sweep())

	// 5. 显式释放 + 重复释放幂等
	rel, err := oc.Release("api-preview")
	must(err)
	fmt.Printf("[release] id=%s reclaimed=%v state=%s\n", rel.Instance.ID, rel.Reclaimed, rel.Instance.State)
	rel2, err := oc.Release("api-preview")
	must(err)
	fmt.Printf("[release] id=%s reclaimed=%v state=%s (重复回收不重复扣减)\n",
		rel2.Instance.ID, rel2.Reclaimed, rel2.Instance.State)

	// 6. 准备失败路径
	res3, err := oc.Acquire("broken", time.Minute)
	must(err)
	must(oc.FailPrepare(res3.Instance.ID))
	got, _ := oc.Get("broken")
	fmt.Printf("[fail   ] id=%s state=%s\n", got.ID, got.State)

	s := oc.Stats()
	fmt.Printf("[stats  ] active=%d/%d byState=%v\n", s.Active, s.Max, s.ByState)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
