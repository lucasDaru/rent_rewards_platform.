package rewards_test

import (
	"testing"

	"github.com/rentpoints/pointsd/internal/ledger"
	"github.com/rentpoints/pointsd/internal/rewards"
)

func TestReversal_DoesNotRestoreMonthlyCap(t *testing.T) {
	store := ledger.NewMemoryStore()
	h := newHandler(store)

	deliver(t, h, settled("pay_1", "mbr_1", 6000))
	deliver(t, h, settled("pay_2", "mbr_1", 6000))
	deliver(t, h, reversed("pay_1", "mbr_1", 6000))
	deliver(t, h, settled("pay_3", "mbr_1", 5000))

	if got := balance(t, store, "mbr_1"); got != 4000 {
		t.Fatalf("balance = %d, want 4000", got)
	}
}

func TestReversal_ConcurrentRedeliveryClawsBackOnce(t *testing.T) {
	store := ledger.NewMemoryStore(ledger.WithLatency(dbLatency))
	replicas := []*rewards.Handler{newHandler(store), newHandler(store)}

	deliver(t, replicas[0], settled("pay_1", "mbr_1", 9500))
	deliver(t, replicas[0], settled("pay_2", "mbr_1", 1000))

	ev := reversed("pay_2", "mbr_1", 1000)
	var fns []func()
	for i := 0; i < 8; i++ {
		h := replicas[i%2]
		fns = append(fns, func() { deliver(t, h, ev) })
	}
	concurrently(fns...)

	if got := balance(t, store, "mbr_1"); got != 9500 {
		t.Fatalf("balance = %d, want 9500", got)
	}
}

func TestReversal_ZeroAwardDoesNotReduceBalance(t *testing.T) {
	store := ledger.NewMemoryStore()
	h := newHandler(store)

	deliver(t, h, settled("pay_1", "mbr_1", 10000))
	deliver(t, h, settled("pay_2", "mbr_1", 1000))
	deliver(t, h, reversed("pay_2", "mbr_1", 1000))

	if got := balance(t, store, "mbr_1"); got != 10000 {
		t.Fatalf("balance = %d, want 10000", got)
	}
}
