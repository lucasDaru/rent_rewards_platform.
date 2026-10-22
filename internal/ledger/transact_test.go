package ledger

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestTransact_ErrorDiscardsWrites(t *testing.T) {
	store := NewMemoryStore()
	err := store.Transact(context.Background(), "mbr_1", func(tx Tx) error {
		tx.Append(Entry{MemberID: "mbr_1", PaymentID: "pay_1", Kind: KindAward, Points: 100, Month: "2026-10"})
		return errors.New("rollback")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	got, err := store.Balance(context.Background(), "mbr_1")
	if err != nil {
		t.Fatal(err)
	}
	if got != 0 {
		t.Fatalf("balance = %d, want 0", got)
	}
}

func TestTransact_SerializesMemberWrites(t *testing.T) {
	store := NewMemoryStore()
	const n = 20
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			err := store.Transact(context.Background(), "mbr_1", func(tx Tx) error {
				tx.Append(Entry{
					MemberID:  "mbr_1",
					PaymentID: fmt.Sprintf("pay_%d", i),
					Kind:      KindAward,
					Points:    1,
					Month:     "2026-10",
				})
				return nil
			})
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	got, err := store.Balance(context.Background(), "mbr_1")
	if err != nil {
		t.Fatal(err)
	}
	if got != n {
		t.Fatalf("balance = %d, want %d", got, n)
	}
}
