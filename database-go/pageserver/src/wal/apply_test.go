package wal

import (
	"bytes"
	"context"
	"testing"

	"pageserver/src/page"
	"pageserver/src/store"
)

func TestApplyFPIThenGet(t *testing.T) {
	ctx := context.Background()
	svc := page.NewService(store.NewMemory(), 32)
	applier := NewApplier(svc)

	pageBytes := bytes.Repeat([]byte{0x11}, store.PageSize)
	payload, err := EncodeFPI(FPIRecord{
		DBOid: 1, RelFilenode: 100, Fork: 0, Block: 0, LSN: 42, Page: pageBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	applied, err := applier.Apply(ctx, "default", "main", 0, 42, payload)
	if err != nil || applied < 42 {
		t.Fatalf("apply: %d %v", applied, err)
	}
	got, lsn, err := svc.GetPage(ctx, "default", "main", 1, 100, 0, 0, 100)
	if err != nil || lsn != 42 || !bytes.Equal(got, pageBytes) {
		t.Fatalf("get: lsn=%d err=%v", lsn, err)
	}
}
