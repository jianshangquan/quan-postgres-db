package store

import (
	"bytes"
	"context"
	"os"
	"testing"
)

func TestGCKeepNewest(t *testing.T) {
	dir := t.TempDir()
	disk, err := NewDisk(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	old := bytes.Repeat([]byte{0x01}, PageSize)
	newer := bytes.Repeat([]byte{0x02}, PageSize)
	if err := disk.PutPage(ctx, "default", "main", 1, 10, 0, 0, 10, old); err != nil {
		t.Fatal(err)
	}
	if err := disk.PutPage(ctx, "default", "main", 1, 10, 0, 0, 20, newer); err != nil {
		t.Fatal(err)
	}
	n := disk.GCKeepNewest()
	if n != 1 {
		t.Fatalf("deleted=%d want 1", n)
	}
	got, lsn, err := disk.GetPageAtOrBefore(ctx, "default", "main", 1, 10, 0, 0, 100)
	if err != nil || lsn != 20 || !bytes.Equal(got, newer) {
		t.Fatalf("lsn=%d err=%v", lsn, err)
	}
	oldPath := disk.path(PageKey("default", "main", 1, 10, 0, 0, 10))
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("old version should be gone: %v", err)
	}
}

func TestValidateIDRejectSlash(t *testing.T) {
	if err := ValidateID("../x"); err == nil {
		t.Fatal("expected reject")
	}
}
