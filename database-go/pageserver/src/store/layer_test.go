package store

import (
	"bytes"
	"context"
	"testing"
)

func TestImageLayerPutGet(t *testing.T) {
	dir := t.TempDir()
	disk, err := NewDisk(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	page := bytes.Repeat([]byte{0x11}, PageSize)
	if err := disk.PutPage(ctx, "default", "main", 1, 10, 0, 0, 5, page); err != nil {
		t.Fatal(err)
	}
	got, lsn, err := disk.GetPageAtOrBefore(ctx, "default", "main", 1, 10, 0, 0, 100)
	if err != nil || lsn != 5 || !bytes.Equal(got, page) {
		t.Fatalf("lsn=%d err=%v", lsn, err)
	}
	layer, _, ok := disk.index.Loc("default", "main", 1, 10, 0, 0, 5)
	if !ok || layer == "" {
		t.Fatalf("expected layer loc, got %q ok=%v", layer, ok)
	}
}

func TestCompactLooseThenGet(t *testing.T) {
	dir := t.TempDir()
	disk, err := NewDisk(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	page := bytes.Repeat([]byte{0x22}, PageSize)
	if err := disk.PutPageLoose(ctx, "default", "main", 1, 20, 0, 1, 7, page); err != nil {
		t.Fatal(err)
	}
	n := disk.CompactLoose()
	if n != 1 {
		t.Fatalf("compact=%d want 1", n)
	}
	got, lsn, err := disk.GetPageAtOrBefore(ctx, "default", "main", 1, 20, 0, 1, 100)
	if err != nil || lsn != 7 || !bytes.Equal(got, page) {
		t.Fatalf("lsn=%d err=%v", lsn, err)
	}
	// Restart and reload index from manifests.
	disk2, err := NewDisk(dir)
	if err != nil {
		t.Fatal(err)
	}
	got2, lsn2, err := disk2.GetPageAtOrBefore(ctx, "default", "main", 1, 20, 0, 1, 100)
	if err != nil || lsn2 != 7 || !bytes.Equal(got2, page) {
		t.Fatalf("after reload lsn=%d err=%v", lsn2, err)
	}
}

func TestGCAfterLayer(t *testing.T) {
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
}
