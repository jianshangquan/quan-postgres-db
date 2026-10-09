package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestAppendReopenServesFrame(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWal(dir)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{0xab}, 64)
	flush, err := w.Append("default", "main", 100, 164, payload)
	if err != nil {
		t.Fatal(err)
	}
	if flush < 164 {
		t.Fatalf("flush=%d", flush)
	}
	segno := SegnoFromLSN(100)

	// Reopen store from same dir.
	w2, err := NewWal(dir)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := w2.GetFrame("default", "main", segno)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(frame.Payload, payload) {
		t.Fatalf("payload mismatch len=%d", len(frame.Payload))
	}
	if _, err := os.Stat(filepath.Join(dir, "default", "main", "wal")); err != nil {
		t.Fatal(err)
	}
}

func TestAppendRejectsBadTenant(t *testing.T) {
	w, err := NewWal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, err = w.Append("../x", "main", 1, 2, []byte{1})
	if err == nil {
		t.Fatal("expected reject")
	}
}
