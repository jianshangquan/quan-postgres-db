package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"pageserver/src/page"
	"pageserver/src/store"
	"pageserver/src/wal"
)

func TestPullAppliesSegmentFromHTTPTest(t *testing.T) {
	ctx := context.Background()
	disk, err := store.NewDisk(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	svc := page.NewService(disk, 64)
	applier := wal.NewApplier(svc)
	state := &pullState{}

	pageBytes := bytes.Repeat([]byte{0x42}, store.PageSize)
	payload, err := wal.EncodeFPI(wal.FPIRecord{
		DBOid: 1, RelFilenode: 50, Fork: 0, Block: 0, LSN: 99, Page: pageBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	startLSN := uint64(16 * 1024 * 1024) // segno 1
	endLSN := startLSN + uint64(len(payload))
	segno := startLSN / (16 * 1024 * 1024)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		q := r.URL.Query()
		if q.Get("list") == "1" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"segnos": []uint64{segno}, "flush_lsn": endLSN,
			})
			return
		}
		w.Header().Set("X-Start-LSN", strconv.FormatUint(startLSN, 10))
		w.Header().Set("X-End-LSN", strconv.FormatUint(endLSN, 10))
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	if err := pullOnce(srv.Client(), []string{srv.URL}, "tok", "default", "main", applier, disk, state); err != nil {
		t.Fatal(err)
	}
	got, lsn, err := svc.GetPage(ctx, "default", "main", 1, 50, 0, 0, 200)
	if err != nil || lsn != 99 || !bytes.Equal(got, pageBytes) {
		t.Fatalf("get after pull: lsn=%d err=%v", lsn, err)
	}
	if state.Applied() < endLSN {
		t.Fatalf("applied=%d want >= %d", state.Applied(), endLSN)
	}
}

func TestAuthRequired(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/page", nil)
	if checkHTTPToken(req, "secret") {
		t.Fatal("empty auth should fail")
	}
	req.Header.Set("Authorization", "Bearer secret")
	if !checkHTTPToken(req, "secret") {
		t.Fatal("bearer should pass")
	}
	req2 := httptest.NewRequest(http.MethodGet, "/v1/page", nil)
	req2.Header.Set("X-Quan-Token", "secret")
	if !checkHTTPToken(req2, "secret") {
		t.Fatal("x-quan-token should pass")
	}
}
