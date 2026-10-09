package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"pageserver/src/page"
	"pageserver/src/store"
)

func TestSyncFanoutRequiresPeerOK(t *testing.T) {
	disk, err := store.NewDisk(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	svc := page.NewService(disk, 64)
	pageBytes := bytes.Repeat([]byte{0xab}, store.PageSize)

	var peerHits atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peerHits.Add(1)
		if r.Header.Get("X-Quan-Peer") != "1" {
			http.Error(w, "missing peer header", http.StatusForbidden)
			return
		}
		http.Error(w, "peer down", http.StatusInternalServerError)
	}))
	defer peer.Close()

	peerHTTPClient = peer.Client()
	h := requireAuth("tok", pageHandler(svc, "tok", []string{peer.URL}, false))

	req := httptest.NewRequest(http.MethodPut,
		"/v1/page?tenant=default&timeline=main&db=1&rel=2&fork=0&block=0&lsn=10",
		bytes.NewReader(pageBytes))
	req.Header.Set("Authorization", "Bearer tok")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503 body=%s", rr.Code, rr.Body.String())
	}
	if peerHits.Load() != 1 {
		t.Fatalf("peer hits=%d", peerHits.Load())
	}
}

func TestSyncFanoutOK(t *testing.T) {
	disk, err := store.NewDisk(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	svc := page.NewService(disk, 64)
	pageBytes := bytes.Repeat([]byte{0xcd}, store.PageSize)

	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer peer.Close()

	peerHTTPClient = peer.Client()
	h := requireAuth("tok", pageHandler(svc, "tok", []string{peer.URL}, false))

	req := httptest.NewRequest(http.MethodPut,
		"/v1/page?tenant=default&timeline=main&db=1&rel=2&fork=0&block=0&lsn=11",
		bytes.NewReader(pageBytes))
	req.Header.Set("Authorization", "Bearer tok")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", rr.Code, rr.Body.String())
	}
}
