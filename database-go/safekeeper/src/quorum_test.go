package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"safekeeper/src/wal"
)

const (
	testToken = "test-token-secret"
	badToken  = "wrong-token"
)

func startSK(t *testing.T, store *wal.Wal, token string, accept *atomic.Bool) (addr string, closeFn func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	done := make(chan struct{})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-done:
					return
				default:
					return
				}
			}
			if accept != nil && !accept.Load() {
				conn.Close()
				continue
			}
			wg.Add(1)
			go func(c net.Conn) {
				defer wg.Done()
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(5 * time.Second))
				got, err := wal.ReadAuth(c)
				if err != nil || got != token {
					return
				}
				msg, err := wal.ReadAppendMsg(c)
				if err != nil {
					return
				}
				_ = store.CreateTimeline(msg.TenantID, msg.TimelineID)
				flush, err := store.Append(msg.TenantID, msg.TimelineID, msg.StartLSN, msg.EndLSN, msg.Payload)
				if err != nil {
					return
				}
				_ = wal.WriteAck(c, flush)
			}(conn)
		}
	}()
	return ln.Addr().String(), func() {
		close(done)
		_ = ln.Close()
		wg.Wait()
	}
}

func pushOne(addr, token string, payload []byte) (uint64, error) {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if err := wal.WriteAuth(conn, token); err != nil {
		return 0, err
	}
	if err := wal.WriteAppendMsg(conn, wal.AppendMsg{
		TenantID: "default", TimelineID: "main",
		StartLSN: 1000, EndLSN: 1000 + uint64(len(payload)), Payload: payload,
	}); err != nil {
		return 0, err
	}
	return wal.ReadAck(conn)
}

func TestQuorumTwoOfThree(t *testing.T) {
	dir := t.TempDir()
	stores := make([]*wal.Wal, 3)
	addrs := make([]string, 3)
	closes := make([]func(), 3)
	accepts := make([]*atomic.Bool, 3)
	for i := 0; i < 3; i++ {
		s, err := wal.NewWal(fmt.Sprintf("%s/sk%d", dir, i))
		if err != nil {
			t.Fatal(err)
		}
		stores[i] = s
		accepts[i] = &atomic.Bool{}
		accepts[i].Store(true)
		addrs[i], closes[i] = startSK(t, s, testToken, accepts[i])
		defer closes[i]()
	}
	// Close one member.
	accepts[2].Store(false)

	payload := []byte("hello-wal-chunk")
	var acks int
	for _, a := range addrs {
		if _, err := pushOne(a, testToken, payload); err == nil {
			acks++
		}
	}
	if acks < 2 {
		t.Fatalf("expected >=2 acks with one down, got %d", acks)
	}
}

func TestQuorumFailsWhenTwoDown(t *testing.T) {
	dir := t.TempDir()
	addrs := make([]string, 3)
	closes := make([]func(), 3)
	accepts := make([]*atomic.Bool, 3)
	for i := 0; i < 3; i++ {
		s, err := wal.NewWal(fmt.Sprintf("%s/sk%d", dir, i))
		if err != nil {
			t.Fatal(err)
		}
		accepts[i] = &atomic.Bool{}
		accepts[i].Store(i == 0) // only first accepts
		addrs[i], closes[i] = startSK(t, s, testToken, accepts[i])
		defer closes[i]()
	}
	payload := []byte("chunk")
	var acks int
	for _, a := range addrs {
		if _, err := pushOne(a, testToken, payload); err == nil {
			acks++
		}
	}
	need := 2
	if acks >= need {
		t.Fatalf("expected quorum fail with 1/3 up, got acks=%d", acks)
	}
}

func TestBadTokenRejected(t *testing.T) {
	s, err := wal.NewWal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	addr, closeFn := startSK(t, s, testToken, nil)
	defer closeFn()
	_, err = pushOne(addr, badToken, []byte("x"))
	if err == nil {
		t.Fatal("expected auth failure")
	}
}

func TestAuthHandshakeWire(t *testing.T) {
	var buf [6]byte
	binary.BigEndian.PutUint32(buf[0:4], wal.AuthMagic)
	binary.BigEndian.PutUint16(buf[4:6], 3)
	r, w := net.Pipe()
	defer r.Close()
	defer w.Close()
	go func() {
		_, _ = w.Write(buf[:])
		_, _ = w.Write([]byte("abc"))
	}()
	tok, err := wal.ReadAuth(r)
	if err != nil || tok != "abc" {
		t.Fatalf("got %q err=%v", tok, err)
	}
	_ = io.EOF
}
