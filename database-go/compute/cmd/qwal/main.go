// qwal pushes one WAL file/chunk to safekeeper(s) over TCP (QWAL).
// Writes to every address in SAFEKEEPER_ADDRS and exits non-zero unless
// QUORUM members ack (so Postgres archive_command retries).
package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	wireMagic = 0x5157414c // QWAL
	authMagic = 0x51574155 // QWAU
)

func main() {
	addrSingle := flag.String("safekeeper", env("SAFEKEEPER_ADDR", ""), "single host:port (legacy)")
	addrsFlag := flag.String("safekeepers", env("SAFEKEEPER_ADDRS", ""), "comma-separated host:ports")
	tenant := flag.String("tenant", env("TENANT_ID", "default"), "")
	timeline := flag.String("timeline", env("TIMELINE_ID", "main"), "")
	startLSN := flag.Uint64("start-lsn", 0, "start LSN")
	endLSN := flag.Uint64("end-lsn", 0, "end LSN (default start+len)")
	path := flag.String("file", "", "WAL bytes file (required)")
	quorum := flag.Int("quorum", envInt("QUORUM", 0), "acks required (default: majority)")
	token := flag.String("token", env("QUAN_STORAGE_TOKEN", ""), "QWAL auth token")
	flag.Parse()
	if *path == "" {
		fail("-file required")
	}
	if *token == "" {
		fail("set QUAN_STORAGE_TOKEN or -token")
	}
	payload, err := os.ReadFile(*path)
	if err != nil {
		fail(err.Error())
	}
	if len(payload) == 0 {
		fail("empty payload")
	}
	if *startLSN == 0 {
		fail("-start-lsn required")
	}
	end := *endLSN
	if end == 0 {
		end = *startLSN + uint64(len(payload))
	}
	addrs := parseAddrs(*addrsFlag, *addrSingle)
	if len(addrs) == 0 {
		fail("set SAFEKEEPER_ADDRS or -safekeeper")
	}
	need := *quorum
	if need <= 0 {
		need = len(addrs)/2 + 1
	}
	if need > len(addrs) {
		need = len(addrs)
	}

	var mu sync.Mutex
	acks := 0
	var maxAck uint64
	var wg sync.WaitGroup
	for _, addr := range addrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			ack, err := push(addr, *token, *tenant, *timeline, *startLSN, end, payload)
			if err != nil {
				fmt.Fprintf(os.Stderr, "qwal %s: %v\n", addr, err)
				return
			}
			mu.Lock()
			acks++
			if ack > maxAck {
				maxAck = ack
			}
			mu.Unlock()
		}(addr)
	}
	wg.Wait()
	if acks < need {
		fail(fmt.Sprintf("quorum failed: %d/%d acks (need %d)", acks, len(addrs), need))
	}
	fmt.Printf("qwal ack_lsn=%d bytes=%d acks=%d/%d\n", maxAck, len(payload), acks, len(addrs))
}

func dialSafekeeper(addr string) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	caPath := env("QUAN_TLS_CA", env("TLS_CA_FILE", ""))
	if caPath == "" {
		return conn, nil
	}
	pem, err := os.ReadFile(caPath)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		_ = conn.Close()
		return nil, fmt.Errorf("no certs in %s", caPath)
	}
	host, _, _ := net.SplitHostPort(addr)
	tlsConn := tls.Client(conn, &tls.Config{RootCAs: pool, ServerName: host, MinVersion: tls.VersionTLS12})
	if err := tlsConn.Handshake(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return tlsConn, nil
}

func push(addr, token, tenant, timeline string, start, end uint64, payload []byte) (uint64, error) {
	conn, err := dialSafekeeper(addr)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(60 * time.Second))

	tbTok := []byte(token)
	var ah [6]byte
	binary.BigEndian.PutUint32(ah[0:4], authMagic)
	binary.BigEndian.PutUint16(ah[4:6], uint16(len(tbTok)))
	if _, err := conn.Write(ah[:]); err != nil {
		return 0, err
	}
	if _, err := conn.Write(tbTok); err != nil {
		return 0, err
	}

	tb := []byte(tenant)
	tlb := []byte(timeline)
	var hdr [28]byte
	binary.BigEndian.PutUint32(hdr[0:4], wireMagic)
	binary.BigEndian.PutUint16(hdr[4:6], uint16(len(tb)))
	binary.BigEndian.PutUint16(hdr[6:8], uint16(len(tlb)))
	binary.BigEndian.PutUint64(hdr[8:16], start)
	binary.BigEndian.PutUint64(hdr[16:24], end)
	binary.BigEndian.PutUint32(hdr[24:28], uint32(len(payload)))
	if _, err := conn.Write(hdr[:]); err != nil {
		return 0, err
	}
	if _, err := conn.Write(tb); err != nil {
		return 0, err
	}
	if _, err := conn.Write(tlb); err != nil {
		return 0, err
	}
	if _, err := conn.Write(payload); err != nil {
		return 0, err
	}
	var ack [8]byte
	if _, err := io.ReadFull(conn, ack[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(ack[:]), nil
}

func parseAddrs(multi, single string) []string {
	var out []string
	seen := map[string]bool{}
	add := func(s string) {
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			return
		}
		seen[s] = true
		out = append(out, s)
	}
	for _, p := range strings.Split(multi, ",") {
		add(p)
	}
	add(single)
	return out
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
