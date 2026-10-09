// walstreamer tails Postgres pg_wal and pushes chunks to safekeeper over TCP (QWAL).
//
// Postgres WAL segment files are always 16 MiB (preallocated / recycled). Size alone
// cannot detect new writes — we track bytes sent per safekeeper per file and, for the
// current segment, only stream up to pg_current_wal_flush_lsn.
//
// Quorum: each safekeeper has its own sent-offset. Lagging members are retried every
// poll. Progress is considered quorum-committed when at least QUORUM members have
// the same offset (majority by default).
package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"walstreamer/src/wal"
)

const segSize = 16 * 1024 * 1024

func main() {
	pgdata := flag.String("pgdata", env("PGDATA", ""), "Postgres data directory")
	skSingle := flag.String("safekeeper", env("SAFEKEEPER_ADDR", ""), "single safekeeper host:port (legacy)")
	skAddrs := flag.String("safekeepers", env("SAFEKEEPER_ADDRS", ""), "comma-separated safekeeper host:ports")
	tenant := flag.String("tenant", env("TENANT_ID", "default"), "")
	timeline := flag.String("timeline", env("TIMELINE_ID", "main"), "")
	interval := flag.Duration("interval", time.Second, "poll interval")
	psql := flag.String("psql", env("PSQL", "psql"), "psql binary for flush LSN")
	pgURL := flag.String("pgurl", env("PGURL", "postgres://postgres@127.0.0.1:5432/postgres?sslmode=disable"), "libpq URL for LSN probe")
	quorum := flag.Int("quorum", envInt("QUORUM", 0), "acks required for commit watermark (default: majority)")
	token := flag.String("token", env("QUAN_STORAGE_TOKEN", ""), "QWAL auth token")
	flag.Parse()
	if *pgdata == "" {
		fail("set -pgdata or PGDATA")
	}
	if *token == "" {
		fail("set QUAN_STORAGE_TOKEN or -token")
	}
	addrs := parseAddrs(*skAddrs, *skSingle)
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

	walDir := filepath.Join(*pgdata, "pg_wal")
	// sent[addr][segname] = bytes successfully acked by that safekeeper
	sent := make(map[string]map[string]int64, len(addrs))
	for _, a := range addrs {
		sent[a] = map[string]int64{}
	}

	fmt.Printf("walstreamer: %s → %v quorum=%d (tenant=%s timeline=%s)\n",
		walDir, addrs, need, *tenant, *timeline)

	for {
		flushLSN, curName, err := currentFlush(*psql, *pgURL)
		if err != nil {
			fmt.Fprintf(os.Stderr, "lsn probe: %v (streaming completed segments only)\n", err)
		}

		entries, err := os.ReadDir(walDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read pg_wal: %v\n", err)
			time.Sleep(*interval)
			continue
		}
		var names []string
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			if len(name) == 24 && isHex(name) {
				names = append(names, name)
			}
		}
		sort.Strings(names)

		for _, name := range names {
			path := filepath.Join(walDir, name)
			st, err := os.Stat(path)
			if err != nil {
				continue
			}
			avail := st.Size()
			if name == curName && flushLSN > 0 {
				segStart := lsnFromSeg(name)
				valid := int64(flushLSN - segStart)
				if valid < 0 {
					valid = 0
				}
				if valid > segSize {
					valid = segSize
				}
				avail = valid
				// Recycled segment: flush rewound past what we sent.
				for _, a := range addrs {
					if prev := sent[a][name]; valid < prev {
						sent[a][name] = 0
					}
				}
			} else if name == curName && flushLSN == 0 {
				continue
			}

			var wg sync.WaitGroup
			var mu sync.Mutex
			for _, addr := range addrs {
				prev := sent[addr][name]
				if avail <= prev {
					continue
				}
				wg.Add(1)
				go func(addr string, prev int64) {
					defer wg.Done()
					f, err := os.Open(path)
					if err != nil {
						return
					}
					if _, err := f.Seek(prev, io.SeekStart); err != nil {
						f.Close()
						return
					}
					chunk, err := io.ReadAll(io.LimitReader(f, avail-prev))
					f.Close()
					if err != nil || len(chunk) == 0 {
						return
					}
					startLSN := lsnFromSeg(name) + uint64(prev)
					endLSN := startLSN + uint64(len(chunk))
					ack, err := push(addr, *token, *tenant, *timeline, startLSN, endLSN, chunk)
					if err != nil {
						fmt.Fprintf(os.Stderr, "push %s → %s: %v\n", name, addr, err)
						return
					}
					mu.Lock()
					sent[addr][name] = prev + int64(len(chunk))
					mu.Unlock()
					fmt.Printf("streamed %s → %s +%d bytes lsn=%d..%d ack=%d\n",
						name, addr, len(chunk), startLSN, endLSN, ack)
				}(addr, prev)
			}
			wg.Wait()

			offs := make([]int64, 0, len(addrs))
			for _, a := range addrs {
				offs = append(offs, sent[a][name])
			}
			sort.Slice(offs, func(i, j int) bool { return offs[i] > offs[j] })
			if len(offs) >= need {
				fmt.Printf("quorum watermark %s offset=%d (need=%d)\n", name, offs[need-1], need)
			}
		}

		if len(sent[addrs[0]]) > 64 {
			for k := range sent[addrs[0]] {
				if _, err := os.Stat(filepath.Join(walDir, k)); err != nil {
					for _, a := range addrs {
						delete(sent[a], k)
					}
				}
			}
		}
		time.Sleep(*interval)
	}
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

func currentFlush(psqlBin, pgURL string) (lsn uint64, walfile string, err error) {
	cmd := exec.Command(psqlBin, pgURL, "-v", "ON_ERROR_STOP=1", "-tAc",
		`SELECT pg_current_wal_flush_lsn()::text || '|' || pg_walfile_name(pg_current_wal_flush_lsn());`)
	out, err := cmd.Output()
	if err != nil {
		return 0, "", err
	}
	line := strings.TrimSpace(string(out))
	parts := strings.Split(line, "|")
	if len(parts) != 2 {
		return 0, "", fmt.Errorf("bad lsn probe output: %q", line)
	}
	lsn, err = parsePGLsn(parts[0])
	if err != nil {
		return 0, "", err
	}
	return lsn, strings.TrimSpace(parts[1]), nil
}

func parsePGLsn(s string) (uint64, error) {
	var hi, lo uint64
	n, err := fmt.Sscanf(strings.TrimSpace(s), "%X/%X", &hi, &lo)
	if err != nil || n != 2 {
		return 0, fmt.Errorf("parse lsn %q", s)
	}
	return hi<<32 | lo, nil
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
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	if err := wal.WriteAuth(conn, token); err != nil {
		return 0, err
	}
	if err := wal.WriteAppendMsg(conn, wal.AppendMsg{
		TenantID: tenant, TimelineID: timeline,
		StartLSN: start, EndLSN: end, Payload: payload,
	}); err != nil {
		return 0, err
	}
	return wal.ReadAck(conn)
}

func lsnFromSeg(name string) uint64 {
	if len(name) != 24 {
		return 0
	}
	var logid, seg uint64
	_, _ = fmt.Sscanf(name[8:16], "%x", &logid)
	_, _ = fmt.Sscanf(name[16:24], "%x", &seg)
	return logid*0x100000000 + seg*segSize
}

func isHex(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
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
