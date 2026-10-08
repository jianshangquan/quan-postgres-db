// walstreamer tails Postgres pg_wal and pushes chunks to safekeeper over TCP (QWAL).
//
// Postgres WAL segment files are always 16 MiB (preallocated / recycled). Size alone
// cannot detect new writes — we track bytes sent per file and, for the current
// segment, only stream up to pg_current_wal_flush_lsn (via -current-lsn or probing
// the file's logical tip from the open segment name + optional helper).
package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"walstreamer/src/wal"
)

const segSize = 16 * 1024 * 1024

func main() {
	pgdata := flag.String("pgdata", env("PGDATA", ""), "Postgres data directory")
	sk := flag.String("safekeeper", env("SAFEKEEPER_ADDR", "127.0.0.1:5454"), "safekeeper host:port")
	tenant := flag.String("tenant", env("TENANT_ID", "default"), "")
	timeline := flag.String("timeline", env("TIMELINE_ID", "main"), "")
	interval := flag.Duration("interval", time.Second, "poll interval")
	psql := flag.String("psql", env("PSQL", "psql"), "psql binary for flush LSN")
	pgURL := flag.String("pgurl", env("PGURL", "postgres://postgres@127.0.0.1:5432/postgres?sslmode=disable"), "libpq URL for LSN probe")
	flag.Parse()
	if *pgdata == "" {
		fail("set -pgdata or PGDATA")
	}
	walDir := filepath.Join(*pgdata, "pg_wal")
	sent := map[string]int64{}
	fmt.Printf("walstreamer: %s → %s (tenant=%s timeline=%s)\n", walDir, *sk, *tenant, *timeline)

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
			// Current open segment: only bytes up to flush LSN are valid (file is always 16MiB).
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
			} else if name == curName && flushLSN == 0 {
				// No LSN probe — skip current file to avoid shipping uninitialized tail.
				continue
			}

			prev := sent[name]
			// Recycled segment: Postgres reused the filename; reset if flush rewound past what we sent.
			if name == curName && flushLSN > 0 {
				segStart := lsnFromSeg(name)
				off := int64(flushLSN - segStart)
				if off >= 0 && off < prev {
					prev = 0
					sent[name] = 0
				}
			}
			if avail <= prev {
				continue
			}

			f, err := os.Open(path)
			if err != nil {
				continue
			}
			if _, err := f.Seek(prev, io.SeekStart); err != nil {
				f.Close()
				continue
			}
			toRead := avail - prev
			chunk, err := io.ReadAll(io.LimitReader(f, toRead))
			f.Close()
			if err != nil || len(chunk) == 0 {
				continue
			}
			startLSN := lsnFromSeg(name) + uint64(prev)
			endLSN := startLSN + uint64(len(chunk))
			ack, err := push(*sk, *tenant, *timeline, startLSN, endLSN, chunk)
			if err != nil {
				fmt.Fprintf(os.Stderr, "push %s: %v\n", name, err)
				continue
			}
			sent[name] = prev + int64(len(chunk))
			fmt.Printf("streamed %s +%d bytes lsn=%d..%d ack=%d\n", name, len(chunk), startLSN, endLSN, ack)
		}
		if len(sent) > 64 {
			for k := range sent {
				if _, err := os.Stat(filepath.Join(walDir, k)); err != nil {
					delete(sent, k)
				}
			}
		}
		time.Sleep(*interval)
	}
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

// parsePGLsn parses "X/Y" into a uint64 LSN.
func parsePGLsn(s string) (uint64, error) {
	var hi, lo uint64
	n, err := fmt.Sscanf(strings.TrimSpace(s), "%X/%X", &hi, &lo)
	if err != nil || n != 2 {
		return 0, fmt.Errorf("parse lsn %q", s)
	}
	return hi<<32 | lo, nil
}

func push(addr, tenant, timeline string, start, end uint64, payload []byte) (uint64, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
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

func fail(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
