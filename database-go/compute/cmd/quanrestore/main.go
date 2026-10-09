// quanrestore materializes a slim PGDATA bootstrap from pageserver basebackup.
// Relation pages are served via GetPage (smgr); optional -full still materializes snapshot.
package main

import (
	"archive/tar"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const pageSize = 8192

type pageRef struct {
	DBOid       uint32 `json:"DBOid"`
	RelFilenode uint32 `json:"RelFilenode"`
	Fork        uint32 `json:"Fork"`
	Block       uint32 `json:"Block"`
	LSN         uint64 `json:"LSN"`
}

func main() {
	pgdata := flag.String("pgdata", os.Getenv("PGDATA"), "target PGDATA")
	pageserver := flag.String("pageserver", env("PAGESERVER_HTTP", "http://pageserver:8081"), "")
	safekeeper := flag.String("safekeeper", env("SAFEKEEPER_HTTP", "http://safekeeper-1:8080"), "")
	tenant := flag.String("tenant", env("TENANT_ID", "default"), "")
	timeline := flag.String("timeline", env("TIMELINE_ID", "main"), "")
	full := flag.Bool("full", false, "materialize all pages from /v1/snapshot (legacy)")
	doWAL := flag.Bool("wal", true, "materialize pg_wal segments from safekeeper")
	pgTL := flag.Uint("pg-timeline", 1, "Postgres WAL timeline id for segment filenames")
	flag.Parse()
	if *pgdata == "" {
		fail("pgdata required")
	}
	cli := tlsHTTPClient(10 * time.Minute)
	psBase := firstPageserverBase(*pageserver)
	if psBase == "" {
		fail("set PAGESERVER_HTTP or QUAN_PAGESERVER_HTTP_URLS")
	}

	st, err := timelineStatus(cli, psBase, *tenant, *timeline)
	if err != nil {
		fail("timeline status: " + err.Error())
	}
	if !st {
		// Allow basebackup-only timelines (page_count may still be 0).
		if _, err := getBytes(cli, fmt.Sprintf("%s/v1/basebackup?tenant=%s&timeline=%s", psBase, *tenant, *timeline)); err != nil {
			fail("timeline not found on pageserver; cannot restore")
		}
	}

	if err := os.MkdirAll(*pgdata, 0o700); err != nil {
		fail(err.Error())
	}

	base, err := getBytes(cli, fmt.Sprintf("%s/v1/basebackup?tenant=%s&timeline=%s", psBase, *tenant, *timeline))
	if err != nil {
		fail("basebackup: " + err.Error())
	}
	if err := extractTar(base, *pgdata); err != nil {
		fail("extract base: " + err.Error())
	}

	for _, d := range []string{
		"pg_wal", "pg_wal/archive_status",
		"pg_stat", "pg_stat_tmp",
		"pg_logical", "pg_logical/snapshots", "pg_logical/mappings",
		"pg_snapshots", "pg_dynshmem", "pg_notify", "pg_replslot",
		"pg_serial", "pg_subtrans", "pg_tblspc", "pg_commit_ts", "pg_twophase",
		"base", "global",
	} {
		_ = os.MkdirAll(filepath.Join(*pgdata, d), 0o700)
	}
	ensurePGVersion(*pgdata)

	refs, _ := listPages(cli, psBase, *tenant, *timeline)
	nFiles := touchRelationStubs(*pgdata, refs)

	if *full {
		snap, err := getBytes(cli, fmt.Sprintf("%s/v1/snapshot?tenant=%s&timeline=%s", psBase, *tenant, *timeline))
		if err != nil {
			fail("snapshot: " + err.Error())
		}
		n, err := materializePages(snap, *pgdata)
		if err != nil {
			fail("pages: " + err.Error())
		}
		fmt.Printf("quanrestore: full materialize %d pages (%d stubs) into %s\n", n, nFiles, *pgdata)
		return
	}

	fmt.Printf("quanrestore: slim bootstrap (GetPage-native), %d relation stubs into %s\n", nFiles, *pgdata)

	if *doWAL {
		seeds := splitSeeds(env("SAFEKEEPER_HTTP_SEEDS", *safekeeper))
		n, tip, err := restoreWAL(cli, seeds, *tenant, *timeline, *pgdata, uint32(*pgTL))
		if err != nil {
			fmt.Fprintf(os.Stderr, "quanrestore: wal restore failed: %v\n", err)
			os.Exit(2)
		}
		fmt.Printf("quanrestore: wal restore %d segments (tip_lsn=%d) into %s/pg_wal\n", n, tip, *pgdata)
		if n == 0 {
			os.Exit(3) // signal entrypoint to fall back to pg_resetwal
		}
	}
}

func touchRelationStubs(pgdata string, refs []pageRef) int {
	type key struct{ db, rel, fork uint32 }
	seen := map[key]uint32{} // max block
	for _, r := range refs {
		if r.RelFilenode == 0 || r.Block > 1024*1024 {
			continue
		}
		k := key{r.DBOid, r.RelFilenode, r.Fork}
		if prev, ok := seen[k]; !ok || r.Block > prev {
			seen[k] = r.Block
		}
	}
	for k, maxBlk := range seen {
		path := relPath(pgdata, k.db, k.rel, k.fork)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			continue
		}
		// Zero-length stub: GetPage-native mdread never needs local bytes.
		// Size hint: (maxBlk+1)*BLCKSZ so local mdnblocks fallback is sane if remote misses.
		size := int64(maxBlk+1) * pageSize
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			continue
		}
		_ = f.Truncate(size)
		f.Close()
	}
	return len(seen)
}

func ensurePGVersion(pgdata string) {
	root := filepath.Join(pgdata, "PG_VERSION")
	ver := "16"
	if b, err := os.ReadFile(root); err == nil && len(bytes.TrimSpace(b)) > 0 {
		ver = string(bytes.TrimSpace(b))
	} else {
		_ = os.WriteFile(root, []byte(ver+"\n"), 0o600)
	}
	base := filepath.Join(pgdata, "base")
	var donor []byte
	for _, cand := range []string{
		filepath.Join(base, "1", "pg_filenode.map"),
		filepath.Join(base, "4", "pg_filenode.map"),
		filepath.Join(base, "5", "pg_filenode.map"),
		filepath.Join(pgdata, "global", "pg_filenode.map"),
	} {
		if b, err := os.ReadFile(cand); err == nil && len(b) > 0 {
			donor = b
			break
		}
	}
	globalMap := filepath.Join(pgdata, "global", "pg_filenode.map")
	if _, err := os.Stat(globalMap); err != nil && len(donor) > 0 {
		_ = os.WriteFile(globalMap, donor, 0o600)
	}
	entries, err := os.ReadDir(base)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(base, e.Name())
		verPath := filepath.Join(dir, "PG_VERSION")
		if _, err := os.Stat(verPath); err != nil {
			_ = os.WriteFile(verPath, []byte(ver+"\n"), 0o600)
		}
		mapPath := filepath.Join(dir, "pg_filenode.map")
		if _, err := os.Stat(mapPath); err != nil && len(donor) > 0 {
			_ = os.WriteFile(mapPath, donor, 0o600)
		}
	}
}

func materializePages(archive []byte, pgdata string) (int, error) {
	tr := tar.NewReader(bytes.NewReader(archive))
	type fileKey struct {
		db, rel, fork uint32
	}
	files := map[fileKey]map[uint32][]byte{}
	count := 0
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return count, err
		}
		parts := strings.Split(hdr.Name, "/")
		if len(parts) != 4 {
			continue
		}
		db := u32(parts[0])
		rel := u32(parts[1])
		fork := u32(parts[2])
		block := u32(parts[3])
		page, err := io.ReadAll(tr)
		if err != nil || len(page) != pageSize {
			continue
		}
		k := fileKey{db, rel, fork}
		if files[k] == nil {
			files[k] = map[uint32][]byte{}
		}
		files[k][block] = page
		count++
	}
	for k, blocks := range files {
		path := relPath(pgdata, k.db, k.rel, k.fork)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return count, err
		}
		var maxBlk uint32
		for b := range blocks {
			if b > maxBlk {
				maxBlk = b
			}
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			return count, err
		}
		zeros := make([]byte, pageSize)
		for b := uint32(0); b <= maxBlk; b++ {
			p := blocks[b]
			if p == nil {
				p = zeros
			}
			if _, err := f.Write(p); err != nil {
				f.Close()
				return count, err
			}
		}
		f.Close()
	}
	return count, nil
}

func relPath(pgdata string, db, rel, fork uint32) string {
	name := fmt.Sprintf("%d", rel)
	switch fork {
	case 1:
		name += "_fsm"
	case 2:
		name += "_vm"
	case 3:
		name += "_init"
	}
	if db == 0 {
		return filepath.Join(pgdata, "global", name)
	}
	return filepath.Join(pgdata, "base", fmt.Sprintf("%d", db), name)
}

func timelineStatus(cli *http.Client, base, tenant, timeline string) (bool, error) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/timeline?tenant=%s&timeline=%s", base, tenant, timeline), nil)
	if err != nil {
		return false, err
	}
	addAuth(req)
	resp, err := cli.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	var m map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return false, err
	}
	ex, _ := m["exists"].(bool)
	pc, _ := m["page_count"].(float64)
	return ex && pc > 0, nil
}

func listPages(cli *http.Client, base, tenant, timeline string) ([]pageRef, error) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/pages?tenant=%s&timeline=%s", base, tenant, timeline), nil)
	if err != nil {
		return nil, err
	}
	addAuth(req)
	resp, err := cli.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var refs []pageRef
	if err := json.NewDecoder(resp.Body).Decode(&refs); err != nil {
		return nil, err
	}
	return refs, nil
}

func addAuth(req *http.Request) {
	if t := os.Getenv("QUAN_STORAGE_TOKEN"); t != "" {
		req.Header.Set("Authorization", "Bearer "+t)
		req.Header.Set("X-Quan-Token", t)
	}
}

func getBytes(cli *http.Client, url string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	addAuth(req)
	resp, err := cli.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(b))
	}
	return io.ReadAll(resp.Body)
}

func extractTar(archive []byte, dest string) error {
	tr := tar.NewReader(bytes.NewReader(archive))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		target := filepath.Join(dest, hdr.Name)
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			return err
		}
		f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			return err
		}
		if _, err := io.Copy(f, tr); err != nil {
			f.Close()
			return err
		}
		f.Close()
	}
}

func u32(s string) uint32 {
	v, _ := strconv.ParseUint(s, 10, 32)
	return uint32(v)
}

func firstPageserverBase(flagVal string) string {
	if multi := os.Getenv("QUAN_PAGESERVER_HTTP_URLS"); multi != "" {
		for _, p := range strings.Split(multi, ",") {
			p = strings.TrimSpace(strings.TrimRight(p, "/"))
			if p != "" {
				return p
			}
		}
	}
	return strings.TrimSpace(strings.TrimRight(flagVal, "/"))
}

func tlsHTTPClient(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	caPath := env("QUAN_TLS_CA", env("TLS_CA_FILE", ""))
	if caPath != "" {
		if pem, err := os.ReadFile(caPath); err == nil {
			pool := x509.NewCertPool()
			if pool.AppendCertsFromPEM(pem) {
				transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
			}
		}
	}
	return &http.Client{Timeout: timeout, Transport: transport}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func splitSeeds(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

const pgSegSize = 16 * 1024 * 1024

func pgWALFileName(pgTimeline uint32, segno uint64) string {
	start := segno * pgSegSize
	logid := start >> 32
	seg := (start & 0xffffffff) / pgSegSize
	return fmt.Sprintf("%08X%08X%08X", pgTimeline, logid, seg)
}

func restoreWAL(cli *http.Client, seeds []string, tenant, timeline, pgdata string, pgTimeline uint32) (n int, tip uint64, err error) {
	if len(seeds) == 0 {
		return 0, 0, fmt.Errorf("no safekeeper seeds")
	}
	var segnos []uint64
	var listErr error
	for _, base := range seeds {
		segnos, listErr = listWALSegnos(cli, base, tenant, timeline)
		if listErr == nil {
			break
		}
	}
	if listErr != nil {
		return 0, 0, listErr
	}
	walDir := filepath.Join(pgdata, "pg_wal")
	_ = os.MkdirAll(filepath.Join(walDir, "archive_status"), 0o700)
	for _, segno := range segnos {
		var payload []byte
		var startLSN, endLSN uint64
		var fetchErr error
		for _, base := range seeds {
			payload, startLSN, endLSN, fetchErr = fetchWALSeg(cli, base, tenant, timeline, segno)
			if fetchErr == nil && len(payload) > 0 {
				break
			}
		}
		if fetchErr != nil || len(payload) == 0 {
			continue
		}
		name := pgWALFileName(pgTimeline, segno)
		path := filepath.Join(walDir, name)
		if len(payload) < pgSegSize {
			padded := make([]byte, pgSegSize)
			copy(padded, payload)
			payload = padded
		}
		if err := os.WriteFile(path, payload, 0o600); err != nil {
			return n, tip, err
		}
		n++
		if endLSN > tip {
			tip = endLSN
		}
		if startLSN > tip {
			tip = startLSN
		}
	}
	return n, tip, nil
}

func listWALSegnos(cli *http.Client, base, tenant, timeline string) ([]uint64, error) {
	url := fmt.Sprintf("%s/wal?tenant=%s&timeline=%s&list=1", base, tenant, timeline)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	addAuth(req)
	resp, err := cli.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list wal %s: %s", url, string(b))
	}
	var m struct {
		Segnos []uint64 `json:"segnos"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, err
	}
	return m.Segnos, nil
}

func fetchWALSeg(cli *http.Client, base, tenant, timeline string, segno uint64) (payload []byte, start, end uint64, err error) {
	url := fmt.Sprintf("%s/wal?tenant=%s&timeline=%s&segno=%d", base, tenant, timeline, segno)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, 0, err
	}
	addAuth(req)
	resp, err := cli.Do(req)
	if err != nil {
		return nil, 0, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return nil, 0, 0, fmt.Errorf("get wal: %s", string(b))
	}
	start, _ = strconv.ParseUint(resp.Header.Get("X-Start-LSN"), 10, 64)
	end, _ = strconv.ParseUint(resp.Header.Get("X-End-LSN"), 10, 64)
	payload, err = io.ReadAll(resp.Body)
	return payload, start, end, err
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
