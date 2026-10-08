// quansync walks a Postgres PGDATA directory, uploads relation pages via PutPage,
// and uploads a basebackup tar of cluster control files to the pageserver.
package main

import (
	"archive/tar"
	"bytes"
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

func main() {
	pgdata := flag.String("pgdata", os.Getenv("PGDATA"), "Postgres data directory")
	pageserver := flag.String("pageserver", env("PAGESERVER_HTTP", "http://pageserver:8081"), "pageserver HTTP base")
	tenant := flag.String("tenant", env("TENANT_ID", "default"), "tenant id")
	timeline := flag.String("timeline", env("TIMELINE_ID", "main"), "timeline id")
	defaultLSN := uint64(time.Now().UnixNano())
	if v := os.Getenv("QUAN_PAGE_LSN"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil && n > 0 {
			defaultLSN = n
		}
	}
	lsn := flag.Uint64("lsn", defaultLSN, "version LSN for uploaded pages (or QUAN_PAGE_LSN)")
	baseOnly := flag.Bool("base-only", false, "upload basebackup only (no relation page walk; WAL/FPI supplies pages)")
	flag.Parse()
	if *pgdata == "" {
		fail("pgdata required")
	}

	cli := &http.Client{Timeout: 60 * time.Second}
	ensureTimeline(cli, *pageserver, *tenant, *timeline)

	if *baseOnly {
		tarBytes, err := buildBaseTar(*pgdata)
		if err != nil {
			fail(err.Error())
		}
		if err := putBase(cli, *pageserver, *tenant, *timeline, tarBytes); err != nil {
			fail(err.Error())
		}
		fmt.Printf("quansync: basebackup-only (%d bytes)\n", len(tarBytes))
		return
	}

	var uploaded int
	err := filepath.Walk(*pgdata, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			base := info.Name()
			if base == "pg_wal" || base == "pg_stat" || base == "pg_stat_tmp" || base == "pg_logical" || base == "pg_snapshots" || base == "pg_dynshmem" || base == "pg_notify" || base == "pg_replslot" || base == "pg_serial" || base == "pg_subtrans" || base == "pg_tblspc" || base == "pg_commit_ts" {
				if path != *pgdata {
					return filepath.SkipDir
				}
			}
			return nil
		}
		rel := strings.TrimPrefix(path, *pgdata)
		rel = strings.TrimPrefix(rel, string(os.PathSeparator))
		dbOid, filenode, fork, ok := parseRelPath(rel)
		if !ok {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		buf := make([]byte, pageSize)
		var block uint32
		for {
			n, err := io.ReadFull(f, buf)
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				if n == 0 {
					break
				}
				// pad short last page
				for i := n; i < pageSize; i++ {
					buf[i] = 0
				}
			} else if err != nil {
				return err
			}
			pageLSN := *lsn + uint64(block)
			if err := putPage(cli, *pageserver, *tenant, *timeline, dbOid, filenode, fork, block, pageLSN, buf); err != nil {
				return err
			}
			uploaded++
			block++
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
		}
		return nil
	})
	if err != nil {
		fail(err.Error())
	}

	tarBytes, err := buildBaseTar(*pgdata)
	if err != nil {
		fail("base tar: " + err.Error())
	}
	if err := putBase(cli, *pageserver, *tenant, *timeline, tarBytes); err != nil {
		fail("basebackup: " + err.Error())
	}
	fmt.Printf("quansync: uploaded %d pages + basebackup (%d bytes) lsn_base=%d\n", uploaded, len(tarBytes), *lsn)
}

func parseRelPath(rel string) (dbOid, filenode, fork uint32, ok bool) {
	// global/12345 or global/12345.1 or base/16384/12345 or base/16384/12345_fsm
	parts := strings.Split(rel, string(os.PathSeparator))
	if len(parts) < 2 {
		return 0, 0, 0, false
	}
	name := parts[len(parts)-1]
	if strings.Contains(name, ".") && !strings.Contains(name, "_") {
		// mapped files like pg_filenode.map — skip
		if strings.HasSuffix(name, ".map") || strings.HasPrefix(name, "pg_") {
			return 0, 0, 0, false
		}
	}
	switch parts[0] {
	case "global":
		dbOid = 0
	case "base":
		if len(parts) != 3 {
			return 0, 0, 0, false
		}
		db, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			return 0, 0, 0, false
		}
		dbOid = uint32(db)
	default:
		return 0, 0, 0, false
	}
	base := name
	fork = 0
	if i := strings.Index(base, "_"); i >= 0 {
		suffix := base[i+1:]
		base = base[:i]
		switch suffix {
		case "fsm":
			fork = 1
		case "vm":
			fork = 2
		case "init":
			fork = 3
		default:
			return 0, 0, 0, false
		}
	}
	// strip segment suffix .1 .2
	if i := strings.Index(base, "."); i >= 0 {
		base = base[:i]
	}
	fn, err := strconv.ParseUint(base, 10, 32)
	if err != nil {
		return 0, 0, 0, false
	}
	return dbOid, uint32(fn), fork, true
}

func buildBaseTar(pgdata string) ([]byte, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	include := []string{
		"PG_VERSION", "postgresql.conf", "pg_hba.conf", "pg_ident.conf",
		"postgresql.auto.conf",
		"global", "pg_xact", "pg_multixact", "pg_twophase",
	}
	for _, name := range include {
		p := filepath.Join(pgdata, name)
		info, err := os.Stat(p)
		if err != nil {
			continue
		}
		if info.IsDir() {
			_ = filepath.Walk(p, func(path string, fi os.FileInfo, err error) error {
				if err != nil || fi.IsDir() {
					return err
				}
				rel, _ := filepath.Rel(pgdata, path)
				return writeTarFile(tw, path, rel, fi)
			})
		} else {
			if err := writeTarFile(tw, p, name, info); err != nil {
				return nil, err
			}
		}
	}
	// Per-database markers required for restore (PG_VERSION, pg_filenode.map).
	baseDir := filepath.Join(pgdata, "base")
	if entries, err := os.ReadDir(baseDir); err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			for _, name := range []string{"PG_VERSION", "pg_filenode.map"} {
				p := filepath.Join(baseDir, e.Name(), name)
				if fi, err := os.Stat(p); err == nil {
					_ = writeTarFile(tw, p, filepath.Join("base", e.Name(), name), fi)
				}
			}
		}
	}
	if fi, err := os.Stat(filepath.Join(pgdata, "global", "pg_filenode.map")); err == nil {
		_ = writeTarFile(tw, filepath.Join(pgdata, "global", "pg_filenode.map"), "global/pg_filenode.map", fi)
	}
	// Also include empty base/ dir marker via pg_control
	ctrl := filepath.Join(pgdata, "global", "pg_control")
	if fi, err := os.Stat(ctrl); err == nil {
		_ = writeTarFile(tw, ctrl, "global/pg_control", fi)
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeTarFile(tw *tar.Writer, path, name string, fi os.FileInfo) error {
	hdr, err := tar.FileInfoHeader(fi, "")
	if err != nil {
		return err
	}
	hdr.Name = name
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(tw, f)
	return err
}

func addAuth(req *http.Request) {
	if t := os.Getenv("QUAN_STORAGE_TOKEN"); t != "" {
		req.Header.Set("Authorization", "Bearer "+t)
		req.Header.Set("X-Quan-Token", t)
	}
}

func ensureTimeline(cli *http.Client, base, tenant, timeline string) {
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/timeline?tenant=%s&timeline=%s", base, tenant, timeline), nil)
	addAuth(req)
	resp, err := cli.Do(req)
	if err == nil {
		resp.Body.Close()
	}
}

func putPage(cli *http.Client, base, tenant, timeline string, db, rel, fork, block uint32, lsn uint64, page []byte) error {
	url := fmt.Sprintf("%s/v1/page?tenant=%s&timeline=%s&db=%d&rel=%d&fork=%d&block=%d&lsn=%d",
		base, tenant, timeline, db, rel, fork, block, lsn)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(page))
	if err != nil {
		return err
	}
	addAuth(req)
	resp, err := cli.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("put page %s: %s", url, string(b))
	}
	return nil
}

func putBase(cli *http.Client, base, tenant, timeline string, archive []byte) error {
	url := fmt.Sprintf("%s/v1/basebackup?tenant=%s&timeline=%s", base, tenant, timeline)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(archive))
	if err != nil {
		return err
	}
	addAuth(req)
	resp, err := cli.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("put base: %s", string(b))
	}
	return nil
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
