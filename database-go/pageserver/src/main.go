package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"pageserver/src/page"
	"pageserver/src/store"
	"pageserver/src/wal"
)

func main() {
	listen := env("LISTEN_ADDR", ":8081")
	dataDir := env("DATA_DIR", "./data")
	token := env("QUAN_STORAGE_TOKEN", "")
	if token == "" {
		log.Fatal("QUAN_STORAGE_TOKEN is required")
	}
	cachePages := envInt("CACHE_PAGES", 8192)
	replica := env("PAGESERVER_ROLE", "primary") == "replica"
	peers := resolvePeerURLs()
	skURLs := splitCSV(env("SAFEKEEPER_HTTP_URLS", ""))
	tenant := env("TENANT_ID", "default")
	timeline := env("TIMELINE_ID", "main")
	gcEvery := envDuration("GC_INTERVAL", time.Minute)
	lagLimit := envDuration("HEALTH_LAG_LIMIT", 30*time.Second)

	disk, err := store.NewDisk(dataDir)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	svc := page.NewService(disk, cachePages)
	applier := wal.NewApplier(svc)
	state := &pullState{}
	peerHTTPClient = newHTTPClient(8 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if len(skURLs) > 0 {
		go runWALPull(ctx, skURLs, token, tenant, timeline, applier, disk, state)
	}
	go func() {
		t := time.NewTicker(gcEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if n := disk.CompactLoose(); n > 0 {
					log.Printf("compact packed %d loose page versions", n)
				}
				n := disk.GCKeepNewest()
				if n > 0 {
					log.Printf("gc deleted %d old page versions", n)
				}
			}
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthHandler(dataDir, state, lagLimit, len(skURLs) > 0))
	mux.HandleFunc("/metrics", requireAuth(token, metricsHandler(svc, disk, state)))
	mux.HandleFunc("/v1/ingest", requireAuth(token, ingestHandler(applier)))
	mux.HandleFunc("/v1/page", requireAuth(token, pageHandler(svc, token, peers, replica)))
	mux.HandleFunc("/v1/relsize", requireAuth(token, relsizeHandler(svc)))
	mux.HandleFunc("/v1/timeline", requireAuth(token, timelineHandler(svc)))
	mux.HandleFunc("/v1/pages", requireAuth(token, pagesHandler(svc)))
	mux.HandleFunc("/v1/basebackup", requireAuth(token, basebackupHandler(disk, token, peers, replica)))

	srv := &http.Server{Addr: listen, Handler: mux}
	go func() {
		tlsOn := os.Getenv("TLS_CERT_FILE") != "" && os.Getenv("TLS_KEY_FILE") != ""
		log.Printf("pageserver http=%s tls=%v data=%s role=%s cache=%d peers=%v sk=%v",
			listen, tlsOn, dataDir, env("PAGESERVER_ROLE", "primary"), cachePages, peers, skURLs)
		if err := serveHTTPServer(srv); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	cancel()
	shCtx, shCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shCancel()
	_ = srv.Shutdown(shCtx)
}

func healthHandler(dataDir string, state *pullState, lagLimit time.Duration, hasSK bool) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		probe := filepath.Join(dataDir, ".healthz_probe")
		if err := os.WriteFile(probe, []byte("ok"), 0o644); err != nil {
			http.Error(w, "data dir not writable", http.StatusServiceUnavailable)
			return
		}
		_ = os.Remove(probe)

		applied := state.Applied()
		skFlush := state.SKFlush()
		status := http.StatusOK
		if hasSK && skFlush > applied {
			since := state.laggingSince.Load()
			if since > 0 && time.Since(time.Unix(0, since)) > lagLimit {
				status = http.StatusServiceUnavailable
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok": status == 200, "role": "pageserver",
			"applied_lsn": applied, "safekeeper_flush_lsn": skFlush,
		})
	}
}

func metricsHandler(svc *page.Service, disk *store.Disk, state *pullState) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		c := svc.Cache()
		fmt.Fprintf(w, "# HELP pageserver_applied_lsn Applied WAL LSN\n")
		fmt.Fprintf(w, "# TYPE pageserver_applied_lsn gauge\n")
		fmt.Fprintf(w, "pageserver_applied_lsn %d\n", state.Applied())
		fmt.Fprintf(w, "# HELP pageserver_safekeeper_flush_lsn Safekeeper flush LSN\n")
		fmt.Fprintf(w, "# TYPE pageserver_safekeeper_flush_lsn gauge\n")
		fmt.Fprintf(w, "pageserver_safekeeper_flush_lsn %d\n", state.SKFlush())
		lag := uint64(0)
		if state.SKFlush() > state.Applied() {
			lag = state.SKFlush() - state.Applied()
		}
		fmt.Fprintf(w, "# HELP pageserver_wal_lag_bytes Approx LSN lag\n")
		fmt.Fprintf(w, "# TYPE pageserver_wal_lag_bytes gauge\n")
		fmt.Fprintf(w, "pageserver_wal_lag_bytes %d\n", lag)
		fmt.Fprintf(w, "# HELP pageserver_gc_deleted_total GC deleted page versions\n")
		fmt.Fprintf(w, "# TYPE pageserver_gc_deleted_total counter\n")
		fmt.Fprintf(w, "pageserver_gc_deleted_total %d\n", disk.GCDeleted.Load())
		fmt.Fprintf(w, "# HELP pageserver_cache_hits_total Cache hits\n")
		fmt.Fprintf(w, "# TYPE pageserver_cache_hits_total counter\n")
		fmt.Fprintf(w, "pageserver_cache_hits_total %d\n", c.Hits)
		fmt.Fprintf(w, "# HELP pageserver_cache_misses_total Cache misses\n")
		fmt.Fprintf(w, "# TYPE pageserver_cache_misses_total counter\n")
		fmt.Fprintf(w, "pageserver_cache_misses_total %d\n", c.Miss)
	}
}

func ingestHandler(applier *wal.Applier) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		tenant, timeline := q.Get("tenant"), q.Get("timeline")
		if tenant == "" {
			tenant = "default"
		}
		if timeline == "" {
			timeline = "main"
		}
		if err := store.ValidateID(tenant); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := store.ValidateID(timeline); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		startLSN := parseU64(q.Get("start_lsn"))
		endLSN := parseU64(q.Get("end_lsn"))
		body, err := io.ReadAll(io.LimitReader(r.Body, 64<<20))
		if err != nil || len(body) == 0 {
			http.Error(w, "body required", http.StatusBadRequest)
			return
		}
		if endLSN == 0 {
			endLSN = startLSN + uint64(len(body))
		}
		applied, err := applier.Apply(r.Context(), tenant, timeline, startLSN, endLSN, body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]uint64{"applied_lsn": applied})
	}
}

func pageHandler(svc *page.Service, token string, peers []string, replica bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		tenant, timeline := q.Get("tenant"), q.Get("timeline")
		if tenant == "" {
			tenant = "default"
		}
		if timeline == "" {
			timeline = "main"
		}
		if err := store.ValidateID(tenant); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := store.ValidateID(timeline); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		dbOid := parseU32(q.Get("db"))
		rel := parseU32(q.Get("rel"))
		fork := parseU32(q.Get("fork"))
		block := parseU32(q.Get("block"))
		lsn := parseU64(q.Get("lsn"))

		switch r.Method {
		case http.MethodGet:
			req := lsn
			if req == 0 {
				req = ^uint64(0)
			}
			pageBytes, gotLSN, err := svc.GetPage(r.Context(), tenant, timeline, dbOid, rel, fork, block, req)
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			w.Header().Set("X-Page-LSN", strconv.FormatUint(gotLSN, 10))
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(pageBytes)
		case http.MethodPut:
			if replica && r.Header.Get("X-Quan-Peer") != "1" {
				http.Error(w, "replica rejects external PUT", http.StatusForbidden)
				return
			}
			body, err := io.ReadAll(io.LimitReader(r.Body, store.PageSize+1))
			if err != nil || len(body) != store.PageSize {
				http.Error(w, "body must be 8192 bytes", http.StatusBadRequest)
				return
			}
			if lsn == 0 {
				lsn = 1
			}
			if err := svc.PutPage(r.Context(), tenant, timeline, dbOid, rel, fork, block, lsn, body); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if !replica && len(peers) > 0 {
				if err := fanoutPageToPeers(peers, token, tenant, timeline, dbOid, rel, fork, block, lsn, body); err != nil {
					log.Printf("peer fanout page failed: %v", err)
					http.Error(w, "peer fanout failed: "+err.Error(), http.StatusServiceUnavailable)
					return
				}
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		default:
			http.Error(w, "GET or PUT", http.StatusMethodNotAllowed)
		}
	}
}

func relsizeHandler(svc *page.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		tenant, timeline := q.Get("tenant"), q.Get("timeline")
		if tenant == "" {
			tenant = "default"
		}
		if timeline == "" {
			timeline = "main"
		}
		lsn := parseU64(q.Get("lsn"))
		if lsn == 0 {
			lsn = ^uint64(0)
		}
		n, err := svc.RelSize(r.Context(), tenant, timeline, parseU32(q.Get("db")), parseU32(q.Get("rel")), parseU32(q.Get("fork")), lsn)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]uint32{"nblocks": n})
	}
}

func pagesHandler(svc *page.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "GET only", http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		tenant, timeline := q.Get("tenant"), q.Get("timeline")
		if tenant == "" {
			tenant = "default"
		}
		if timeline == "" {
			timeline = "main"
		}
		lsn := parseU64(q.Get("lsn"))
		if lsn == 0 {
			lsn = ^uint64(0)
		}
		refs, err := svc.Store().ListPages(r.Context(), tenant, timeline, lsn)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(refs)
	}
}

func basebackupHandler(disk *store.Disk, token string, peers []string, replica bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		tenant, timeline := q.Get("tenant"), q.Get("timeline")
		if tenant == "" {
			tenant = "default"
		}
		if timeline == "" {
			timeline = "main"
		}
		if err := store.ValidateID(tenant); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := store.ValidateID(timeline); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		switch r.Method {
		case http.MethodPut:
			if replica && r.Header.Get("X-Quan-Peer") != "1" {
				http.Error(w, "replica rejects external PUT", http.StatusForbidden)
				return
			}
			body, err := io.ReadAll(io.LimitReader(r.Body, 256<<20))
			if err != nil || len(body) == 0 {
				http.Error(w, "body required", http.StatusBadRequest)
				return
			}
			if err := disk.PutBasebackup(r.Context(), tenant, timeline, body); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if !replica && len(peers) > 0 {
				if err := fanoutBaseToPeers(peers, token, tenant, timeline, body); err != nil {
					log.Printf("peer fanout basebackup failed: %v", err)
					http.Error(w, "peer fanout failed: "+err.Error(), http.StatusServiceUnavailable)
					return
				}
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		case http.MethodGet:
			b, err := disk.GetBasebackup(r.Context(), tenant, timeline)
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/x-tar")
			_, _ = w.Write(b)
		default:
			http.Error(w, "GET or PUT", http.StatusMethodNotAllowed)
		}
	}
}

func timelineHandler(svc *page.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		tenant, timeline := q.Get("tenant"), q.Get("timeline")
		if tenant == "" {
			tenant = "default"
		}
		if timeline == "" {
			timeline = "main"
		}
		if err := store.ValidateID(tenant); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := store.ValidateID(timeline); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		switch r.Method {
		case http.MethodPost:
			if err := svc.EnsureTimeline(r.Context(), tenant, timeline); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		case http.MethodGet:
			exists, last, count, err := svc.TimelineStatus(r.Context(), tenant, timeline)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"exists": exists, "tenant_id": tenant, "timeline_id": timeline,
				"last_lsn": last, "page_count": count,
			})
		default:
			http.Error(w, "GET or POST", http.StatusMethodNotAllowed)
		}
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func parseU64(s string) uint64 {
	n, _ := strconv.ParseUint(s, 10, 64)
	return n
}

func parseU32(s string) uint32 {
	n, _ := strconv.ParseUint(s, 10, 32)
	return uint32(n)
}
