package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"pageserver/src/page"
	"pageserver/src/store"
	"pageserver/src/wal"
)

func main() {
	listen := env("LISTEN_ADDR", ":8081")
	dataDir := env("DATA_DIR", "./data")

	disk, err := store.NewDisk(dataDir)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	svc := page.NewService(disk, 512)
	applier := wal.NewApplier(svc)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "role": "pageserver"})
	})
	mux.HandleFunc("/v1/ingest", ingestHandler(applier))
	mux.HandleFunc("/v1/page", pageHandler(svc))
	mux.HandleFunc("/v1/relsize", relsizeHandler(svc))
	mux.HandleFunc("/v1/timeline", timelineHandler(svc))
	mux.HandleFunc("/v1/pages", pagesHandler(svc))
	mux.HandleFunc("/v1/basebackup", basebackupHandler(disk))

	srv := &http.Server{Addr: listen, Handler: mux}
	go func() {
		log.Printf("pageserver http=%s data=%s", listen, dataDir)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
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
		log.Printf("ingest POST tenant=%s timeline=%s lsn=%d..%d bytes=%d",
			tenant, timeline, startLSN, endLSN, len(body))
		applied, err := applier.Apply(r.Context(), tenant, timeline, startLSN, endLSN, body)
		if err != nil {
			log.Printf("ingest error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		log.Printf("ingest ok applied_lsn=%d", applied)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]uint64{"applied_lsn": applied})
	}
}

func pageHandler(svc *page.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		tenant, timeline := q.Get("tenant"), q.Get("timeline")
		if tenant == "" {
			tenant = "default"
		}
		if timeline == "" {
			timeline = "main"
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

func basebackupHandler(disk *store.Disk) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		tenant, timeline := q.Get("tenant"), q.Get("timeline")
		if tenant == "" {
			tenant = "default"
		}
		if timeline == "" {
			timeline = "main"
		}
		switch r.Method {
		case http.MethodPut:
			body, err := io.ReadAll(io.LimitReader(r.Body, 256<<20))
			if err != nil || len(body) == 0 {
				http.Error(w, "body required", http.StatusBadRequest)
				return
			}
			if err := disk.PutBasebackup(r.Context(), tenant, timeline, body); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
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

func parseU64(s string) uint64 {
	n, _ := strconv.ParseUint(s, 10, 64)
	return n
}

func parseU32(s string) uint32 {
	n, _ := strconv.ParseUint(s, 10, 32)
	return uint32(n)
}
