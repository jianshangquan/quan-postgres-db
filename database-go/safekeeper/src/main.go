package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"safekeeper/src/wal"
)

func main() {
	listen := env("LISTEN_ADDR", ":5454")
	httpAddr := env("HTTP_ADDR", ":8080")
	dataDir := env("DATA_DIR", "./data")
	token := env("QUAN_STORAGE_TOKEN", "")
	if token == "" {
		log.Fatal("QUAN_STORAGE_TOKEN is required")
	}
	peers := splitCSV(env("SAFEKEEPER_PEERS", ""))

	store, err := wal.NewWal(dataDir)
	if err != nil {
		log.Fatalf("wal: %v", err)
	}

	ln, err := listenTCP(listen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	tlsOn := os.Getenv("TLS_CERT_FILE") != "" && os.Getenv("TLS_KEY_FILE") != ""
	log.Printf("safekeeper tcp=%s http=%s tls=%v data=%s peers=%v", listen, httpAddr, tlsOn, dataDir, peers)

	go acceptLoop(ln, store, token)
	go serveHTTP(httpAddr, store, token, dataDir)
	if len(peers) > 0 {
		go peerPullLoop(store, peers, token)
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	_ = ln.Close()
}

func serveHTTP(addr string, store *wal.Wal, token, dataDir string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		probe := filepath.Join(dataDir, ".healthz_probe")
		if err := os.WriteFile(probe, []byte("ok"), 0o644); err != nil {
			http.Error(w, "data dir not writable", http.StatusServiceUnavailable)
			return
		}
		_ = os.Remove(probe)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok": true, "role": "safekeeper", "flush_lsn": store.MaxFlushLSN(),
		})
	})
	mux.HandleFunc("/wal", requireAuth(token, walHTTP(store)))
	mux.HandleFunc("/metrics", requireAuth(token, metricsHTTP(store)))
	log.Printf("safekeeper http listen %s", addr)
	if err := listenHTTP(addr, mux); err != nil {
		log.Printf("safekeeper http: %v", err)
	}
}

func metricsHTTP(store *wal.Wal) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "# HELP safekeeper_append_total WAL appends\n")
		fmt.Fprintf(w, "# TYPE safekeeper_append_total counter\n")
		fmt.Fprintf(w, "safekeeper_append_total %d\n", store.Appends.Load())
		fmt.Fprintf(w, "# HELP safekeeper_flush_lsn Highest durable flush LSN\n")
		fmt.Fprintf(w, "# TYPE safekeeper_flush_lsn gauge\n")
		fmt.Fprintf(w, "safekeeper_flush_lsn %d\n", store.MaxFlushLSN())
	}
}

func walHTTP(store *wal.Wal) http.HandlerFunc {
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
		if err := wal.ValidateID(tenant); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := wal.ValidateID(timeline); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if q.Get("list") == "1" {
			segnos, err := store.ListSegments(tenant, timeline)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			flush, _ := store.FlushLSN(tenant, timeline)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"segnos": segnos, "flush_lsn": flush})
			return
		}
		segno, err := strconv.ParseUint(q.Get("segno"), 10, 64)
		if err != nil {
			http.Error(w, "segno required", http.StatusBadRequest)
			return
		}
		frame, err := store.GetFrame(tenant, timeline, segno)
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("X-Start-LSN", strconv.FormatUint(frame.StartLSN, 10))
		w.Header().Set("X-End-LSN", strconv.FormatUint(frame.EndLSN, 10))
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(frame.Payload)
	}
}

func acceptLoop(ln net.Listener, store *wal.Wal, token string) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			return
		}
		go serveConn(conn, store, token)
	}
}

func serveConn(conn net.Conn, store *wal.Wal, token string) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Minute))

	got, err := wal.ReadAuth(conn)
	if err != nil {
		log.Printf("auth %s: %v", conn.RemoteAddr(), err)
		return
	}
	if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
		log.Printf("auth %s: bad token", conn.RemoteAddr())
		return
	}

	for {
		msg, err := wal.ReadAppendMsg(conn)
		if err != nil {
			if err != io.EOF {
				log.Printf("wal read %s: %v", conn.RemoteAddr(), err)
			}
			return
		}
		if err := store.CreateTimeline(msg.TenantID, msg.TimelineID); err != nil {
			log.Printf("create timeline: %v", err)
			return
		}
		flush, err := store.Append(msg.TenantID, msg.TimelineID, msg.StartLSN, msg.EndLSN, msg.Payload)
		if err != nil {
			log.Printf("append: %v", err)
			return
		}
		if err := wal.WriteAck(conn, flush); err != nil {
			log.Printf("ack: %v", err)
			return
		}
		log.Printf("wal append tenant=%s timeline=%s lsn=%d..%d ack=%d bytes=%d",
			msg.TenantID, msg.TimelineID, msg.StartLSN, msg.EndLSN, flush, len(msg.Payload))
		_ = conn.SetDeadline(time.Now().Add(5 * time.Minute))
	}
}

func peerPullLoop(store *wal.Wal, peers []string, token string) {
	cli := newHTTPClient(15 * time.Second)
	for {
		time.Sleep(5 * time.Second)
		tls, err := store.ListTimelines()
		if err != nil {
			continue
		}
		if len(tls) == 0 {
			tls = [][2]string{{"default", "main"}}
		}
		for _, pair := range tls {
			tenant, timeline := pair[0], pair[1]
			local, _ := store.ListSegments(tenant, timeline)
			have := map[uint64]bool{}
			for _, s := range local {
				have[s] = true
			}
			for _, peer := range peers {
				base := strings.TrimRight(peer, "/")
				req, err := http.NewRequest(http.MethodGet,
					fmt.Sprintf("%s/wal?tenant=%s&timeline=%s&list=1", base, tenant, timeline), nil)
				if err != nil {
					continue
				}
				req.Header.Set("Authorization", "Bearer "+token)
				resp, err := cli.Do(req)
				if err != nil {
					continue
				}
				var body struct {
					Segnos []uint64 `json:"segnos"`
				}
				_ = json.NewDecoder(resp.Body).Decode(&body)
				resp.Body.Close()
				if resp.StatusCode != 200 {
					continue
				}
				for _, segno := range body.Segnos {
					if have[segno] {
						continue
					}
					if err := pullSegment(cli, store, base, token, tenant, timeline, segno); err != nil {
						log.Printf("peer pull %s segno=%d: %v", peer, segno, err)
						continue
					}
					have[segno] = true
					log.Printf("peer pull tenant=%s timeline=%s segno=%d from %s", tenant, timeline, segno, peer)
				}
			}
		}
	}
}

func pullSegment(cli *http.Client, store *wal.Wal, base, token, tenant, timeline string, segno uint64) error {
	req, err := http.NewRequest(http.MethodGet,
		fmt.Sprintf("%s/wal?tenant=%s&timeline=%s&segno=%d", base, tenant, timeline, segno), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := cli.Do(req.WithContext(context.Background()))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return err
	}
	start, _ := strconv.ParseUint(resp.Header.Get("X-Start-LSN"), 10, 64)
	end, _ := strconv.ParseUint(resp.Header.Get("X-End-LSN"), 10, 64)
	if start == 0 {
		start = segno * wal.PGSegmentSize
	}
	if end == 0 {
		end = start + uint64(len(payload))
	}
	_ = store.CreateTimeline(tenant, timeline)
	_, err = store.Append(tenant, timeline, start, end, payload)
	return err
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
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
