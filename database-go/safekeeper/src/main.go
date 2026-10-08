package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"safekeeper/src/wal"
)

func main() {
	listen := env("LISTEN_ADDR", ":5454") // TCP QWAL from compute / walstreamer
	httpAddr := env("HTTP_ADDR", ":8080") // restore / health (read path)
	dataDir := env("DATA_DIR", "./data")
	pageserver := env("PAGESERVER_URL", "http://127.0.0.1:8081")

	store, err := wal.NewWal(dataDir)
	if err != nil {
		log.Fatalf("wal: %v", err)
	}

	ln, err := net.Listen("tcp", listen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("safekeeper tcp=%s http=%s data=%s pageserver=%s", listen, httpAddr, dataDir, pageserver)

	go acceptLoop(ln, store, pageserver)
	go serveHTTP(httpAddr, store)

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	_ = ln.Close()
}

func serveHTTP(addr string, store *wal.Wal) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "role": "safekeeper"})
	})
	mux.HandleFunc("/wal", walHTTP(store))
	log.Printf("safekeeper http listen %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("safekeeper http: %v", err)
	}
}

// walHTTP serves raw Postgres WAL payloads for restore_command / quanrestore.
// GET /wal?tenant=&timeline=&list=1 → {"segnos":[...]}
// GET /wal?tenant=&timeline=&segno=N → payload bytes + X-Start-LSN / X-End-LSN
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
		if q.Get("list") == "1" {
			segnos, err := store.ListSegments(tenant, timeline)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"segnos": segnos})
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

func acceptLoop(ln net.Listener, store *wal.Wal, pageserverURL string) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			return
		}
		go serveConn(conn, store, pageserverURL)
	}
}

func serveConn(conn net.Conn, store *wal.Wal, pageserverURL string) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Minute))

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
		if pageserverURL != "" {
			go forwardIngest(pageserverURL, msg.TenantID, msg.TimelineID, msg.StartLSN, msg.EndLSN, msg.Payload)
		}
		_ = conn.SetDeadline(time.Now().Add(5 * time.Minute))
	}
}

func forwardIngest(base, tenant, timeline string, startLSN, endLSN uint64, payload []byte) {
	u, err := url.Parse(base)
	if err != nil {
		return
	}
	u.Path = "/v1/ingest"
	q := u.Query()
	q.Set("tenant", tenant)
	q.Set("timeline", timeline)
	q.Set("start_lsn", strconv.FormatUint(startLSN, 10))
	q.Set("end_lsn", strconv.FormatUint(endLSN, 10))
	u.RawQuery = q.Encode()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(payload))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("ingest fanout: %v", err)
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
