package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"pageserver/src/store"
	"pageserver/src/wal"
)

type pullState struct {
	appliedLSN   atomic.Uint64
	skFlushLSN   atomic.Uint64
	lastCatchup  atomic.Int64 // unix nano when applied caught sk flush
	laggingSince atomic.Int64
}

func (p *pullState) Applied() uint64 { return p.appliedLSN.Load() }
func (p *pullState) SKFlush() uint64 { return p.skFlushLSN.Load() }

func runWALPull(ctx context.Context, urls []string, token, tenant, timeline string, applier *wal.Applier, disk *store.Disk, state *pullState) {
	if len(urls) == 0 {
		return
	}
	cli := newHTTPClient(30 * time.Second)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// Seed applied from meta.
	if meta, err := disk.GetMeta(ctx, tenant, timeline); err == nil {
		state.appliedLSN.Store(meta.LastLSN)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := pullOnce(cli, urls, token, tenant, timeline, applier, disk, state); err != nil {
				log.Printf("wal pull: %v", err)
			}
		}
	}
}

func pullOnce(cli *http.Client, urls []string, token, tenant, timeline string, applier *wal.Applier, disk *store.Disk, state *pullState) error {
	var lastErr error
	for _, raw := range urls {
		base := strings.TrimRight(raw, "/")
		req, err := http.NewRequest(http.MethodGet,
			fmt.Sprintf("%s/wal?tenant=%s&timeline=%s&list=1", base, tenant, timeline), nil)
		if err != nil {
			lastErr = err
			continue
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := cli.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		var body struct {
			Segnos   []uint64 `json:"segnos"`
			FlushLSN uint64   `json:"flush_lsn"`
		}
		decErr := json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if resp.StatusCode != 200 || decErr != nil {
			lastErr = fmt.Errorf("list status=%d err=%v", resp.StatusCode, decErr)
			continue
		}
		if body.FlushLSN > 0 {
			state.skFlushLSN.Store(body.FlushLSN)
		}
		applied := state.appliedLSN.Load()
		for _, segno := range body.Segnos {
			if err := fetchAndApply(cli, base, token, tenant, timeline, segno, applied, applier, disk, state); err != nil {
				lastErr = err
				continue
			}
		}
		updateLag(state)
		return nil
	}
	return lastErr
}

func fetchAndApply(cli *http.Client, base, token, tenant, timeline string, segno, applied uint64, applier *wal.Applier, disk *store.Disk, state *pullState) error {
	req, err := http.NewRequest(http.MethodGet,
		fmt.Sprintf("%s/wal?tenant=%s&timeline=%s&segno=%d", base, tenant, timeline, segno), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := cli.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("segno %d status %d", segno, resp.StatusCode)
	}
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return err
	}
	start, _ := strconv.ParseUint(resp.Header.Get("X-Start-LSN"), 10, 64)
	end, _ := strconv.ParseUint(resp.Header.Get("X-End-LSN"), 10, 64)
	if start == 0 {
		start = segno * (16 * 1024 * 1024)
	}
	if end == 0 {
		end = start + uint64(len(payload))
	}
	if end <= applied {
		return nil
	}
	got, err := applier.Apply(context.Background(), tenant, timeline, start, end, payload)
	if err != nil {
		return err
	}
	if got > state.appliedLSN.Load() {
		state.appliedLSN.Store(got)
	}
	_ = disk
	return nil
}

func updateLag(state *pullState) {
	applied := state.appliedLSN.Load()
	flush := state.skFlushLSN.Load()
	now := time.Now().UnixNano()
	if flush == 0 || applied >= flush {
		state.lastCatchup.Store(now)
		state.laggingSince.Store(0)
		return
	}
	if state.laggingSince.Load() == 0 {
		state.laggingSince.Store(now)
	}
}
