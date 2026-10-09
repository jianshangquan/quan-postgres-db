package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// peerHTTPClient is used for sync fanout; tests may replace it.
var peerHTTPClient = &http.Client{Timeout: 8 * time.Second}

func resolvePeerURLs() []string {
	urls := splitCSV(env("PAGESERVER_PEER_URLS", ""))
	if len(urls) == 0 {
		if u := env("PAGESERVER_PEER_URL", ""); u != "" {
			urls = []string{u}
		}
	}
	return urls
}

func fanoutPageToPeers(peers []string, token, tenant, timeline string, db, rel, fork, block uint32, lsn uint64, page []byte) error {
	for _, peer := range peers {
		if err := fanoutPage(peer, token, tenant, timeline, db, rel, fork, block, lsn, page); err != nil {
			return fmt.Errorf("%s: %w", peer, err)
		}
	}
	return nil
}

func fanoutBaseToPeers(peers []string, token, tenant, timeline string, archive []byte) error {
	for _, peer := range peers {
		if err := fanoutBase(peer, token, tenant, timeline, archive); err != nil {
			return fmt.Errorf("%s: %w", peer, err)
		}
	}
	return nil
}

func fanoutPage(peer, token, tenant, timeline string, db, rel, fork, block uint32, lsn uint64, page []byte) error {
	url := fmt.Sprintf("%s/v1/page?tenant=%s&timeline=%s&db=%d&rel=%d&fork=%d&block=%d&lsn=%d",
		strings.TrimRight(peer, "/"), tenant, timeline, db, rel, fork, block, lsn)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(page))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Quan-Peer", "1")
	req.Header.Set("Content-Type", "application/octet-stream")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	req = req.WithContext(ctx)
	resp, err := peerHTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func fanoutBase(peer, token, tenant, timeline string, archive []byte) error {
	url := fmt.Sprintf("%s/v1/basebackup?tenant=%s&timeline=%s", strings.TrimRight(peer, "/"), tenant, timeline)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(archive))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Quan-Peer", "1")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req = req.WithContext(ctx)
	resp, err := peerHTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}
