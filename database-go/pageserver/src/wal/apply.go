package wal

import (
	"context"
	"fmt"
	"log"

	"pageserver/src/page"
	"pageserver/src/store"
)

// Applier materializes pages from WAL payloads (FPI1 first, then PG segment FPI).
type Applier struct {
	svc *page.Service
}

func NewApplier(svc *page.Service) *Applier {
	return &Applier{svc: svc}
}

func (a *Applier) Apply(ctx context.Context, tenantID, timelineID string, startLSN, endLSN uint64, payload []byte) (uint64, error) {
	fps, rest, err := DecodeRecords(payload)
	if err != nil {
		return 0, err
	}
	// Prefer leftover / full buffer for PG decode (FPI1 may consume a prefix).
	pgPayload := payload
	pgStart := startLSN
	if len(fps) > 0 && len(rest) > 0 {
		pgPayload = rest
		pgStart = endLSN // best-effort; FPI1 stream usually isn't mixed with raw PG
	}
	if len(fps) == 0 && len(pgPayload) >= xlRecHeaderSize {
		if pgfps, err2 := ExtractFPIsFromPGWAL(pgPayload, pgStart, endLSN); err2 == nil {
			fps = append(fps, pgfps...)
		}
	}

	var applied uint64
	for _, rec := range fps {
		if err := a.svc.PutPage(ctx, tenantID, timelineID, rec.DBOid, rec.RelFilenode, rec.Fork, rec.Block, rec.LSN, rec.Page); err != nil {
			return applied, fmt.Errorf("put page: %w", err)
		}
		if rec.LSN > applied {
			applied = rec.LSN
		}
	}
	if endLSN > applied {
		applied = endLSN
	}
	if applied > 0 {
		_ = a.svc.EnsureTimeline(ctx, tenantID, timelineID)
		meta, err := a.svc.Store().GetMeta(ctx, tenantID, timelineID)
		if err != nil {
			meta = store.TimelineMeta{TenantID: tenantID, TimelineID: timelineID}
		}
		if applied > meta.LastLSN {
			meta.LastLSN = applied
			_ = a.svc.Store().PutMeta(ctx, meta)
		}
	}
	log.Printf("apply tenant=%s timeline=%s lsn=%d..%d bytes=%d pages=%d applied_lsn=%d",
		tenantID, timelineID, startLSN, endLSN, len(payload), len(fps), applied)
	return applied, nil
}
