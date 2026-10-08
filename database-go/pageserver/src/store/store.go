package store

import (
	"context"
	"encoding/json"
	"fmt"
)

const PageSize = 8192

// TimelineMeta tracks the highest applied LSN for a timeline.
type TimelineMeta struct {
	TenantID   string `json:"tenant_id"`
	TimelineID string `json:"timeline_id"`
	LastLSN    uint64 `json:"last_lsn"`
	PageCount  int    `json:"page_count,omitempty"`
}

// PageRef identifies a stored page version.
type PageRef struct {
	DBOid       uint32 `json:"DBOid"`
	RelFilenode uint32 `json:"RelFilenode"`
	Fork        uint32 `json:"Fork"`
	Block       uint32 `json:"Block"`
	LSN         uint64 `json:"LSN"`
}

// Store persists page layers and timeline metadata.
type Store interface {
	PutMeta(ctx context.Context, meta TimelineMeta) error
	GetMeta(ctx context.Context, tenantID, timelineID string) (TimelineMeta, error)
	PutPage(ctx context.Context, tenantID, timelineID string, dbOid, rel, fork, block uint32, lsn uint64, page []byte) error
	GetPage(ctx context.Context, tenantID, timelineID string, dbOid, rel, fork, block uint32, lsn uint64) ([]byte, error)
	GetPageAtOrBefore(ctx context.Context, tenantID, timelineID string, dbOid, rel, fork, block uint32, requestLSN uint64) (page []byte, lsn uint64, err error)
	ListPages(ctx context.Context, tenantID, timelineID string, requestLSN uint64) ([]PageRef, error)
	PutBasebackup(ctx context.Context, tenantID, timelineID string, archive []byte) error
	GetBasebackup(ctx context.Context, tenantID, timelineID string) ([]byte, error)
	// RelMaxBlock returns the highest block number stored for a relation fork, or -1 if none.
	RelMaxBlock(ctx context.Context, tenantID, timelineID string, dbOid, rel, fork uint32, requestLSN uint64) (int64, error)
}

func BasebackupKey(tenantID, timelineID string) string {
	return fmt.Sprintf("tenants/%s/timelines/%s/basebackup.tar", tenantID, timelineID)
}

func LayersPrefix(tenantID, timelineID string) string {
	return fmt.Sprintf("tenants/%s/timelines/%s/layers/", tenantID, timelineID)
}

func PageKey(tenantID, timelineID string, dbOid, rel, fork, block uint32, lsn uint64) string {
	return fmt.Sprintf("tenants/%s/timelines/%s/layers/%d/%d/%d/%d/%016x", tenantID, timelineID, dbOid, rel, fork, block, lsn)
}

func PagePrefix(tenantID, timelineID string, dbOid, rel, fork, block uint32) string {
	return fmt.Sprintf("tenants/%s/timelines/%s/layers/%d/%d/%d/%d/", tenantID, timelineID, dbOid, rel, fork, block)
}

func MetaKey(tenantID, timelineID string) string {
	return fmt.Sprintf("tenants/%s/timelines/%s/meta.json", tenantID, timelineID)
}

func EncodeMeta(meta TimelineMeta) ([]byte, error) {
	return json.Marshal(meta)
}

func DecodeMeta(b []byte) (TimelineMeta, error) {
	var m TimelineMeta
	err := json.Unmarshal(b, &m)
	return m, err
}
