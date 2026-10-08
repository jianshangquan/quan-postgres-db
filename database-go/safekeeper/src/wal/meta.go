package wal

import (
	"encoding/json"
	"os"
)

// TimelineMeta tracks the highest durable LSN for a timeline.
type TimelineMeta struct {
	TenantID   string `json:"tenant_id"`
	TimelineID string `json:"timeline_id"`
	FlushLSN   uint64 `json:"flush_lsn"`
}

func (w *Wal) readMetaLocked(tenantID, timelineID string) (TimelineMeta, error) {
	b, err := os.ReadFile(w.metaPath(tenantID, timelineID))
	if err != nil {
		return TimelineMeta{}, err
	}
	var meta TimelineMeta
	if err := json.Unmarshal(b, &meta); err != nil {
		return TimelineMeta{}, err
	}
	return meta, nil
}

func (w *Wal) writeMetaLocked(meta TimelineMeta) error {
	b, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	path := w.metaPath(meta.TenantID, meta.TimelineID)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
