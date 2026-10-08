package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Wal is the safekeeper durable WAL store: append, merge, and serve segments.
type Wal struct {
	dir string
	mu  sync.Mutex
	// tip caches FlushLSN per "tenant/timeline"
	tip map[string]uint64
}

// NewWal opens (or creates) a WAL root directory.
func NewWal(dir string) (*Wal, error) {
	if dir == "" {
		return nil, errors.New("wal: data dir required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir: %w", err)
	}
	return &Wal{dir: dir, tip: make(map[string]uint64)}, nil
}

func tlKey(tenant, timeline string) string {
	return tenant + "/" + timeline
}

func (w *Wal) timelineDir(tenant, timeline string) string {
	return filepath.Join(w.dir, tenant, timeline)
}

func (w *Wal) walDir(tenant, timeline string) string {
	return filepath.Join(w.timelineDir(tenant, timeline), "wal")
}

func (w *Wal) metaPath(tenant, timeline string) string {
	return filepath.Join(w.timelineDir(tenant, timeline), "meta.json")
}

func (w *Wal) segmentPath(tenant, timeline string, segno uint64) string {
	return filepath.Join(w.walDir(tenant, timeline), fmt.Sprintf("%016x.wal", segno))
}

// CreateTimeline ensures tenant/timeline directories and zero meta exist.
func (w *Wal) CreateTimeline(tenantID, timelineID string) error {
	if tenantID == "" || timelineID == "" {
		return errors.New("wal: tenant_id and timeline_id required")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := os.MkdirAll(w.walDir(tenantID, timelineID), 0o755); err != nil {
		return err
	}
	path := w.metaPath(tenantID, timelineID)
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	meta := TimelineMeta{TenantID: tenantID, TimelineID: timelineID, FlushLSN: 0}
	return w.writeMetaLocked(meta)
}

// Append stores a WAL frame, merging with any existing segment for the same segno.
// Returns the timeline flush LSN (tip) after the write.
func (w *Wal) Append(tenantID, timelineID string, startLSN, endLSN uint64, payload []byte) (flushLSN uint64, err error) {
	if tenantID == "" || timelineID == "" {
		return 0, errors.New("wal: tenant_id and timeline_id required")
	}
	if len(payload) == 0 {
		return 0, errors.New("wal: empty payload")
	}
	if endLSN < startLSN {
		return 0, fmt.Errorf("wal: end_lsn %d < start_lsn %d", endLSN, startLSN)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := os.MkdirAll(w.walDir(tenantID, timelineID), 0o755); err != nil {
		return 0, err
	}

	incoming := Frame{
		TenantID:   tenantID,
		TimelineID: timelineID,
		StartLSN:   startLSN,
		EndLSN:     endLSN,
		Payload:    payload,
	}
	segno := SegnoFromLSN(startLSN)
	chosen := incoming
	if raw, err := os.ReadFile(w.segmentPath(tenantID, timelineID, segno)); err == nil {
		if existing, err := DecodeFrame(raw); err == nil {
			chosen = PreferWALFrame(existing, incoming)
		}
	}

	enc := EncodeFrame(chosen)
	path := w.segmentPath(tenantID, timelineID, segno)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, enc, 0o644); err != nil {
		return 0, err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return 0, err
	}

	meta, _ := w.readMetaLocked(tenantID, timelineID)
	if meta.TenantID == "" {
		meta = TimelineMeta{TenantID: tenantID, TimelineID: timelineID}
	}
	if chosen.EndLSN > meta.FlushLSN {
		meta.FlushLSN = chosen.EndLSN
		if err := w.writeMetaLocked(meta); err != nil {
			return 0, err
		}
	}
	w.tip[tlKey(tenantID, timelineID)] = meta.FlushLSN
	return meta.FlushLSN, nil
}

// GetSegment returns the encoded frame bytes for segno, or os.ErrNotExist.
func (w *Wal) GetSegment(tenantID, timelineID string, segno uint64) ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return os.ReadFile(w.segmentPath(tenantID, timelineID, segno))
}

// GetFrame decodes the segment frame for segno.
func (w *Wal) GetFrame(tenantID, timelineID string, segno uint64) (Frame, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	raw, err := os.ReadFile(w.segmentPath(tenantID, timelineID, segno))
	if err != nil {
		return Frame{}, err
	}
	return DecodeFrame(raw)
}

// ListSegments returns sorted segment numbers that have WAL on disk.
func (w *Wal) ListSegments(tenantID, timelineID string) ([]uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	entries, err := os.ReadDir(w.walDir(tenantID, timelineID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if filepath.Ext(name) != ".wal" {
			continue
		}
		var segno uint64
		if _, err := fmt.Sscanf(name, "%016x.wal", &segno); err != nil {
			continue
		}
		out = append(out, segno)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// FlushLSN returns the highest durable LSN for the timeline.
func (w *Wal) FlushLSN(tenantID, timelineID string) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if tip, ok := w.tip[tlKey(tenantID, timelineID)]; ok {
		return tip, nil
	}
	meta, err := w.readMetaLocked(tenantID, timelineID)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	w.tip[tlKey(tenantID, timelineID)] = meta.FlushLSN
	return meta.FlushLSN, nil
}
