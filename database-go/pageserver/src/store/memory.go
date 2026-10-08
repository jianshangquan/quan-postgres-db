package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// Memory is an in-process Store for tests.
type Memory struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewMemory() *Memory {
	return &Memory{data: make(map[string][]byte)}
}

func (m *Memory) put(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.data[key] = cp
	return nil
}

func (m *Memory) get(_ context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("not found: %s", key)
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp, nil
}

func (m *Memory) PutMeta(ctx context.Context, meta TimelineMeta) error {
	b, err := EncodeMeta(meta)
	if err != nil {
		return err
	}
	return m.put(ctx, MetaKey(meta.TenantID, meta.TimelineID), b)
}

func (m *Memory) GetMeta(ctx context.Context, tenantID, timelineID string) (TimelineMeta, error) {
	b, err := m.get(ctx, MetaKey(tenantID, timelineID))
	if err != nil {
		return TimelineMeta{}, err
	}
	return DecodeMeta(b)
}

func (m *Memory) PutPage(ctx context.Context, tenantID, timelineID string, dbOid, rel, fork, block uint32, lsn uint64, page []byte) error {
	if len(page) != PageSize {
		return fmt.Errorf("page must be %d bytes", PageSize)
	}
	return m.put(ctx, PageKey(tenantID, timelineID, dbOid, rel, fork, block, lsn), page)
}

func (m *Memory) GetPage(ctx context.Context, tenantID, timelineID string, dbOid, rel, fork, block uint32, lsn uint64) ([]byte, error) {
	return m.get(ctx, PageKey(tenantID, timelineID, dbOid, rel, fork, block, lsn))
}

func (m *Memory) GetPageAtOrBefore(ctx context.Context, tenantID, timelineID string, dbOid, rel, fork, block uint32, requestLSN uint64) ([]byte, uint64, error) {
	_ = ctx
	prefix := PagePrefix(tenantID, timelineID, dbOid, rel, fork, block)
	m.mu.RLock()
	defer m.mu.RUnlock()
	var bestLSN uint64
	var best []byte
	for k, v := range m.data {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		lsnHex := k[strings.LastIndex(k, "/")+1:]
		lsn, err := strconv.ParseUint(lsnHex, 16, 64)
		if err != nil {
			continue
		}
		if lsn <= requestLSN && lsn >= bestLSN {
			bestLSN = lsn
			best = v
		}
	}
	if best == nil {
		return nil, 0, fmt.Errorf("page not found at or before LSN %d", requestLSN)
	}
	cp := make([]byte, len(best))
	copy(cp, best)
	return cp, bestLSN, nil
}

func (m *Memory) PutBasebackup(ctx context.Context, tenantID, timelineID string, archive []byte) error {
	return m.put(ctx, BasebackupKey(tenantID, timelineID), archive)
}

func (m *Memory) GetBasebackup(ctx context.Context, tenantID, timelineID string) ([]byte, error) {
	return m.get(ctx, BasebackupKey(tenantID, timelineID))
}

func (m *Memory) RelMaxBlock(ctx context.Context, tenantID, timelineID string, dbOid, rel, fork uint32, requestLSN uint64) (int64, error) {
	_ = ctx
	prefix := fmt.Sprintf("tenants/%s/timelines/%s/layers/%d/%d/%d/", tenantID, timelineID, dbOid, rel, fork)
	var maxBlk int64 = -1
	m.mu.RLock()
	defer m.mu.RUnlock()
	for k := range m.data {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		rest := strings.TrimPrefix(k, prefix)
		parts := strings.Split(rest, "/")
		if len(parts) != 2 {
			continue
		}
		blk64, e1 := strconv.ParseUint(parts[0], 10, 32)
		lsn, e2 := strconv.ParseUint(parts[1], 16, 64)
		if e1 != nil || e2 != nil || lsn > requestLSN {
			continue
		}
		if int64(blk64) > maxBlk {
			maxBlk = int64(blk64)
		}
	}
	return maxBlk, nil
}

func (m *Memory) ListPages(ctx context.Context, tenantID, timelineID string, requestLSN uint64) ([]PageRef, error) {
	_ = ctx
	prefix := LayersPrefix(tenantID, timelineID)
	type key struct{ db, rel, fork, block uint32 }
	best := map[key]uint64{}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for k := range m.data {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		rest := strings.TrimPrefix(k, prefix)
		parts := strings.Split(rest, "/")
		if len(parts) != 5 {
			continue
		}
		db64, e1 := strconv.ParseUint(parts[0], 10, 32)
		rel64, e2 := strconv.ParseUint(parts[1], 10, 32)
		fork64, e3 := strconv.ParseUint(parts[2], 10, 32)
		blk64, e4 := strconv.ParseUint(parts[3], 10, 32)
		lsn, e5 := strconv.ParseUint(parts[4], 16, 64)
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil || lsn > requestLSN {
			continue
		}
		kk := key{uint32(db64), uint32(rel64), uint32(fork64), uint32(blk64)}
		if prev, ok := best[kk]; !ok || lsn >= prev {
			best[kk] = lsn
		}
	}
	out := make([]PageRef, 0, len(best))
	for kk, lsn := range best {
		out = append(out, PageRef{DBOid: kk.db, RelFilenode: kk.rel, Fork: kk.fork, Block: kk.block, LSN: lsn})
	}
	return out, nil
}

var _ Store = (*Memory)(nil)
