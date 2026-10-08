package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Disk is a filesystem-backed Store under root.
type Disk struct {
	root string
}

func NewDisk(root string) (*Disk, error) {
	if root == "" {
		return nil, fmt.Errorf("store: data dir required")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &Disk{root: root}, nil
}

func (d *Disk) path(key string) string {
	return filepath.Join(d.root, filepath.FromSlash(key))
}

func (d *Disk) put(_ context.Context, key string, data []byte) error {
	path := d.path(key)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	// Unique tmp name — concurrent PutPage/meta updates must not share "*.tmp".
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return nil
}

func (d *Disk) get(_ context.Context, key string) ([]byte, error) {
	b, err := os.ReadFile(d.path(key))
	if err != nil {
		return nil, fmt.Errorf("not found: %s", key)
	}
	return b, nil
}

func (d *Disk) PutMeta(ctx context.Context, meta TimelineMeta) error {
	b, err := EncodeMeta(meta)
	if err != nil {
		return err
	}
	return d.put(ctx, MetaKey(meta.TenantID, meta.TimelineID), b)
}

func (d *Disk) GetMeta(ctx context.Context, tenantID, timelineID string) (TimelineMeta, error) {
	b, err := d.get(ctx, MetaKey(tenantID, timelineID))
	if err != nil {
		return TimelineMeta{}, err
	}
	return DecodeMeta(b)
}

func (d *Disk) PutPage(ctx context.Context, tenantID, timelineID string, dbOid, rel, fork, block uint32, lsn uint64, page []byte) error {
	if len(page) != PageSize {
		return fmt.Errorf("page must be %d bytes", PageSize)
	}
	return d.put(ctx, PageKey(tenantID, timelineID, dbOid, rel, fork, block, lsn), page)
}

func (d *Disk) GetPage(ctx context.Context, tenantID, timelineID string, dbOid, rel, fork, block uint32, lsn uint64) ([]byte, error) {
	return d.get(ctx, PageKey(tenantID, timelineID, dbOid, rel, fork, block, lsn))
}

func (d *Disk) GetPageAtOrBefore(ctx context.Context, tenantID, timelineID string, dbOid, rel, fork, block uint32, requestLSN uint64) ([]byte, uint64, error) {
	_ = ctx
	dir := d.path(PagePrefix(tenantID, timelineID, dbOid, rel, fork, block))
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, fmt.Errorf("page not found at or before LSN %d", requestLSN)
	}
	var bestLSN uint64
	var bestName string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		lsn, err := strconv.ParseUint(e.Name(), 16, 64)
		if err != nil {
			continue
		}
		if lsn <= requestLSN && lsn >= bestLSN {
			bestLSN = lsn
			bestName = e.Name()
		}
	}
	if bestName == "" {
		return nil, 0, fmt.Errorf("page not found at or before LSN %d", requestLSN)
	}
	b, err := os.ReadFile(filepath.Join(dir, bestName))
	if err != nil {
		return nil, 0, err
	}
	return b, bestLSN, nil
}

func (d *Disk) PutBasebackup(ctx context.Context, tenantID, timelineID string, archive []byte) error {
	return d.put(ctx, BasebackupKey(tenantID, timelineID), archive)
}

func (d *Disk) GetBasebackup(ctx context.Context, tenantID, timelineID string) ([]byte, error) {
	return d.get(ctx, BasebackupKey(tenantID, timelineID))
}

func (d *Disk) RelMaxBlock(ctx context.Context, tenantID, timelineID string, dbOid, rel, fork uint32, requestLSN uint64) (int64, error) {
	_ = ctx
	dir := d.path(fmt.Sprintf("tenants/%s/timelines/%s/layers/%d/%d/%d/", tenantID, timelineID, dbOid, rel, fork))
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return -1, nil
		}
		return -1, err
	}
	var maxBlk int64 = -1
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		blk, err := strconv.ParseUint(e.Name(), 10, 32)
		if err != nil {
			continue
		}
		// Ensure at least one version <= requestLSN exists.
		blockDir := filepath.Join(dir, e.Name())
		vers, err := os.ReadDir(blockDir)
		if err != nil {
			continue
		}
		ok := false
		for _, v := range vers {
			if v.IsDir() {
				continue
			}
			lsn, err := strconv.ParseUint(v.Name(), 16, 64)
			if err != nil || lsn > requestLSN {
				continue
			}
			ok = true
			break
		}
		if ok && int64(blk) > maxBlk {
			maxBlk = int64(blk)
		}
	}
	return maxBlk, nil
}

func (d *Disk) ListPages(ctx context.Context, tenantID, timelineID string, requestLSN uint64) ([]PageRef, error) {
	_ = ctx
	root := d.path(LayersPrefix(tenantID, timelineID))
	type key struct{ db, rel, fork, block uint32 }
	best := map[key]uint64{}
	_ = filepath.WalkDir(root, func(path string, e os.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return nil
		}
		parts := strings.Split(filepath.ToSlash(rel), "/")
		if len(parts) != 5 {
			return nil
		}
		db64, e1 := strconv.ParseUint(parts[0], 10, 32)
		rel64, e2 := strconv.ParseUint(parts[1], 10, 32)
		fork64, e3 := strconv.ParseUint(parts[2], 10, 32)
		blk64, e4 := strconv.ParseUint(parts[3], 10, 32)
		lsn, e5 := strconv.ParseUint(parts[4], 16, 64)
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil || lsn > requestLSN {
			return nil
		}
		kk := key{uint32(db64), uint32(rel64), uint32(fork64), uint32(blk64)}
		if prev, ok := best[kk]; !ok || lsn >= prev {
			best[kk] = lsn
		}
		return nil
	})
	out := make([]PageRef, 0, len(best))
	for kk, lsn := range best {
		out = append(out, PageRef{DBOid: kk.db, RelFilenode: kk.rel, Fork: kk.fork, Block: kk.block, LSN: lsn})
	}
	return out, nil
}

var _ Store = (*Disk)(nil)
