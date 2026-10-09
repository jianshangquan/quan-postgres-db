package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
)

// Disk is a filesystem-backed Store under root with an in-memory version index.
// New page versions append into image layer files; legacy loose files are still readable
// and CompactLoose packs them into layers.
type Disk struct {
	root      string
	index     *Index
	layers    *layerWriter
	GCDeleted atomic.Uint64
}

func NewDisk(root string) (*Disk, error) {
	if root == "" {
		return nil, fmt.Errorf("store: data dir required")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	d := &Disk{root: root, index: NewIndex()}
	d.layers = newLayerWriter(d)
	if err := d.rebuildIndex(); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *Disk) Root() string  { return d.root }
func (d *Disk) Index() *Index { return d.index }

func (d *Disk) path(key string) string {
	return filepath.Join(d.root, filepath.FromSlash(key))
}

func (d *Disk) rebuildIndex() error {
	d.index = NewIndex()
	root := filepath.Join(d.root, "tenants")
	_ = filepath.WalkDir(root, func(path string, e os.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return nil
		}
		parts := strings.Split(filepath.ToSlash(rel), "/")
		// <tenant>/timelines/<timeline>/layers/<db>/<rel>/<fork>/<block>/<lsnhex>
		if len(parts) != 9 || parts[1] != "timelines" || parts[3] != "layers" {
			return nil
		}
		tenant, timeline := parts[0], parts[2]
		db64, e1 := strconv.ParseUint(parts[4], 10, 32)
		rel64, e2 := strconv.ParseUint(parts[5], 10, 32)
		fork64, e3 := strconv.ParseUint(parts[6], 10, 32)
		blk64, e4 := strconv.ParseUint(parts[7], 10, 32)
		lsn, e5 := strconv.ParseUint(parts[8], 16, 64)
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil {
			return nil
		}
		d.index.Put(tenant, timeline, uint32(db64), uint32(rel64), uint32(fork64), uint32(blk64), lsn)
		return nil
	})
	return d.loadLayerManifests()
}

func (d *Disk) put(_ context.Context, key string, data []byte) error {
	path := d.path(key)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
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
	if err := tmp.Sync(); err != nil {
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
	if err := ValidateID(meta.TenantID); err != nil {
		return err
	}
	if err := ValidateID(meta.TimelineID); err != nil {
		return err
	}
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
	if err := ValidateID(tenantID); err != nil {
		return err
	}
	if err := ValidateID(timelineID); err != nil {
		return err
	}
	if len(page) != PageSize {
		return fmt.Errorf("page must be %d bytes", PageSize)
	}
	_ = ctx
	layerID, off, err := d.layers.append(tenantID, timelineID, dbOid, rel, fork, block, lsn, page)
	if err != nil {
		return err
	}
	d.index.PutLoc(tenantID, timelineID, dbOid, rel, fork, block, lsn, layerID, off)
	return nil
}

// PutPageLoose writes a legacy one-file-per-version page (used by CompactLoose source and tests).
func (d *Disk) PutPageLoose(ctx context.Context, tenantID, timelineID string, dbOid, rel, fork, block uint32, lsn uint64, page []byte) error {
	if err := ValidateID(tenantID); err != nil {
		return err
	}
	if err := ValidateID(timelineID); err != nil {
		return err
	}
	if len(page) != PageSize {
		return fmt.Errorf("page must be %d bytes", PageSize)
	}
	if err := d.put(ctx, PageKey(tenantID, timelineID, dbOid, rel, fork, block, lsn), page); err != nil {
		return err
	}
	d.index.Put(tenantID, timelineID, dbOid, rel, fork, block, lsn)
	return nil
}

func (d *Disk) GetPage(ctx context.Context, tenantID, timelineID string, dbOid, rel, fork, block uint32, lsn uint64) ([]byte, error) {
	return d.readVersion(ctx, tenantID, timelineID, dbOid, rel, fork, block, lsn)
}

func (d *Disk) readVersion(ctx context.Context, tenantID, timelineID string, dbOid, rel, fork, block uint32, lsn uint64) ([]byte, error) {
	layer, off, ok := d.index.Loc(tenantID, timelineID, dbOid, rel, fork, block, lsn)
	if ok && layer != "" {
		return d.readLayerPage(tenantID, timelineID, layer, off)
	}
	return d.get(ctx, PageKey(tenantID, timelineID, dbOid, rel, fork, block, lsn))
}

func (d *Disk) GetPageAtOrBefore(ctx context.Context, tenantID, timelineID string, dbOid, rel, fork, block uint32, requestLSN uint64) ([]byte, uint64, error) {
	bestLSN, ok := d.index.BestAtOrBefore(tenantID, timelineID, dbOid, rel, fork, block, requestLSN)
	if !ok {
		return nil, 0, fmt.Errorf("page not found at or before LSN %d", requestLSN)
	}
	b, err := d.readVersion(ctx, tenantID, timelineID, dbOid, rel, fork, block, bestLSN)
	if err != nil {
		return nil, 0, err
	}
	return b, bestLSN, nil
}

func (d *Disk) PutBasebackup(ctx context.Context, tenantID, timelineID string, archive []byte) error {
	if err := ValidateID(tenantID); err != nil {
		return err
	}
	if err := ValidateID(timelineID); err != nil {
		return err
	}
	return d.put(ctx, BasebackupKey(tenantID, timelineID), archive)
}

func (d *Disk) GetBasebackup(ctx context.Context, tenantID, timelineID string) ([]byte, error) {
	return d.get(ctx, BasebackupKey(tenantID, timelineID))
}

func (d *Disk) RelMaxBlock(ctx context.Context, tenantID, timelineID string, dbOid, rel, fork uint32, requestLSN uint64) (int64, error) {
	_ = ctx
	return d.index.MaxBlock(tenantID, timelineID, dbOid, rel, fork, requestLSN), nil
}

func (d *Disk) ListPages(ctx context.Context, tenantID, timelineID string, requestLSN uint64) ([]PageRef, error) {
	_ = ctx
	return d.index.ListBest(tenantID, timelineID, requestLSN), nil
}

// CompactLoose packs loose one-file page versions into image layers. Returns count packed.
func (d *Disk) CompactLoose() int {
	ctx := context.Background()
	n := 0
	for _, bv := range d.index.LooseVersions() {
		for _, lsn := range bv.LSNs {
			key := PageKey(bv.Tenant, bv.Timeline, bv.DBOid, bv.Rel, bv.Fork, bv.Block, lsn)
			page, err := d.get(ctx, key)
			if err != nil {
				continue
			}
			layerID, off, err := d.layers.append(bv.Tenant, bv.Timeline, bv.DBOid, bv.Rel, bv.Fork, bv.Block, lsn, page)
			if err != nil {
				continue
			}
			d.index.PutLoc(bv.Tenant, bv.Timeline, bv.DBOid, bv.Rel, bv.Fork, bv.Block, lsn, layerID, off)
			_ = os.Remove(d.path(key))
			n++
		}
	}
	return n
}

// GCKeepNewest deletes older page versions, keeping only the newest LSN per block.
// Loose files are removed; layer-backed old versions are dropped from the index only.
func (d *Disk) GCKeepNewest() int {
	deleted := 0
	for _, item := range d.index.AllVersions() {
		if len(item.LSNs) <= 1 {
			continue
		}
		newest := item.LSNs[len(item.LSNs)-1]
		for i, lsn := range item.LSNs {
			if lsn == newest {
				continue
			}
			layer := ""
			if i < len(item.Layers) {
				layer = item.Layers[i]
			}
			if layer == "" {
				key := PageKey(item.Tenant, item.Timeline, item.DBOid, item.Rel, item.Fork, item.Block, lsn)
				_ = os.Remove(d.path(key))
			}
			d.index.Remove(item.Tenant, item.Timeline, item.DBOid, item.Rel, item.Fork, item.Block, lsn)
			deleted++
		}
	}
	d.GCDeleted.Add(uint64(deleted))
	return deleted
}

var _ Store = (*Disk)(nil)
