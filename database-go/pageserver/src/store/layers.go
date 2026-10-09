package store

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	// ImageLayerTargetBytes is ~16 MiB of page images per layer file.
	ImageLayerTargetBytes = 16 << 20
)

type layerEntry struct {
	DBOid  uint32 `json:"db"`
	Rel    uint32 `json:"rel"`
	Fork   uint32 `json:"fork"`
	Block  uint32 `json:"block"`
	LSN    uint64 `json:"lsn"`
	Offset int64  `json:"off"`
}

type layerManifest struct {
	ID      string       `json:"id"`
	Entries []layerEntry `json:"entries"`
}

type openLayer struct {
	id   string
	file *os.File
	size int64
	man  layerManifest
}

type layerWriter struct {
	mu   sync.Mutex
	disk *Disk
	open map[string]*openLayer // tenant/timeline -> open
}

func newLayerWriter(d *Disk) *layerWriter {
	return &layerWriter{disk: d, open: make(map[string]*openLayer)}
}

func timelineKey(tenant, timeline string) string {
	return tenant + "/" + timeline
}

func (d *Disk) imageLayerDir(tenant, timeline string) string {
	return filepath.Join(d.root, "tenants", tenant, "timelines", timeline, "imagelayers")
}

func (lw *layerWriter) append(tenant, timeline string, dbOid, rel, fork, block uint32, lsn uint64, page []byte) (layerID string, off int64, err error) {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	k := timelineKey(tenant, timeline)
	ol, ok := lw.open[k]
	if !ok || ol.size+int64(len(page)) > ImageLayerTargetBytes {
		if ol != nil {
			_ = lw.flushLocked(ol, tenant, timeline)
			_ = ol.file.Close()
		}
		ol, err = lw.createLocked(tenant, timeline)
		if err != nil {
			return "", 0, err
		}
		lw.open[k] = ol
	}
	off = ol.size
	n, err := ol.file.Write(page)
	if err != nil {
		return "", 0, err
	}
	if n != len(page) {
		return "", 0, fmt.Errorf("short write")
	}
	if err := ol.file.Sync(); err != nil {
		return "", 0, err
	}
	ol.size += int64(n)
	ol.man.Entries = append(ol.man.Entries, layerEntry{
		DBOid: dbOid, Rel: rel, Fork: fork, Block: block, LSN: lsn, Offset: off,
	})
	if err := lw.flushLocked(ol, tenant, timeline); err != nil {
		return "", 0, err
	}
	return ol.id, off, nil
}

func (lw *layerWriter) createLocked(tenant, timeline string) (*openLayer, error) {
	dir := lw.disk.imageLayerDir(tenant, timeline)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	id := fmt.Sprintf("%016x", nextLayerID())
	path := filepath.Join(dir, id+".img")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	st, _ := f.Stat()
	return &openLayer{
		id: id, file: f, size: st.Size(),
		man: layerManifest{ID: id},
	}, nil
}

func (lw *layerWriter) flushLocked(ol *openLayer, tenant, timeline string) error {
	b, err := json.Marshal(ol.man)
	if err != nil {
		return err
	}
	idxPath := filepath.Join(lw.disk.imageLayerDir(tenant, timeline), ol.id+".idx")
	tmp := idxPath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, idxPath)
}

var layerSeq uint64

func nextLayerID() uint64 {
	// time-ish unique enough for lab; combine with counter
	layerSeq++
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], layerSeq)
	_ = buf
	return uint64(os.Getpid())<<32 | layerSeq
}

func (d *Disk) readLayerPage(tenant, timeline, layerID string, off int64) ([]byte, error) {
	path := filepath.Join(d.imageLayerDir(tenant, timeline), layerID+".img")
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, PageSize)
	n, err := f.ReadAt(buf, off)
	if err != nil {
		return nil, err
	}
	if n != PageSize {
		return nil, fmt.Errorf("short layer read")
	}
	return buf, nil
}

func (d *Disk) loadLayerManifests() error {
	root := filepath.Join(d.root, "tenants")
	return filepath.WalkDir(root, func(path string, e os.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".idx" {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return nil
		}
		// <tenant>/timelines/<timeline>/imagelayers/<id>.idx
		parts := strings.Split(filepath.ToSlash(rel), "/")
		if len(parts) != 5 || parts[1] != "timelines" || parts[3] != "imagelayers" {
			return nil
		}
		tenant, timeline := parts[0], parts[2]
		b, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		var man layerManifest
		if json.Unmarshal(b, &man) != nil || man.ID == "" {
			return nil
		}
		for _, ent := range man.Entries {
			d.index.PutLoc(tenant, timeline, ent.DBOid, ent.Rel, ent.Fork, ent.Block, ent.LSN, man.ID, ent.Offset)
		}
		return nil
	})
}
