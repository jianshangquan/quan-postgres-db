package store

import (
	"sync"
)

type blockKey struct {
	tenant, timeline        string
	dbOid, rel, fork, block uint32
}

// pageLoc locates a page version: empty Layer means loose PageKey file.
type pageLoc struct {
	lsn   uint64
	layer string // layer id within timeline; "" = loose
	off   int64
}

// Index is an in-memory map of page versions per block.
type Index struct {
	mu     sync.RWMutex
	blocks map[blockKey][]pageLoc // sorted ascending by LSN
}

func NewIndex() *Index {
	return &Index{blocks: make(map[blockKey][]pageLoc)}
}

func (idx *Index) Put(tenant, timeline string, dbOid, rel, fork, block uint32, lsn uint64) {
	idx.PutLoc(tenant, timeline, dbOid, rel, fork, block, lsn, "", 0)
}

func (idx *Index) PutLoc(tenant, timeline string, dbOid, rel, fork, block uint32, lsn uint64, layer string, off int64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	k := blockKey{tenant, timeline, dbOid, rel, fork, block}
	lsns := idx.blocks[k]
	for i, v := range lsns {
		if v.lsn == lsn {
			lsns[i] = pageLoc{lsn: lsn, layer: layer, off: off}
			idx.blocks[k] = lsns
			return
		}
	}
	i := 0
	for i < len(lsns) && lsns[i].lsn < lsn {
		i++
	}
	entry := pageLoc{lsn: lsn, layer: layer, off: off}
	lsns = append(lsns, pageLoc{})
	copy(lsns[i+1:], lsns[i:])
	lsns[i] = entry
	idx.blocks[k] = lsns
}

func (idx *Index) Loc(tenant, timeline string, dbOid, rel, fork, block uint32, lsn uint64) (layer string, off int64, ok bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	for _, v := range idx.blocks[blockKey{tenant, timeline, dbOid, rel, fork, block}] {
		if v.lsn == lsn {
			return v.layer, v.off, true
		}
	}
	return "", 0, false
}

func (idx *Index) Remove(tenant, timeline string, dbOid, rel, fork, block uint32, lsn uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	k := blockKey{tenant, timeline, dbOid, rel, fork, block}
	lsns := idx.blocks[k]
	out := lsns[:0]
	for _, v := range lsns {
		if v.lsn != lsn {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		delete(idx.blocks, k)
	} else {
		idx.blocks[k] = out
	}
}

// BestAtOrBefore returns the highest LSN <= requestLSN, or 0 if none.
func (idx *Index) BestAtOrBefore(tenant, timeline string, dbOid, rel, fork, block uint32, requestLSN uint64) (uint64, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	lsns := idx.blocks[blockKey{tenant, timeline, dbOid, rel, fork, block}]
	var best uint64
	ok := false
	for _, v := range lsns {
		if v.lsn <= requestLSN {
			best = v.lsn
			ok = true
		} else {
			break
		}
	}
	return best, ok
}

// MaxBlock returns highest block number with a version <= requestLSN, or -1.
func (idx *Index) MaxBlock(tenant, timeline string, dbOid, rel, fork uint32, requestLSN uint64) int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	var max int64 = -1
	for k, lsns := range idx.blocks {
		if k.tenant != tenant || k.timeline != timeline || k.dbOid != dbOid || k.rel != rel || k.fork != fork {
			continue
		}
		for _, v := range lsns {
			if v.lsn <= requestLSN {
				if int64(k.block) > max {
					max = int64(k.block)
				}
				break
			}
		}
	}
	return max
}

// PageCount returns number of distinct blocks for a timeline.
func (idx *Index) PageCount(tenant, timeline string) int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	n := 0
	for k := range idx.blocks {
		if k.tenant == tenant && k.timeline == timeline {
			n++
		}
	}
	return n
}

// BlockVersions is one block's LSN list for GC.
type BlockVersions struct {
	Tenant, Timeline        string
	DBOid, Rel, Fork, Block uint32
	LSNs                    []uint64
	Layers                  []string
	Offs                    []int64
}

// AllVersions returns a copy of all block version lists for GC.
func (idx *Index) AllVersions() []BlockVersions {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	out := make([]BlockVersions, 0, len(idx.blocks))
	for k, lsns := range idx.blocks {
		bv := BlockVersions{
			Tenant: k.tenant, Timeline: k.timeline,
			DBOid: k.dbOid, Rel: k.rel, Fork: k.fork, Block: k.block,
			LSNs: make([]uint64, len(lsns)), Layers: make([]string, len(lsns)), Offs: make([]int64, len(lsns)),
		}
		for i, v := range lsns {
			bv.LSNs[i] = v.lsn
			bv.Layers[i] = v.layer
			bv.Offs[i] = v.off
		}
		out = append(out, bv)
	}
	return out
}

// LooseVersions returns versions stored as loose files (empty layer).
func (idx *Index) LooseVersions() []BlockVersions {
	all := idx.AllVersions()
	var out []BlockVersions
	for _, bv := range all {
		var lsns []uint64
		for i, layer := range bv.Layers {
			if layer == "" {
				lsns = append(lsns, bv.LSNs[i])
			}
		}
		if len(lsns) > 0 {
			out = append(out, BlockVersions{
				Tenant: bv.Tenant, Timeline: bv.Timeline,
				DBOid: bv.DBOid, Rel: bv.Rel, Fork: bv.Fork, Block: bv.Block,
				LSNs: lsns,
			})
		}
	}
	return out
}

// ListBest returns best page refs at or before requestLSN.
func (idx *Index) ListBest(tenant, timeline string, requestLSN uint64) []PageRef {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	var out []PageRef
	for k, lsns := range idx.blocks {
		if k.tenant != tenant || k.timeline != timeline {
			continue
		}
		var best uint64
		ok := false
		for _, v := range lsns {
			if v.lsn <= requestLSN {
				best = v.lsn
				ok = true
			}
		}
		if ok {
			out = append(out, PageRef{
				DBOid: k.dbOid, RelFilenode: k.rel, Fork: k.fork, Block: k.block, LSN: best,
			})
		}
	}
	return out
}
