package page

import "sync"

type key struct {
	tenant, timeline        string
	dbOid, rel, fork, block uint32
}

type entry struct {
	lsn  uint64
	page []byte
}

// Cache is a small hot-page LRU for the pageserver.
type Cache struct {
	mu    sync.Mutex
	items map[key]entry
	order []key
	max   int
}

func NewCache(max int) *Cache {
	if max <= 0 {
		max = 256
	}
	return &Cache{items: make(map[key]entry), max: max}
}

func (c *Cache) Put(tenant, timeline string, dbOid, rel, fork, block uint32, lsn uint64, page []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := key{tenant, timeline, dbOid, rel, fork, block}
	cp := make([]byte, len(page))
	copy(cp, page)
	if _, ok := c.items[k]; !ok {
		c.order = append(c.order, k)
	}
	c.items[k] = entry{lsn: lsn, page: cp}
	for len(c.order) > c.max {
		old := c.order[0]
		c.order = c.order[1:]
		delete(c.items, old)
	}
}

func (c *Cache) Get(tenant, timeline string, dbOid, rel, fork, block uint32, requestLSN uint64) ([]byte, uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := key{tenant, timeline, dbOid, rel, fork, block}
	e, ok := c.items[k]
	if !ok || e.lsn > requestLSN {
		return nil, 0, false
	}
	cp := make([]byte, len(e.page))
	copy(cp, e.page)
	return cp, e.lsn, true
}
