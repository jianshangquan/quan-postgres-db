package page

import (
	"context"

	"pageserver/src/store"
)

// Service serves GetPage/PutPage/RelSize with a hot cache in front of Store.
type Service struct {
	store store.Store
	disk  *store.Disk
	cache *Cache
}

func NewService(st store.Store, cacheSize int) *Service {
	svc := &Service{store: st, cache: NewCache(cacheSize)}
	if d, ok := st.(*store.Disk); ok {
		svc.disk = d
	}
	return svc
}

func (s *Service) Store() store.Store { return s.store }
func (s *Service) Cache() *Cache      { return s.cache }
func (s *Service) Disk() *store.Disk  { return s.disk }

func (s *Service) PutPage(ctx context.Context, tenant, timeline string, dbOid, rel, fork, block uint32, lsn uint64, page []byte) error {
	if err := s.store.PutPage(ctx, tenant, timeline, dbOid, rel, fork, block, lsn, page); err != nil {
		return err
	}
	s.cache.Put(tenant, timeline, dbOid, rel, fork, block, lsn, page)
	return s.bumpMeta(ctx, tenant, timeline, lsn)
}

func (s *Service) GetPage(ctx context.Context, tenant, timeline string, dbOid, rel, fork, block uint32, requestLSN uint64) ([]byte, uint64, error) {
	if p, lsn, ok := s.cache.Get(tenant, timeline, dbOid, rel, fork, block, requestLSN); ok {
		return p, lsn, nil
	}
	p, lsn, err := s.store.GetPageAtOrBefore(ctx, tenant, timeline, dbOid, rel, fork, block, requestLSN)
	if err != nil {
		return nil, 0, err
	}
	s.cache.Put(tenant, timeline, dbOid, rel, fork, block, lsn, p)
	return p, lsn, nil
}

func (s *Service) RelSize(ctx context.Context, tenant, timeline string, dbOid, rel, fork uint32, requestLSN uint64) (uint32, error) {
	maxBlk, err := s.store.RelMaxBlock(ctx, tenant, timeline, dbOid, rel, fork, requestLSN)
	if err != nil {
		return 0, err
	}
	if maxBlk < 0 {
		return 0, nil
	}
	return uint32(maxBlk) + 1, nil
}

func (s *Service) EnsureTimeline(ctx context.Context, tenant, timeline string) error {
	meta, err := s.store.GetMeta(ctx, tenant, timeline)
	if err != nil {
		meta = store.TimelineMeta{TenantID: tenant, TimelineID: timeline}
	} else {
		meta.TenantID = tenant
		meta.TimelineID = timeline
	}
	return s.store.PutMeta(ctx, meta)
}

func (s *Service) TimelineStatus(ctx context.Context, tenant, timeline string) (exists bool, lastLSN uint64, pageCount int, err error) {
	meta, metaErr := s.store.GetMeta(ctx, tenant, timeline)
	if metaErr != nil {
		return false, 0, 0, nil
	}
	count := meta.PageCount
	if s.disk != nil {
		count = s.disk.Index().PageCount(tenant, timeline)
	}
	return true, meta.LastLSN, count, nil
}

func (s *Service) bumpMeta(ctx context.Context, tenant, timeline string, lsn uint64) error {
	meta, err := s.store.GetMeta(ctx, tenant, timeline)
	if err != nil {
		meta = store.TimelineMeta{TenantID: tenant, TimelineID: timeline}
	}
	if s.disk != nil {
		meta.PageCount = s.disk.Index().PageCount(tenant, timeline)
	} else {
		meta.PageCount++
	}
	if lsn > meta.LastLSN {
		meta.LastLSN = lsn
	}
	return s.store.PutMeta(ctx, meta)
}

// CachePut updates the hot cache after WAL apply.
func (s *Service) CachePut(tenant, timeline string, dbOid, rel, fork, block uint32, lsn uint64, page []byte) {
	s.cache.Put(tenant, timeline, dbOid, rel, fork, block, lsn, page)
}
