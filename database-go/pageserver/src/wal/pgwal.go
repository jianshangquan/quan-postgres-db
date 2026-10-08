package wal

import (
	"encoding/binary"
	"errors"
)

// Postgres XLog decoder: strip page headers, then structured FPI extract.
// Handles multi-page records (FPI image alone is 8KiB and routinely spans pages).

const (
	pgXLogPageSize        = 8192
	pgXLogLongPageHeader  = 40
	pgXLogShortPageHeader = 24
	xlRecHeaderSize       = 24
	maxSensibleBlocks     = 1024 * 1024

	bkpBlockHasImage = 0x10
	bkpBlockHasData  = 0x20
	bkpBlockSameRel  = 0x80
	bkpBlockForkMask = 0x0F

	// PG14+ bimg_info (PG17: 0x02 is APPLY, not "is compressed").
	bkpImageHasHole       = 0x01
	bkpImageApply         = 0x02
	bkpImageCompressPGLZ  = 0x04
	bkpImageCompressLZ4   = 0x08
	bkpImageCompressZSTD  = 0x10
	bkpImageCompressMask  = bkpImageCompressPGLZ | bkpImageCompressLZ4 | bkpImageCompressZSTD
)

// ExtractFPIsFromPGWAL scans WAL bytes for full-page images.
// startLSN is the LSN of data[0] (used to align to 8KiB XLog pages).
func ExtractFPIsFromPGWAL(data []byte, startLSN, defaultLSN uint64) ([]FPIRecord, error) {
	if len(data) < xlRecHeaderSize {
		return nil, nil
	}
	origLSN := startLSN
	pageOff := int(startLSN % pgXLogPageSize)
	var prefix []byte
	if pageOff != 0 {
		// data[0] is already mid-page; bytes until the next page boundary are
		// payload continuation (no XLog page header). Keep them, then strip headers
		// from the aligned remainder.
		n := pgXLogPageSize - pageOff
		if n > len(data) {
			n = len(data)
		}
		prefix = data[:n]
		data = data[n:]
		startLSN += uint64(n)
	}

	stream, streamLSN := stripXLogPages(data, startLSN)
	if len(prefix) > 0 {
		combined := make([]byte, 0, len(prefix)+len(stream))
		combined = append(combined, prefix...)
		combined = append(combined, stream...)
		stream = combined
		streamLSN = origLSN
	}
	if len(stream) < xlRecHeaderSize {
		return nil, nil
	}
	return extractRecordsContiguous(stream, streamLSN, defaultLSN), nil
}

// XLogPageHeaderData (PG14–17):
//
//	uint16 magic; uint16 info; TimeLineID tli; XLogRecPtr pageaddr; uint32 rem_len;
//	short header MAXALIGN → 24; long header (+sysid,seg_size,blcksz) → 40.
const (
	xlpMagicPG17  = 0xD116 // XLOG_PAGE_MAGIC PG17
	xlpLongHdr    = 0x0002 // XLP_LONG_HEADER
	xlpContRecord = 0x0001 // XLP_FIRST_IS_CONTRECORD
)

func isXLogPageMagic(m uint16) bool {
	// Postgres bumps magic per major; accept the modern 0xD1xx band.
	return m >= 0xD087 && m <= 0xD200
}

func xlogPageHeaderLen(page []byte) (hdrLen int, info uint16, pageAddr uint64, remLen uint32) {
	if len(page) < 20 {
		return pgXLogShortPageHeader, 0, 0, 0
	}
	info = binary.LittleEndian.Uint16(page[2:4])
	pageAddr = binary.LittleEndian.Uint64(page[8:16])
	remLen = binary.LittleEndian.Uint32(page[16:20])
	hdrLen = pgXLogShortPageHeader
	if info&xlpLongHdr != 0 {
		hdrLen = pgXLogLongPageHeader
	}
	return hdrLen, info, pageAddr, remLen
}

// stripXLogPages removes per-page headers so multi-page records become contiguous.
// Skips xlp_rem_len continuation bytes on pages with XLP_FIRST_IS_CONTRECORD.
func stripXLogPages(data []byte, startLSN uint64) ([]byte, uint64) {
	if len(data) < pgXLogPageSize {
		return data, startLSN
	}
	out := make([]byte, 0, len(data))
	firstLSN := startLSN
	for pageOff := 0; pageOff+pgXLogPageSize <= len(data); pageOff += pgXLogPageSize {
		page := data[pageOff : pageOff+pgXLogPageSize]
		magic := binary.LittleEndian.Uint16(page[0:2])
		if !isXLogPageMagic(magic) {
			// Unwritten / recycled tail of a segment.
			break
		}
		hdrLen, info, pageAddr, remLen := xlogPageHeaderLen(page)
		if pageOff == 0 && pageAddr != 0 {
			firstLSN = pageAddr
		}
		payload := page[hdrLen:]
		// Only skip continuation when OUR buffer starts mid-record (page 0 is CONTRECORD
		// from a prior segment we don't have). Within a contiguous multi-page buffer,
		// continuation bytes are part of the stream and must be kept.
		if pageOff == 0 && info&xlpContRecord != 0 && remLen > 0 {
			skip := int(remLen)
			if skip > len(payload) {
				skip = len(payload)
			}
			payload = payload[skip:]
		}
		out = append(out, payload...)
	}
	// Partial trailing page only when every full page was valid.
	fullPages := len(data) / pgXLogPageSize
	if fullPages > 0 && isXLogPageMagic(binary.LittleEndian.Uint16(data[(fullPages-1)*pgXLogPageSize:])) {
		rem := len(data) % pgXLogPageSize
		if rem > 20 {
			page := data[len(data)-rem:]
			if isXLogPageMagic(binary.LittleEndian.Uint16(page[0:2])) {
				hdrLen, _, _, _ := xlogPageHeaderLen(page)
				if rem > hdrLen {
					out = append(out, page[hdrLen:]...)
				}
			}
		}
	}
	return out, firstLSN
}

func extractRecordsContiguous(data []byte, startLSN, defaultLSN uint64) []FPIRecord {
	var out []FPIRecord
	off := 0
	for off+xlRecHeaderSize <= len(data) {
		if isZero(data[off : off+min(8, len(data)-off)]) {
			break
		}
		totalLen := int(binary.LittleEndian.Uint32(data[off : off+4]))
		if totalLen < xlRecHeaderSize || totalLen > len(data)-off {
			// May be page padding / misaligned — advance carefully.
			if totalLen < xlRecHeaderSize || totalLen > 128<<20 {
				off++
				continue
			}
			break
		}
		recLSN := defaultLSN
		if startLSN != 0 {
			recLSN = startLSN + uint64(off)
		}
		body := data[off+xlRecHeaderSize : off+totalLen]
		fps, ok := decodeRecordFPIs(body, recLSN)
		if ok && len(fps) > 0 {
			out = append(out, fps...)
		} else if !ok {
			out = append(out, extractBackupBlocksHeuristic(body, recLSN)...)
		}
		aligned := (totalLen + 7) &^ 7
		if aligned < xlRecHeaderSize {
			break
		}
		off += aligned
	}
	return out
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type decodedBlock struct {
	DBOid       uint32
	RelFilenode uint32
	Fork        uint32
	Block       uint32
	HasImage    bool
	ImageLen    int
	HoleOffset  uint16
	HoleLength  uint16
	HasHole     bool
	Compressed  bool
	Image       []byte
	HasData     bool
	DataLen     uint16
}

func parseBlockHeaders(body []byte) (blocks []decodedBlock, rem []byte, ok bool) {
	if len(body) < 4 {
		return nil, body, false
	}
	ptr := 0
	var lastDB, lastRel uint32
	var haveRel bool
	lastBlockID := -1
	for ptr+1 <= len(body) {
		id := body[ptr]
		switch id {
		case 255: // DATA_SHORT
			if ptr+2 > len(body) {
				return nil, body, false
			}
			ptr += 2
			goto headersDone
		case 254: // DATA_LONG
			if ptr+5 > len(body) {
				return nil, body, false
			}
			ptr += 5
			goto headersDone
		case 253: // ORIGIN
			if ptr+3 > len(body) {
				return nil, body, false
			}
			ptr += 3
			continue
		case 252: // TOPLEVEL_XID
			if ptr+5 > len(body) {
				return nil, body, false
			}
			ptr += 5
			continue
		}
		if ptr+4 > len(body) {
			break
		}
		if int(id) <= lastBlockID {
			break
		}
		lastBlockID = int(id)
		forkFlags := body[ptr+1]
		dataLen := binary.LittleEndian.Uint16(body[ptr+2 : ptr+4])
		ptr += 4

		blk := decodedBlock{
			Fork:     uint32(forkFlags & bkpBlockForkMask),
			HasImage: forkFlags&bkpBlockHasImage != 0,
			HasData:  forkFlags&bkpBlockHasData != 0,
			DataLen:  dataLen,
		}

		if blk.HasImage {
			if ptr+5 > len(body) {
				return nil, body, false
			}
			imgLen := int(binary.LittleEndian.Uint16(body[ptr : ptr+2]))
			holeOff := binary.LittleEndian.Uint16(body[ptr+2 : ptr+4])
			bimgInfo := body[ptr+4]
			ptr += 5
			blk.HoleOffset = holeOff
			blk.HasHole = bimgInfo&bkpImageHasHole != 0
			blk.Compressed = bimgInfo&bkpImageCompressMask != 0
			// CompressHeader (hole_length) is present only when the image is
			// both holey and compressed. Uncompressed holey images store
			// length = BLCKSZ - hole_length, so hole_length is derived.
			if blk.HasHole && blk.Compressed {
				if ptr+2 > len(body) {
					return nil, body, false
				}
				blk.HoleLength = binary.LittleEndian.Uint16(body[ptr : ptr+2])
				ptr += 2
			}
			if imgLen <= 0 || imgLen > PageSize {
				return nil, body, false
			}
			blk.ImageLen = imgLen
			if blk.HasHole && !blk.Compressed {
				if imgLen >= PageSize {
					return nil, body, false
				}
				blk.HoleLength = uint16(PageSize - imgLen)
			}
		}

		if forkFlags&bkpBlockSameRel == 0 {
			if ptr+12 > len(body) {
				return nil, body, false
			}
			blk.DBOid = binary.LittleEndian.Uint32(body[ptr+4 : ptr+8])
			blk.RelFilenode = binary.LittleEndian.Uint32(body[ptr+8 : ptr+12])
			ptr += 12
			lastDB, lastRel = blk.DBOid, blk.RelFilenode
			haveRel = true
		} else {
			if !haveRel {
				return nil, body, false
			}
			blk.DBOid, blk.RelFilenode = lastDB, lastRel
		}

		if ptr+4 > len(body) {
			return nil, body, false
		}
		blk.Block = binary.LittleEndian.Uint32(body[ptr : ptr+4])
		ptr += 4
		if blk.Block > maxSensibleBlocks || blk.Fork > 3 {
			return nil, body, false
		}
		blocks = append(blocks, blk)
		if len(blocks) > 32 {
			return nil, body, false
		}
	}
headersDone:
	if len(blocks) == 0 {
		return nil, body, false
	}

	for i := range blocks {
		if !blocks[i].HasImage {
			continue
		}
		n := blocks[i].ImageLen
		if n == 0 || ptr+n > len(body) {
			return nil, body, false
		}
		raw := body[ptr : ptr+n]
		ptr += n
		if blocks[i].Compressed {
			// wal_compression on — not supported in v1.
			blocks[i].HasImage = false
			blocks[i].Image = nil
			continue
		}
		page, err := restoreBlockImage(raw, blocks[i].HasHole, blocks[i].HoleOffset, blocks[i].HoleLength)
		if err != nil {
			blocks[i].HasImage = false
			blocks[i].Image = nil
			continue
		}
		blocks[i].Image = page
	}
	for i := range blocks {
		if !blocks[i].HasData || blocks[i].DataLen == 0 {
			continue
		}
		n := int(blocks[i].DataLen)
		if ptr+n > len(body) {
			return nil, body, false
		}
		ptr += n
	}
	return blocks, body[ptr:], true
}

// restoreBlockImage rebuilds an 8KiB page from a (possibly holey) FPW image.
// Mirrors Postgres RestoreBlockImage for the uncompressed HAS_HOLE case.
func restoreBlockImage(image []byte, hasHole bool, holeOff, holeLen uint16) ([]byte, error) {
	page := make([]byte, PageSize)
	if !hasHole {
		if len(image) != PageSize {
			return nil, errBadImage
		}
		copy(page, image)
		return page, nil
	}
	ho, hl := int(holeOff), int(holeLen)
	if ho > PageSize || hl > PageSize || ho+hl > PageSize {
		return nil, errBadImage
	}
	if len(image) != PageSize-hl {
		return nil, errBadImage
	}
	copy(page[0:ho], image[0:ho])
	// hole stays zero
	copy(page[ho+hl:], image[ho:])
	return page, nil
}

var errBadImage = errors.New("bad FPW image")

func decodeRecordFPIs(body []byte, lsn uint64) ([]FPIRecord, bool) {
	blocks, _, ok := parseBlockHeaders(body)
	if !ok {
		return nil, false
	}
	var out []FPIRecord
	for _, b := range blocks {
		if !b.HasImage || len(b.Image) != PageSize {
			continue
		}
		if b.RelFilenode == 0 {
			continue
		}
		out = append(out, FPIRecord{
			DBOid:       b.DBOid,
			RelFilenode: b.RelFilenode,
			Fork:        b.Fork,
			Block:       b.Block,
			LSN:         lsn,
			Page:        b.Image,
		})
	}
	return out, true
}

func extractBackupBlocksHeuristic(body []byte, lsn uint64) []FPIRecord {
	var out []FPIRecord
	i := 0
	for i+4 < len(body) {
		found := false
		for n := 1; n <= 20 && i+n+2 <= len(body); n++ {
			dl := binary.LittleEndian.Uint16(body[i+n : i+n+2])
			if dl != PageSize || i+n+2+PageSize > len(body) {
				continue
			}
			page := body[i+n+2 : i+n+2+PageSize]
			if !looksLikePostgresPage(page) {
				continue
			}
			rel, fork, block := guessRelForkBlock(body[i : i+n+2])
			if rel == 0 || block > maxSensibleBlocks {
				i = i + n + 2 + PageSize
				found = true
				break
			}
			cp := make([]byte, PageSize)
			copy(cp, page)
			out = append(out, FPIRecord{
				RelFilenode: rel,
				Fork:        fork,
				Block:       block,
				LSN:         lsn,
				Page:        cp,
			})
			i = i + n + 2 + PageSize
			found = true
			break
		}
		if !found {
			i++
		}
	}
	return out
}

func looksLikePostgresPage(page []byte) bool {
	if len(page) != PageSize {
		return false
	}
	psv := binary.LittleEndian.Uint16(page[18:20])
	if psv&0xFF00 != 0x2000 {
		return false
	}
	lower := binary.LittleEndian.Uint16(page[12:14])
	upper := binary.LittleEndian.Uint16(page[14:16])
	special := binary.LittleEndian.Uint16(page[16:18])
	if lower > upper || upper > special || int(special) > PageSize {
		return false
	}
	return true
}

func guessRelForkBlock(hdr []byte) (rel, fork, block uint32) {
	if len(hdr) >= 12 {
		rel = binary.LittleEndian.Uint32(hdr[0:4])
		fork = binary.LittleEndian.Uint32(hdr[4:8])
		block = binary.LittleEndian.Uint32(hdr[8:12])
	} else if len(hdr) >= 4 {
		rel = binary.LittleEndian.Uint32(hdr[0:4])
	}
	if rel == 0 || block > maxSensibleBlocks || fork > 3 {
		return 0, 0, 0
	}
	return rel, fork, block
}

func isZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

// BuildSyntheticFPIRecord builds a minimal block-header+image body for tests.
func BuildSyntheticFPIRecord(dbOid, rel, fork, block uint32, page []byte) []byte {
	if len(page) != PageSize {
		panic("page size")
	}
	buf := make([]byte, 0, 4+5+12+4+2+PageSize)
	buf = append(buf, 0)
	buf = append(buf, byte(bkpBlockHasImage)|byte(fork&0x0f))
	buf = append(buf, 0, 0)
	imgHdr := make([]byte, 5)
	binary.LittleEndian.PutUint16(imgHdr[0:2], uint16(PageSize))
	imgHdr[4] = bkpImageApply // not compressed, no hole
	buf = append(buf, imgHdr...)
	rnode := make([]byte, 12)
	binary.LittleEndian.PutUint32(rnode[0:4], 1663)
	binary.LittleEndian.PutUint32(rnode[4:8], dbOid)
	binary.LittleEndian.PutUint32(rnode[8:12], rel)
	buf = append(buf, rnode...)
	blk := make([]byte, 4)
	binary.LittleEndian.PutUint32(blk, block)
	buf = append(buf, blk...)
	buf = append(buf, 255, 0)
	buf = append(buf, page...)
	return buf
}
