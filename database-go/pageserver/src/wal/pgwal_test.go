package wal

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func makeValidHeapPage(fill byte) []byte {
	page := make([]byte, PageSize)
	binary.LittleEndian.PutUint16(page[12:14], 24)
	binary.LittleEndian.PutUint16(page[14:16], 8192)
	binary.LittleEndian.PutUint16(page[16:18], 8192)
	binary.LittleEndian.PutUint16(page[18:20], 0x2000)
	for i := 24; i < PageSize; i++ {
		page[i] = fill
	}
	return page
}

func TestDecodeRecordFPIsDirect(t *testing.T) {
	page := makeValidHeapPage(0xAB)
	body := BuildSyntheticFPIRecord(5, 16384, 0, 2, page)
	fps, ok := decodeRecordFPIs(body, 0x1000)
	if !ok || len(fps) != 1 {
		t.Fatalf("ok=%v n=%d", ok, len(fps))
	}
	if fps[0].DBOid != 5 || fps[0].RelFilenode != 16384 || fps[0].Block != 2 {
		t.Fatalf("ids: %+v", fps[0])
	}
	if !bytes.Equal(fps[0].Page, page) {
		t.Fatal("page mismatch")
	}
}

func TestExtractFPIAcrossXLogPages(t *testing.T) {
	page := makeValidHeapPage(0xCD)
	body := BuildSyntheticFPIRecord(5, 16384, 0, 2, page)
	total := xlRecHeaderSize + len(body) // > 8KiB → spans pages after headers

	// Build two XLog pages with a continuing record.
	seg := make([]byte, 2*pgXLogPageSize)
	writeShortPHD := func(page []byte, addr uint64, info uint16, remLen uint32) {
		binary.LittleEndian.PutUint16(page[0:2], xlpMagicPG17)
		binary.LittleEndian.PutUint16(page[2:4], info)
		binary.LittleEndian.PutUint32(page[4:8], 1) // tli
		binary.LittleEndian.PutUint64(page[8:16], addr)
		binary.LittleEndian.PutUint32(page[16:20], remLen)
	}
	hdr0 := pgXLogShortPageHeader
	rec := make([]byte, total)
	binary.LittleEndian.PutUint32(rec[0:4], uint32(total))
	copy(rec[xlRecHeaderSize:], body)

	space0 := pgXLogPageSize - hdr0
	writeShortPHD(seg[0:], 0x2000, 0, 0)
	copy(seg[hdr0:], rec[:space0])
	// Page 1: continuation of remaining record bytes
	rem := uint32(len(rec) - space0)
	writeShortPHD(seg[pgXLogPageSize:], 0x2000+pgXLogPageSize, xlpContRecord, rem)
	hdr1 := pgXLogShortPageHeader
	copy(seg[pgXLogPageSize+hdr1:], rec[space0:])

	fps, err := ExtractFPIsFromPGWAL(seg, 0x2000, 0x2000)
	if err != nil {
		t.Fatal(err)
	}
	if len(fps) != 1 {
		t.Fatalf("want 1 fpi, got %d", len(fps))
	}
	if !bytes.Equal(fps[0].Page, page) {
		t.Fatal("page mismatch")
	}
}

func TestRestoreHoleyImage(t *testing.T) {
	full := makeValidHeapPage(0xEE)
	holeOff, holeLen := 100, 200
	image := make([]byte, 0, PageSize-holeLen)
	image = append(image, full[:holeOff]...)
	image = append(image, full[holeOff+holeLen:]...)
	got, err := restoreBlockImage(image, true, uint16(holeOff), uint16(holeLen))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[:holeOff], full[:holeOff]) {
		t.Fatal("prefix")
	}
	for i := holeOff; i < holeOff+holeLen; i++ {
		if got[i] != 0 {
			t.Fatalf("hole @%d", i)
		}
	}
}

func TestDecodeHoleyUncompressedFPI(t *testing.T) {
	// PG17: bimg_info = HAS_HOLE|APPLY, hole_length derived as BLCKSZ-imgLen.
	full := makeValidHeapPage(0x42)
	holeOff, holeLen := 284, 2388
	image := append(append([]byte{}, full[:holeOff]...), full[holeOff+holeLen:]...)
	body := make([]byte, 0, 64+len(image))
	body = append(body, 0)                                  // block id
	body = append(body, byte(bkpBlockHasImage))             // fork_flags
	body = append(body, 0, 0)                               // data_length
	imgHdr := make([]byte, 5)
	binary.LittleEndian.PutUint16(imgHdr[0:2], uint16(len(image)))
	binary.LittleEndian.PutUint16(imgHdr[2:4], uint16(holeOff))
	imgHdr[4] = bkpImageHasHole | bkpImageApply
	body = append(body, imgHdr...)
	rnode := make([]byte, 12)
	binary.LittleEndian.PutUint32(rnode[0:4], 1663)
	binary.LittleEndian.PutUint32(rnode[4:8], 5)
	binary.LittleEndian.PutUint32(rnode[8:12], 16384)
	body = append(body, rnode...)
	blk := make([]byte, 4)
	binary.LittleEndian.PutUint32(blk, 278)
	body = append(body, blk...)
	body = append(body, 255, 0) // DATA_SHORT, main len 0
	body = append(body, image...)

	fps, ok := decodeRecordFPIs(body, 0x2000)
	if !ok || len(fps) != 1 {
		t.Fatalf("ok=%v n=%d", ok, len(fps))
	}
	if fps[0].RelFilenode != 16384 || fps[0].Block != 278 {
		t.Fatalf("ids %+v", fps[0])
	}
	if !bytes.Equal(fps[0].Page[:holeOff], full[:holeOff]) {
		t.Fatal("restored prefix mismatch")
	}
}

func TestExtractFromRealSegmentIfPresent(t *testing.T) {
	pgdata := os.Getenv("PGDATA")
	if pgdata == "" {
		pgdata = filepath.Join(os.Getenv("HOME"), "Library/Application Support/Postgres/var-17")
	}
	walDir := filepath.Join(pgdata, "pg_wal")
	entries, err := os.ReadDir(walDir)
	if err != nil {
		t.Skip(err)
	}
	var (
		path    string
		bestMod int64
	)
	for _, e := range entries {
		name := e.Name()
		if len(name) != 24 || name[0] != '0' {
			continue
		}
		info, err := e.Info()
		if err != nil || info.Size() != 16<<20 {
			continue
		}
		if mt := info.ModTime().UnixNano(); path == "" || mt > bestMod {
			bestMod = mt
			path = filepath.Join(walDir, name)
		}
	}
	if path == "" {
		t.Skip("no WAL segments")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Skip(err)
	}
	fps, err := ExtractFPIsFromPGWAL(b, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("segment %s bytes=%d fps=%d", filepath.Base(path), len(b), len(fps))
	if len(fps) == 0 {
		// Newest segment may be sparse; scan recent ones for any FPW.
		for _, e := range entries {
			name := e.Name()
			if len(name) != 24 || name[0] != '0' {
				continue
			}
			bb, err := os.ReadFile(filepath.Join(walDir, name))
			if err != nil || len(bb) != 16<<20 {
				continue
			}
			ff, _ := ExtractFPIsFromPGWAL(bb, 0, 1)
			if len(ff) > 0 {
				t.Logf("found %d FPIs in %s", len(ff), name)
				return
			}
		}
		t.Fatal("expected at least one FPW in recent WAL segments")
	}
}
