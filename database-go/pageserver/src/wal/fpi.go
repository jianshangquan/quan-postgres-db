package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	MagicFPI1 = "FPI1"
	HeaderLen = 4 + 4 + 4 + 4 + 4 + 8 // magic + db + rel + fork + block + lsn
	PageSize  = 8192
	RecordLen = HeaderLen + PageSize
)

// FPIRecord is a full-page image (synthetic FPI1 or extracted from PG WAL).
type FPIRecord struct {
	DBOid       uint32
	RelFilenode uint32
	Fork        uint32
	Block       uint32
	LSN         uint64
	Page        []byte
}

func EncodeFPI(r FPIRecord) ([]byte, error) {
	if len(r.Page) != PageSize {
		return nil, fmt.Errorf("page must be %d bytes", PageSize)
	}
	buf := make([]byte, RecordLen)
	copy(buf[0:4], MagicFPI1)
	binary.BigEndian.PutUint32(buf[4:8], r.DBOid)
	binary.BigEndian.PutUint32(buf[8:12], r.RelFilenode)
	binary.BigEndian.PutUint32(buf[12:16], r.Fork)
	binary.BigEndian.PutUint32(buf[16:20], r.Block)
	binary.BigEndian.PutUint64(buf[20:28], r.LSN)
	copy(buf[28:], r.Page)
	return buf, nil
}

// DecodeRecords parses consecutive FPI1 records; returns remainder as unknown.
func DecodeRecords(payload []byte) ([]FPIRecord, []byte, error) {
	var fps []FPIRecord
	i := 0
	for i+4 <= len(payload) {
		if string(payload[i:i+4]) != MagicFPI1 {
			return fps, payload[i:], nil
		}
		if i+RecordLen > len(payload) {
			return fps, nil, errors.New("truncated FPI record")
		}
		rec := FPIRecord{
			DBOid:       binary.BigEndian.Uint32(payload[i+4 : i+8]),
			RelFilenode: binary.BigEndian.Uint32(payload[i+8 : i+12]),
			Fork:        binary.BigEndian.Uint32(payload[i+12 : i+16]),
			Block:       binary.BigEndian.Uint32(payload[i+16 : i+20]),
			LSN:         binary.BigEndian.Uint64(payload[i+20 : i+28]),
			Page:        make([]byte, PageSize),
		}
		copy(rec.Page, payload[i+28:i+RecordLen])
		fps = append(fps, rec)
		i += RecordLen
	}
	return fps, nil, nil
}
