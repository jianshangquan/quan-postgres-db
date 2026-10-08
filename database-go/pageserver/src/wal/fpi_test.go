package wal

import (
	"bytes"
	"testing"
)

func TestFPIRoundtrip(t *testing.T) {
	page := bytes.Repeat([]byte{0xAB}, PageSize)
	enc, err := EncodeFPI(FPIRecord{
		DBOid: 5, RelFilenode: 16384, Fork: 0, Block: 1, LSN: 0x1000, Page: page,
	})
	if err != nil {
		t.Fatal(err)
	}
	recs, rest, err := DecodeRecords(enc)
	if err != nil || len(rest) != 0 || len(recs) != 1 {
		t.Fatalf("decode: n=%d rest=%d err=%v", len(recs), len(rest), err)
	}
	if !bytes.Equal(recs[0].Page, page) || recs[0].LSN != 0x1000 || recs[0].DBOid != 5 {
		t.Fatalf("rec: %+v", recs[0])
	}
}
