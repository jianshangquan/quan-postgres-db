package wal

import (
	"encoding/binary"
	"errors"
)

const (
	// PGSegmentSize is Postgres default wal_segment_size (16 MiB).
	PGSegmentSize = 16 * 1024 * 1024
	// PageSize is Postgres BLCKSZ.
	PageSize = 8192
)

// Frame is one durable WAL unit keyed by segment number (from StartLSN).
type Frame struct {
	TenantID   string
	TimelineID string
	StartLSN   uint64
	EndLSN     uint64
	Payload    []byte
}

// SegnoFromLSN maps an LSN to a Postgres-style segment number (16 MiB).
func SegnoFromLSN(lsn uint64) uint64 {
	return lsn / PGSegmentSize
}

// EncodeFrame serializes a Frame for durable storage.
func EncodeFrame(f Frame) []byte {
	tb := []byte(f.TenantID)
	tlb := []byte(f.TimelineID)
	buf := make([]byte, 2+len(tb)+2+len(tlb)+8+8+len(f.Payload))
	o := 0
	binary.BigEndian.PutUint16(buf[o:], uint16(len(tb)))
	o += 2
	copy(buf[o:], tb)
	o += len(tb)
	binary.BigEndian.PutUint16(buf[o:], uint16(len(tlb)))
	o += 2
	copy(buf[o:], tlb)
	o += len(tlb)
	binary.BigEndian.PutUint64(buf[o:], f.StartLSN)
	o += 8
	binary.BigEndian.PutUint64(buf[o:], f.EndLSN)
	o += 8
	copy(buf[o:], f.Payload)
	return buf
}

// DecodeFrame parses an EncodeFrame buffer.
func DecodeFrame(b []byte) (Frame, error) {
	if len(b) < 2+2+8+8 {
		return Frame{}, errors.New("frame too short")
	}
	o := 0
	tl := int(binary.BigEndian.Uint16(b[o:]))
	o += 2
	if o+tl > len(b) {
		return Frame{}, errors.New("bad tenant len")
	}
	tenant := string(b[o : o+tl])
	o += tl
	tll := int(binary.BigEndian.Uint16(b[o:]))
	o += 2
	if o+tll > len(b) {
		return Frame{}, errors.New("bad timeline len")
	}
	timeline := string(b[o : o+tll])
	o += tll
	if o+16 > len(b) {
		return Frame{}, errors.New("missing lsns")
	}
	start := binary.BigEndian.Uint64(b[o:])
	o += 8
	end := binary.BigEndian.Uint64(b[o:])
	o += 8
	payload := make([]byte, len(b)-o)
	copy(payload, b[o:])
	return Frame{
		TenantID:   tenant,
		TimelineID: timeline,
		StartLSN:   start,
		EndLSN:     end,
		Payload:    payload,
	}, nil
}

// PreferWALFrame chooses what to persist for one segno.
// Prefers full archive segments over shorter partials; allows growth/merge.
func PreferWALFrame(existing, incoming Frame) Frame {
	if len(existing.Payload) == 0 {
		return incoming
	}
	if len(incoming.Payload) == 0 {
		return existing
	}
	if len(existing.Payload) >= PGSegmentSize &&
		len(incoming.Payload) < len(existing.Payload) &&
		incoming.EndLSN <= existing.EndLSN {
		return existing
	}
	if incoming.StartLSN <= existing.StartLSN &&
		incoming.EndLSN >= existing.EndLSN &&
		len(incoming.Payload) >= len(existing.Payload) {
		return incoming
	}
	if incoming.StartLSN == existing.StartLSN &&
		incoming.EndLSN >= existing.EndLSN &&
		len(incoming.Payload) >= len(existing.Payload) {
		return incoming
	}
	oldEndByLen := existing.StartLSN + uint64(len(existing.Payload))
	if incoming.StartLSN == existing.EndLSN || incoming.StartLSN == oldEndByLen {
		merged := make([]byte, 0, len(existing.Payload)+len(incoming.Payload))
		merged = append(merged, existing.Payload...)
		merged = append(merged, incoming.Payload...)
		end := existing.EndLSN
		if incoming.EndLSN > end {
			end = incoming.EndLSN
		}
		return Frame{
			TenantID:   incoming.TenantID,
			TimelineID: incoming.TimelineID,
			StartLSN:   existing.StartLSN,
			EndLSN:     end,
			Payload:    merged,
		}
	}
	exSpan := existing.EndLSN - existing.StartLSN
	inSpan := incoming.EndLSN - incoming.StartLSN
	if inSpan > exSpan || (inSpan == exSpan && len(incoming.Payload) > len(existing.Payload)) {
		return incoming
	}
	return existing
}
