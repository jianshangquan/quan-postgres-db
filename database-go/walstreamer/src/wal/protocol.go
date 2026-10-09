package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Wire protocol (TCP) for compute → safekeeper WAL ingest.
//
// Each message:
//
//	u32  magic "QWAL" (0x5157414c)
//	u16  tenant_len
//	u16  timeline_len
//	u64  start_lsn
//	u64  end_lsn
//	u32  payload_len
//	bytes tenant
//	bytes timeline
//	bytes payload
//
// Reply:
//
//	u64  ack_lsn

const (
	WireMagic     = 0x5157414c // "QWAL"
	AuthMagic     = 0x51574155 // "QWAU"
	maxPayloadLen = 64 << 20
	maxIDLen      = 512
	maxTokenLen   = 4096
)

// WriteAuth sends the QWAU token handshake that must precede AppendMsg.
func WriteAuth(w io.Writer, token string) error {
	tb := []byte(token)
	if len(tb) == 0 || len(tb) > maxTokenLen {
		return errors.New("wal wire: bad token length")
	}
	var hdr [6]byte
	binary.BigEndian.PutUint32(hdr[0:4], AuthMagic)
	binary.BigEndian.PutUint16(hdr[4:6], uint16(len(tb)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(tb)
	return err
}

// ReadAuth reads and returns the token from a QWAU handshake.
func ReadAuth(r io.Reader) (string, error) {
	var hdr [6]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return "", err
	}
	if binary.BigEndian.Uint32(hdr[0:4]) != AuthMagic {
		return "", fmt.Errorf("wal wire: bad auth magic %#x", binary.BigEndian.Uint32(hdr[0:4]))
	}
	n := int(binary.BigEndian.Uint16(hdr[4:6]))
	if n == 0 || n > maxTokenLen {
		return "", errors.New("wal wire: bad auth token length")
	}
	tb := make([]byte, n)
	if _, err := io.ReadFull(r, tb); err != nil {
		return "", err
	}
	return string(tb), nil
}

// AppendMsg is one WAL chunk from compute.
type AppendMsg struct {
	TenantID   string
	TimelineID string
	StartLSN   uint64
	EndLSN     uint64
	Payload    []byte
}

// ReadAppendMsg reads one framed WAL append from r.
func ReadAppendMsg(r io.Reader) (AppendMsg, error) {
	var hdr [4 + 2 + 2 + 8 + 8 + 4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return AppendMsg{}, err
	}
	magic := binary.BigEndian.Uint32(hdr[0:4])
	if magic != WireMagic {
		return AppendMsg{}, fmt.Errorf("wal wire: bad magic %#x", magic)
	}
	tenantLen := int(binary.BigEndian.Uint16(hdr[4:6]))
	timelineLen := int(binary.BigEndian.Uint16(hdr[6:8]))
	startLSN := binary.BigEndian.Uint64(hdr[8:16])
	endLSN := binary.BigEndian.Uint64(hdr[16:24])
	payloadLen := int(binary.BigEndian.Uint32(hdr[24:28]))
	if tenantLen == 0 || tenantLen > maxIDLen || timelineLen == 0 || timelineLen > maxIDLen {
		return AppendMsg{}, errors.New("wal wire: bad id length")
	}
	if payloadLen <= 0 || payloadLen > maxPayloadLen {
		return AppendMsg{}, errors.New("wal wire: bad payload length")
	}
	body := make([]byte, tenantLen + timelineLen + payloadLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return AppendMsg{}, err
	}
	o := 0
	tenant := string(body[o : o+tenantLen])
	o += tenantLen
	timeline := string(body[o : o+timelineLen])
	o += timelineLen
	payload := body[o:]
	if endLSN == 0 {
		endLSN = startLSN + uint64(len(payload))
	}
	return AppendMsg{
		TenantID:   tenant,
		TimelineID: timeline,
		StartLSN:   startLSN,
		EndLSN:     endLSN,
		Payload:    payload,
	}, nil
}

// WriteAppendMsg writes one framed WAL append to w.
func WriteAppendMsg(w io.Writer, m AppendMsg) error {
	tb := []byte(m.TenantID)
	tlb := []byte(m.TimelineID)
	if len(tb) == 0 || len(tb) > maxIDLen || len(tlb) == 0 || len(tlb) > maxIDLen {
		return errors.New("wal wire: bad id")
	}
	if len(m.Payload) == 0 || len(m.Payload) > maxPayloadLen {
		return errors.New("wal wire: bad payload")
	}
	var hdr [4 + 2 + 2 + 8 + 8 + 4]byte
	binary.BigEndian.PutUint32(hdr[0:4], WireMagic)
	binary.BigEndian.PutUint16(hdr[4:6], uint16(len(tb)))
	binary.BigEndian.PutUint16(hdr[6:8], uint16(len(tlb)))
	binary.BigEndian.PutUint64(hdr[8:16], m.StartLSN)
	binary.BigEndian.PutUint64(hdr[16:24], m.EndLSN)
	binary.BigEndian.PutUint32(hdr[24:28], uint32(len(m.Payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.Write(tb); err != nil {
		return err
	}
	if _, err := w.Write(tlb); err != nil {
		return err
	}
	_, err := w.Write(m.Payload)
	return err
}

// WriteAck writes the flush LSN reply.
func WriteAck(w io.Writer, ackLSN uint64) error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], ackLSN)
	_, err := w.Write(b[:])
	return err
}

// ReadAck reads the flush LSN reply.
func ReadAck(r io.Reader) (uint64, error) {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b[:]), nil
}
