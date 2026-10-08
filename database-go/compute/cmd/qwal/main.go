// qwal pushes one WAL file/chunk to safekeeper over TCP (QWAL).
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

const wireMagic = 0x5157414c

func main() {
	addr := flag.String("safekeeper", env("SAFEKEEPER_ADDR", "127.0.0.1:5454"), "host:port")
	tenant := flag.String("tenant", env("TENANT_ID", "default"), "")
	timeline := flag.String("timeline", env("TIMELINE_ID", "main"), "")
	startLSN := flag.Uint64("start-lsn", 0, "start LSN")
	endLSN := flag.Uint64("end-lsn", 0, "end LSN (default start+len)")
	path := flag.String("file", "", "WAL bytes file (required)")
	flag.Parse()
	if *path == "" {
		fail("-file required")
	}
	payload, err := os.ReadFile(*path)
	if err != nil {
		fail(err.Error())
	}
	if len(payload) == 0 {
		fail("empty payload")
	}
	if *startLSN == 0 {
		fail("-start-lsn required")
	}
	end := *endLSN
	if end == 0 {
		end = *startLSN + uint64(len(payload))
	}
	ack, err := push(*addr, *tenant, *timeline, *startLSN, end, payload)
	if err != nil {
		fail(err.Error())
	}
	fmt.Printf("qwal ack_lsn=%d bytes=%d\n", ack, len(payload))
}

func push(addr, tenant, timeline string, start, end uint64, payload []byte) (uint64, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(60 * time.Second))
	tb := []byte(tenant)
	tlb := []byte(timeline)
	var hdr [28]byte
	binary.BigEndian.PutUint32(hdr[0:4], wireMagic)
	binary.BigEndian.PutUint16(hdr[4:6], uint16(len(tb)))
	binary.BigEndian.PutUint16(hdr[6:8], uint16(len(tlb)))
	binary.BigEndian.PutUint64(hdr[8:16], start)
	binary.BigEndian.PutUint64(hdr[16:24], end)
	binary.BigEndian.PutUint32(hdr[24:28], uint32(len(payload)))
	if _, err := conn.Write(hdr[:]); err != nil {
		return 0, err
	}
	if _, err := conn.Write(tb); err != nil {
		return 0, err
	}
	if _, err := conn.Write(tlb); err != nil {
		return 0, err
	}
	if _, err := conn.Write(payload); err != nil {
		return 0, err
	}
	var ack [8]byte
	if _, err := io.ReadFull(conn, ack[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(ack[:]), nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
