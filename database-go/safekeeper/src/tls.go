package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"
)

func newHTTPClient(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if ca := firstEnv("TLS_CA_FILE", "QUAN_TLS_CA"); ca != "" {
		pool, err := loadCAPool(ca)
		if err == nil {
			transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
		}
	}
	return &http.Client{Timeout: timeout, Transport: transport}
}

func loadCAPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certs in %s", path)
	}
	return pool, nil
}

func firstEnv(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

func listenTCP(addr string) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	cert := os.Getenv("TLS_CERT_FILE")
	key := os.Getenv("TLS_KEY_FILE")
	if cert == "" || key == "" {
		return ln, nil
	}
	cfg, err := tlsServerConfig(cert, key)
	if err != nil {
		_ = ln.Close()
		return nil, err
	}
	return tls.NewListener(ln, cfg), nil
}

func tlsServerConfig(certFile, keyFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, nil
}

func listenHTTP(addr string, handler http.Handler) error {
	cert := os.Getenv("TLS_CERT_FILE")
	key := os.Getenv("TLS_KEY_FILE")
	if cert != "" && key != "" {
		return http.ListenAndServeTLS(addr, cert, key, handler)
	}
	return http.ListenAndServe(addr, handler)
}
