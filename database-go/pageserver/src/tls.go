package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"time"
)

// newHTTPClient returns an HTTP client that trusts TLS_CA_FILE / QUAN_TLS_CA when set.
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

func serveHTTPServer(srv *http.Server) error {
	cert := os.Getenv("TLS_CERT_FILE")
	key := os.Getenv("TLS_KEY_FILE")
	if cert != "" && key != "" {
		return srv.ListenAndServeTLS(cert, key)
	}
	return srv.ListenAndServe()
}
