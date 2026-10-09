package main

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

func checkHTTPToken(r *http.Request, token string) bool {
	if token == "" {
		return false
	}
	got := r.Header.Get("X-Quan-Token")
	if got == "" {
		auth := r.Header.Get("Authorization")
		if strings.HasPrefix(auth, "Bearer ") {
			got = strings.TrimPrefix(auth, "Bearer ")
		}
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1
}

func requireAuth(token string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !checkHTTPToken(r, token) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}
