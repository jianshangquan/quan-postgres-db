#!/usr/bin/env bash
# Generate a shared CA and per-service server certs for compose TLS.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 3650 \
  -subj "/CN=quan-dev-ca"

gen_server() {
  local name="$1"
  shift
  local sans="$*"
  openssl req -newkey rsa:2048 -nodes -keyout "${name}.key" -out "${name}.csr" \
    -subj "/CN=${name}"
  cat > "${name}.ext" <<EOF
subjectAltName=${sans}
extendedKeyUsage=serverAuth,clientAuth
EOF
  openssl x509 -req -in "${name}.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out "${name}.crt" -days 3650 -extfile "${name}.ext"
  rm -f "${name}.csr" "${name}.ext"
}

gen_server pageserver "DNS:pageserver,DNS:localhost,IP:127.0.0.1"
gen_server pageserver-2 "DNS:pageserver-2,DNS:localhost,IP:127.0.0.1"
gen_server safekeeper-1 "DNS:safekeeper-1,DNS:safekeeper,DNS:localhost,IP:127.0.0.1"
gen_server safekeeper-2 "DNS:safekeeper-2,DNS:localhost,IP:127.0.0.1"
gen_server safekeeper-3 "DNS:safekeeper-3,DNS:localhost,IP:127.0.0.1"

echo "Wrote certs under $ROOT"
ls -1 *.crt *.key
