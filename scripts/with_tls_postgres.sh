#!/usr/bin/env sh
# Runs a command against two throwaway PostgreSQL containers: one serving TLS
# with a certificate from a certificate authority generated for this run, and
# one with no TLS at all. Both listen on loopback ports Docker chooses. The
# containers and every key are removed when the command ends.
#
#   ./scripts/with_tls_postgres.sh cargo test -p tollgate-store-postgres --test tls
#
# The command sees:
#   TOLLGATE_PG_TLS_URL        the TLS server, with no sslmode of its own
#   TOLLGATE_PG_TLS_CA         the authority that signed its certificate, which
#                              names 127.0.0.1 and localhost
#   TOLLGATE_PG_TLS_WRONG_CA   an authority that signed nothing the server holds
#   TOLLGATE_PG_PLAINTEXT_URL  the server without TLS
#   TOLLGATE_REQUIRE_PG_TLS=1  so no TLS scenario can pass by skipping
#
# Needs docker and openssl.
set -eu

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <command> [argument...]" >&2
  exit 2
fi

IMAGE=postgres:16-alpine
# A public fixture for databases that live only as long as this script.
PASSWORD=tollgate-tls-fixture-password

directory=$(mktemp -d)
tls="tollgate-pg-tls-$$-$(basename "$directory" | tr -cd 'A-Za-z0-9')"
plaintext="$tls-plaintext"

# shellcheck disable=SC2329 # invoked by the traps below
cleanup() {
  # A container that never started cannot be removed; anything else that
  # fails is worth reading.
  for name in "$tls" "$plaintext"; do
    if docker inspect "$name" > /dev/null 2>&1; then
      docker rm --force --volumes "$name" > /dev/null ||
        echo "with_tls_postgres: could not remove container $name" >&2
    fi
  done
  rm -rf "$directory"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

cat > "$directory/request.cnf" <<'EOF'
[req]
distinguished_name = name
[name]
[authority]
basicConstraints = critical, CA:TRUE
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
[server]
basicConstraints = CA:FALSE
keyUsage = critical, digitalSignature
extendedKeyUsage = serverAuth
subjectAltName = DNS:localhost, IP:127.0.0.1
EOF

# openssl reports progress on stderr even when it succeeds, so its output is
# shown only on failure.
quietly() {
  if ! "$@" > "$directory/openssl.log" 2>&1; then
    cat "$directory/openssl.log" >&2
    exit 1
  fi
}

authority() {
  quietly openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
    -days 2 -subj "/CN=$2" -config "$directory/request.cnf" -extensions authority \
    -keyout "$directory/$1.key" -out "$directory/$1.pem"
}

authority ca "tollgate test authority"
authority wrong-ca "tollgate unrelated authority"
quietly openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -subj "/CN=tollgate test database" -config "$directory/request.cnf" \
  -keyout "$directory/server.key" -out "$directory/server.csr"
quietly openssl x509 -req -in "$directory/server.csr" -days 2 \
  -CA "$directory/ca.pem" -CAkey "$directory/ca.key" -set_serial 1 \
  -extfile "$directory/request.cnf" -extensions server -out "$directory/server.pem"

# PostgreSQL refuses a key it does not own, so the container copies it first.
docker run --detach --name "$tls" \
  --env POSTGRES_USER=tollgate --env POSTGRES_PASSWORD="$PASSWORD" --env POSTGRES_DB=tollgate \
  --publish 127.0.0.1::5432 \
  --volume "$directory:/fixtures:ro" \
  --entrypoint sh "$IMAGE" -c '
    set -e
    install -o postgres -g postgres -m 600 /fixtures/server.key /var/lib/postgresql/server.key
    install -o postgres -g postgres -m 644 /fixtures/server.pem /var/lib/postgresql/server.pem
    exec docker-entrypoint.sh postgres -c ssl=on \
      -c ssl_cert_file=/var/lib/postgresql/server.pem \
      -c ssl_key_file=/var/lib/postgresql/server.key' > /dev/null
docker run --detach --name "$plaintext" \
  --env POSTGRES_USER=tollgate --env POSTGRES_PASSWORD="$PASSWORD" --env POSTGRES_DB=tollgate \
  --publish 127.0.0.1::5432 "$IMAGE" > /dev/null

# The image's first, temporary server listens on its socket only, so a TCP
# answer comes from the one that stays.
wait_for() {
  attempts=0
  until docker exec "$1" pg_isready -h 127.0.0.1 -U tollgate -d tollgate > /dev/null 2>&1; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 120 ]; then
      echo "with_tls_postgres: $1 never became ready" >&2
      docker logs "$1" >&2
      exit 1
    fi
    sleep 0.5
  done
}

url_of() {
  address=$(docker port "$1" 5432/tcp | head -n 1)
  printf 'postgres://tollgate:%s@%s/tollgate' "$PASSWORD" "$address"
}

wait_for "$tls"
wait_for "$plaintext"
tls_url=$(url_of "$tls")
plaintext_url=$(url_of "$plaintext")

status=0
TOLLGATE_PG_TLS_URL="$tls_url" \
  TOLLGATE_PG_TLS_CA="$directory/ca.pem" \
  TOLLGATE_PG_TLS_WRONG_CA="$directory/wrong-ca.pem" \
  TOLLGATE_PG_PLAINTEXT_URL="$plaintext_url" \
  TOLLGATE_REQUIRE_PG_TLS=1 \
  "$@" || status=$?
exit "$status"
