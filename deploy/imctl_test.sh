#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Sourcing must not execute host operations.
source "${SCRIPT_DIR}/imctl"
TEST_DIR="$(mktemp -d "${TMPDIR:-/tmp}/imctl-test.XXXXXX")"
trap 'rm -rf "${TEST_DIR}"' EXIT
ROOT="${TEST_DIR}"
APP_ROOT="${ROOT}/app"
BASELINE="${ROOT}/baseline"
UNIT=im-test
PORT=8199
VERIFY_MODE=0
mkdir -p "${APP_ROOT}/bin" "${APP_ROOT}/web"
printf '#!/bin/sh\nexit 0\n' > "${APP_ROOT}/bin/im-server"
chmod +x "${APP_ROOT}/bin/im-server"
printf 'test page' > "${APP_ROOT}/web/index.html"
printf 'version: test\n' > "${APP_ROOT}/RELEASE"
touch "${BASELINE}" "${TEST_DIR}/calls"
TEST_HEALTH=200
TEST_READY=200
TEST_NETWORK=0
TEST_COUNT=0
pass() { TEST_COUNT=$((TEST_COUNT+1)); printf 'PASS %s\n' "$1"; }
sleep() { :; }
journalctl() { :; }
ss() { printf 'State Recv-Q Send-Q LocalAddress PeerAddress\n'; }
prepare_backend_config() { :; }
curl() {
  local url="${*: -1}"
  if [ "${TEST_NETWORK}" = 1 ]; then return 7; fi
  case "${url}" in */ready) printf '%s' "${TEST_READY}" ;; *) printf '%s' "${TEST_HEALTH}" ;; esac
}
systemctl() {
  case "$1" in
    is-active) test -f "${TEST_DIR}/active" ;;
    is-failed) return 1 ;;
    stop) rm -f "${TEST_DIR}/active"; printf 'stop\n' >> "${TEST_DIR}/calls" ;;
    start) echo 'Must recreate transient units rather than systemctl start' >&2; return 1 ;;
    *) return 0 ;;
  esac
}
systemd-run() {
  test ! -f "${TEST_DIR}/active"
  touch "${TEST_DIR}/active"
  printf 'create\n' >> "${TEST_DIR}/calls"
}
mysql_start() { printf 'dependencies\n' >> "${TEST_DIR}/calls"; }
mysql_stop() { printf 'mysql-stop\n' >> "${TEST_DIR}/calls"; }
runtime_stop() { printf 'runtime-stop\n' >> "${TEST_DIR}/calls"; }
D() { case "$1" in info) return 0 ;; inspect) printf 'true\n' ;; *) return 1 ;; esac; }

restart >/dev/null
test "$(tr '\n' ' ' < "${TEST_DIR}/calls")" = 'dependencies create '
pass 'restart from fully stopped starts dependencies and recreates backend'
restart >/dev/null
test "$(tr '\n' ' ' < "${TEST_DIR}/calls")" = 'dependencies create dependencies stop create '
pass 'restart running backend recreates transient unit without stopping MySQL'
VERIFY_MODE=1
: > "${TEST_DIR}/calls"
stop >/dev/null
test "$(tr '\n' ' ' < "${TEST_DIR}/calls")" = 'stop '
start >/dev/null
test "$(tr '\n' ' ' < "${TEST_DIR}/calls")" = 'stop create '
pass 'verification stop/start leaves shared database and runtime alone'
if main mysql-stop >/dev/null 2>&1; then exit 1; fi
pass 'verification mode rejects shared MySQL stop'
health >/dev/null
pass 'health accepts both 200 responses'
TEST_READY=503
if health >/dev/null 2>&1; then exit 1; fi
pass 'health rejects database 503'
TEST_READY=404
if wait_ready; then exit 1; fi
pass 'ready 404 cannot fall back to healthy process'
TEST_NETWORK=1
if health >/dev/null 2>&1; then exit 1; fi
pass 'health rejects transport failures'
TEST_NETWORK=0
TEST_READY=200
if grep -q 'IM_REDIS_URL_FILE' "${SCRIPT_DIR}/imctl" \
  && grep -q 'IM_REDIS_PASSWORD_FILE' "${SCRIPT_DIR}/imctl" \
  && grep -q 'IM_RABBITMQ_URL_FILE' "${SCRIPT_DIR}/imctl" \
  && grep -q 'IM_RABBITMQ_PASSWORD_FILE' "${SCRIPT_DIR}/imctl"; then
  pass 'backend wiring uses protected Redis and RabbitMQ file configuration'
else
  echo 'FAIL backend wiring is missing protected infrastructure file configuration'
  exit 1
fi

mysql_command() {
  local tool="$1" query="${*: -1}"
  shift
  if [ "${tool}" = mysqldump ]; then
    printf '%s\n' 'CREATE TABLE `users` (' 'id int' ');' 'INSERT INTO `users` VALUES (1);' 'INSERT INTO `users` VALUES (2);' 'CREATE TABLE `schema_migrations` (' 'version text' ');' "INSERT INTO \`schema_migrations\` VALUES ('001');"
    return
  fi
  case "${query}" in
    'CREATE DATABASE '*) printf 'create-scratch\n' >> "${TEST_DIR}/sql-calls" ;;
    'DROP DATABASE '*) [[ "${query}" = 'DROP DATABASE `im_restore_'* ]]; printf 'drop-scratch\n' >> "${TEST_DIR}/sql-calls" ;;
    *information_schema.tables*) printf '2\n' ;;
    *'COUNT(*) FROM `users`'*) printf '%s\n' "${TEST_USER_COUNT:-2}" ;;
    *'COUNT(*) FROM `schema_migrations`'*) printf '1\n' ;;
    'SELECT version '*) printf '001\n' ;;
    *) command cat >/dev/null ;;
  esac
}
VERIFY_MODE=0
: > "${TEST_DIR}/sql-calls"
BACKUP="$(backup)"
test -s "${BACKUP}/SHA256SUMS" && test ! -e "${BACKUP}/.incomplete"
test "$(wc -l < "${BACKUP}/counts.tsv" | tr -d ' ')" = 2
pass 'backup includes complete checksum and row-count manifests'
restore_check "${BACKUP}" >/dev/null
test "$(tr '\n' ' ' < "${TEST_DIR}/sql-calls")" = 'create-scratch drop-scratch '
pass 'restore verifies row counts and removes only its scratch database'
TEST_USER_COUNT=3
if restore_check "${BACKUP}" >/dev/null 2>&1; then exit 1; fi
TEST_USER_COUNT=2
test "$(tr '\n' ' ' < "${TEST_DIR}/sql-calls")" = 'create-scratch drop-scratch create-scratch drop-scratch '
pass 'failed restore count validation also removes its scratch database'
printf -- '-- tampered\n' >> "${BACKUP}/database.sql"
if restore_check "${BACKUP}" >/dev/null 2>&1; then exit 1; fi
test "$(tr '\n' ' ' < "${TEST_DIR}/sql-calls")" = 'create-scratch drop-scratch create-scratch drop-scratch '
pass 'corrupt backup is refused before database creation'
printf 'USE im;\n' >> "${BACKUP}/database.sql"
(cd "${BACKUP}" && find . -type f ! -name SHA256SUMS -print0 | LC_ALL=C sort -z | xargs -0 sha256sum > SHA256SUMS)
if restore_check "${BACKUP}" >/dev/null 2>&1; then exit 1; fi
test "$(tr '\n' ' ' < "${TEST_DIR}/sql-calls")" = 'create-scratch drop-scratch create-scratch drop-scratch '
pass 'even a rehashed dump cannot select the production database'
printf 'Checks: %s\n' "${TEST_COUNT}"
