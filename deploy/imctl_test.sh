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
# Re-point every path derived by the wrapper at source time into the temporary
# directory, so the real prepare functions never touch /opt.
MYSQL_DATA="${ROOT}/mysql/data"
MYSQL_SECRETS="${ROOT}/mysql/secrets"
MYSQL_ROOT_SECRET="${MYSQL_SECRETS}/mysql-root"
MYSQL_APP_SECRET="${MYSQL_SECRETS}/mysql-app"
CONFIG_DIR="${ROOT}/config"
BACKEND_APP_SECRET="${CONFIG_DIR}/mysql-app"
INFRA_ROOT="${ROOT}/infra-v11"
INFRA_SECRETS="${INFRA_ROOT}/secrets"
INFRA_CONFIG_DIR="${CONFIG_DIR}/infra-v11"
INFRA_REDIS_URL_FILE="${INFRA_CONFIG_DIR}/redis-url"
INFRA_REDIS_PASSWORD_FILE="${INFRA_CONFIG_DIR}/redis-password"
INFRA_RABBITMQ_URL_FILE="${INFRA_CONFIG_DIR}/rabbitmq-url"
INFRA_RABBITMQ_PASSWORD_FILE="${INFRA_CONFIG_DIR}/rabbitmq-password"
mkdir -p "${MYSQL_SECRETS}" "${INFRA_SECRETS}"
printf 'mysql-app-secret\n' > "${MYSQL_APP_SECRET}"
printf 'mysql-root-secret\n' > "${MYSQL_ROOT_SECRET}"
printf 'redis-secret\n' > "${INFRA_SECRETS}/redis-password"
printf 'rabbit-secret\n' > "${INFRA_SECRETS}/rabbitmq-password"
mkdir -p "${APP_ROOT}/bin" "${APP_ROOT}/web"
cat > "${APP_ROOT}/bin/im-server" <<SH
#!/bin/sh
printf '%s\n' "\$*" >> "${TEST_DIR}/server-args"
printf '%s %s %s\n' "\${IM_MESSAGING_NAMESPACE:-}" "\${IM_REDIS_DB:-}" "\${IM_MESSAGING_REQUIRED:-}" >> "${TEST_DIR}/server-env"
exit 0
SH
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
TEST_PORTS=""
TEST_PROCESSES=""
ss() {
  printf 'State Recv-Q Send-Q LocalAddress PeerAddress\n'
  local port
  for port in ${TEST_PORTS}; do
    printf 'LISTEN 0 128 0.0.0.0:%s 0.0.0.0:*\n' "${port}"
  done
}
pgrep() {
  local pid
  for pid in ${TEST_PROCESSES}; do
    printf '%s\n' "${pid}"
  done
}
# The wrapper runs as root in production; in the test it owns the temporary
# files already, so ignore the ownership flags and copy.
install() {
  local source="" target=""
  while [ $# -gt 0 ]; do
    case "$1" in
      -o|-g|-m) shift 2 ;;
      -*) shift ;;
      *)
        if [ -z "${source}" ]; then
          source="$1"
        else
          target="$1"
        fi
        shift
        ;;
    esac
  done
  command cp "${source}" "${target}"
}
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
BACKEND_APP_SECRET="${APP_ROOT}/mysql-app"
printf 'verify-mysql-app\n' > "${BACKEND_APP_SECRET}"
INFRA_CONFIG_DIR="${APP_ROOT}/config/infra-v13-verify"
INFRA_REDIS_URL_FILE="${INFRA_CONFIG_DIR}/redis-url"
INFRA_REDIS_PASSWORD_FILE="${INFRA_CONFIG_DIR}/redis-password"
INFRA_RABBITMQ_URL_FILE="${INFRA_CONFIG_DIR}/rabbitmq-url"
INFRA_RABBITMQ_PASSWORD_FILE="${INFRA_CONFIG_DIR}/rabbitmq-password"
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
rm -f "${TEST_DIR}/active"
: > "${TEST_DIR}/server-args"
: > "${TEST_DIR}/server-env"
backfill_seq >/dev/null
test "$(tr '\n' ' ' < "${TEST_DIR}/server-args")" = '-backfill-seq -backfill-batch 1000 '
test "$(cat "${TEST_DIR}/server-env")" = 'im:v13-verify 1 true'
pass 'seq backfill runs the backend with the seq flag in the verify namespace'
touch "${TEST_DIR}/active"
if backfill_seq >/dev/null 2>&1; then exit 1; fi
rm -f "${TEST_DIR}/active"
pass 'seq backfill refuses to run while the backend is active'
TEST_PORTS="${PORT}"
if backfill_seq >/dev/null 2>&1; then exit 1; fi
TEST_PORTS=""
pass 'seq backfill refuses while the backend port is listening'
TEST_PROCESSES=4242
if backfill_seq >/dev/null 2>&1; then exit 1; fi
TEST_PROCESSES=""
pass 'seq backfill refuses while an im-server process is running'
TEST_PROCESSES="$$"
backfill_seq >/dev/null
TEST_PROCESSES=""
pass 'seq backfill ignores its own process'
INFRA_REDIS_URL_FILE="${TEST_DIR}/missing-redis-url"
INFRA_SECRETS="${TEST_DIR}/missing-infra-secrets"
VERIFY_MODE=0
if backfill_seq >/dev/null 2>&1; then exit 1; fi
VERIFY_MODE=1
INFRA_SECRETS="${INFRA_ROOT}/secrets"
INFRA_REDIS_URL_FILE="${INFRA_CONFIG_DIR}/redis-url"
pass 'seq backfill requires Redis configuration'
# The rebuild drains both persist queues and regenerates Redis, so it needs
# MySQL and RabbitMQ as well, plus the same stopped-backend guards.
: > "${TEST_DIR}/server-args"
: > "${TEST_DIR}/server-env"
unset REBUILD_BATCH REBUILD_QUEUE_TIMEOUT || true
output="$(rebuild_redis)"
test "$(tr '\n' ' ' < "${TEST_DIR}/server-args")" = '-rebuild-redis '
test "$(cat "${TEST_DIR}/server-env")" = 'im:v13-verify 1 true'
grep -q 'Redis rebuild finished' <<<"${output}"
pass 'redis rebuild runs the backend with the rebuild flag in the verify namespace'
: > "${TEST_DIR}/server-args"
REBUILD_BATCH=250 REBUILD_QUEUE_TIMEOUT=5m rebuild_redis >/dev/null
test "$(tr '\n' ' ' < "${TEST_DIR}/server-args")" = '-rebuild-redis -rebuild-batch 250 -rebuild-queue-timeout 5m '
pass 'redis rebuild maps the batch and queue timeout overrides'
touch "${TEST_DIR}/active"
if rebuild_redis >/dev/null 2>&1; then exit 1; fi
rm -f "${TEST_DIR}/active"
pass 'redis rebuild refuses to run while the backend is active'
TEST_PORTS="${PORT}"
if rebuild_redis >/dev/null 2>&1; then exit 1; fi
TEST_PORTS=""
pass 'redis rebuild refuses while the backend port is listening'
TEST_PROCESSES=4242
if rebuild_redis >/dev/null 2>&1; then exit 1; fi
TEST_PROCESSES=""
pass 'redis rebuild refuses while an im-server process is running'
TEST_PROCESSES="$$"
rebuild_redis >/dev/null
TEST_PROCESSES=""
pass 'redis rebuild ignores its own process'
rebuild_config_backup="${INFRA_CONFIG_DIR}"
INFRA_CONFIG_DIR="${TEST_DIR}/rebuild-missing-config"
INFRA_REDIS_URL_FILE="${INFRA_CONFIG_DIR}/redis-url"
INFRA_REDIS_PASSWORD_FILE="${INFRA_CONFIG_DIR}/redis-password"
INFRA_RABBITMQ_URL_FILE="${INFRA_CONFIG_DIR}/rabbitmq-url"
INFRA_RABBITMQ_PASSWORD_FILE="${INFRA_CONFIG_DIR}/rabbitmq-password"
INFRA_SECRETS="${TEST_DIR}/missing-infra-secrets"
if rebuild_redis >/dev/null 2>&1; then exit 1; fi
pass 'redis rebuild requires Redis configuration'
INFRA_SECRETS="${TEST_DIR}/redis-only-secrets"
mkdir -p "${INFRA_SECRETS}"
printf 'redis-only\n' > "${INFRA_SECRETS}/redis-password"
if rebuild_redis >/dev/null 2>&1; then exit 1; fi
pass 'redis rebuild requires RabbitMQ configuration'
INFRA_CONFIG_DIR="${rebuild_config_backup}"
INFRA_REDIS_URL_FILE="${INFRA_CONFIG_DIR}/redis-url"
INFRA_REDIS_PASSWORD_FILE="${INFRA_CONFIG_DIR}/redis-password"
INFRA_RABBITMQ_URL_FILE="${INFRA_CONFIG_DIR}/rabbitmq-url"
INFRA_RABBITMQ_PASSWORD_FILE="${INFRA_CONFIG_DIR}/rabbitmq-password"
INFRA_SECRETS="${INFRA_ROOT}/secrets"
if main rebuild-read >/dev/null 2>&1; then exit 1; fi
if main backfill-read >/dev/null 2>&1; then exit 1; fi
if main backfill-group-read >/dev/null 2>&1; then exit 1; fi
pass 'removed read backfill subcommands are rejected'
usage="$(main unknown-command 2>&1 || true)"
if ! grep -q 'backfill-seq' <<<"${usage}" || ! grep -q 'rebuild-redis' <<<"${usage}" \
  || ! grep -q 'queue-status' <<<"${usage}" \
  || ! grep -q 'dlq-list' <<<"${usage}" || ! grep -q 'dlq-replay' <<<"${usage}" \
  || ! grep -q 'dlq-discard' <<<"${usage}"; then
  echo 'FAIL usage does not list backfill-seq, rebuild-redis, queue-status and the dlq commands'
  exit 1
fi
pass 'usage lists the seq backfill, redis rebuild, queue drain check and the dlq commands'
DI() {
  case "$*" in
    *list_queues*)
      printf 'im.v13-verify.messages.persist\t%s\t0\n' "${TEST_QUEUE_READY:-0}"
      printf 'im.v13-verify.group.messages.persist\t0\t0\n'
      printf 'im.v13-verify.messages.dead\t%s\t0\n' "${TEST_DEAD_READY:-0}"
      printf 'im.v13-verify.group.messages.dead\t0\t0\n'
      ;;
    *) return 1 ;;
  esac
}
queue_status >/dev/null
pass 'queue status reports drained verify queues'
TEST_QUEUE_READY=3
if queue_status >/dev/null 2>&1; then exit 1; fi
pass 'queue status fails while persist messages are pending'
TEST_QUEUE_READY=0
TEST_DEAD_READY=4
queue_status >/dev/null 2>"${TEST_DIR}/queue-warning"
grep -q 'dead letter queues hold 4 message' "${TEST_DIR}/queue-warning"
pass 'queue status warns about dead letters without failing the drain check'
TEST_DEAD_READY=0
DI() { return 1; }
if queue_status >/dev/null 2>&1; then exit 1; fi
pass 'queue status fails when RabbitMQ is unreachable'

: > "${TEST_DIR}/server-args"
dlq_admin list >/dev/null
test "$(tr '\n' ' ' < "${TEST_DIR}/server-args")" = '-dlq-list -dlq-kind all -dlq-id 0 -dlq-limit 100 '
pass 'dlq-list maps to the list flags with defaults'
: > "${TEST_DIR}/server-args"
dlq_admin replay --kind group --id 42 --limit 25 >/dev/null
test "$(tr '\n' ' ' < "${TEST_DIR}/server-args")" = '-dlq-replay -dlq-kind group -dlq-id 42 -dlq-limit 25 '
pass 'dlq-replay maps kind, id and limit'
: > "${TEST_DIR}/server-args"
dlq_admin discard --yes >/dev/null
test "$(tr '\n' ' ' < "${TEST_DIR}/server-args")" = '-dlq-discard -dlq-kind all -dlq-id 0 -dlq-limit 100 -dlq-yes '
pass 'dlq-discard with --yes adds the confirmation flag'
if dlq_admin discard >/dev/null 2>&1; then exit 1; fi
pass 'dlq-discard refuses to run without --yes'
if dlq_admin replay >/dev/null 2>&1; then exit 1; fi
pass 'dlq-replay of every message refuses to run without --yes'
dlq_admin replay --id 7 >/dev/null
pass 'dlq-replay of one message needs no confirmation'
if dlq_admin list --kind both >/dev/null 2>&1; then exit 1; fi
if dlq_admin list --limit 0 >/dev/null 2>&1; then exit 1; fi
if dlq_admin list --limit 10001 >/dev/null 2>&1; then exit 1; fi
if dlq_admin list --id abc >/dev/null 2>&1; then exit 1; fi
if dlq_admin list --bogus >/dev/null 2>&1; then exit 1; fi
if dlq_admin list --kind >/dev/null 2>&1; then exit 1; fi
pass 'dlq commands reject invalid arguments'
# dlq-* must not read or copy the MySQL app secret, and must not need the
# Redis password. Run the real wrapper with both absent.
mysql_app_secret_backup="${MYSQL_APP_SECRET}"
backend_app_secret_backup="${BACKEND_APP_SECRET}"
infra_secrets_backup="${INFRA_SECRETS}"
rabbitmq_url_backup="${INFRA_RABBITMQ_URL_FILE}"
rabbitmq_password_backup="${INFRA_RABBITMQ_PASSWORD_FILE}"
INFRA_SECRETS="${TEST_DIR}/rabbit-only-secrets"
mkdir -p "${INFRA_SECRETS}"
printf 'rabbit-only\n' > "${INFRA_SECRETS}/rabbitmq-password"
MYSQL_APP_SECRET="${TEST_DIR}/absent/mysql-app"
BACKEND_APP_SECRET="${TEST_DIR}/absent/backend-mysql-app"
rm -f "${INFRA_RABBITMQ_URL_FILE}" "${INFRA_RABBITMQ_PASSWORD_FILE}"
rm -f "${INFRA_REDIS_URL_FILE}" "${INFRA_REDIS_PASSWORD_FILE}"
VERIFY_MODE=1
dlq_admin list >/dev/null
test -s "${INFRA_RABBITMQ_URL_FILE}"
test -s "${INFRA_RABBITMQ_PASSWORD_FILE}"
test ! -e "${BACKEND_APP_SECRET}"
test ! -e "${INFRA_REDIS_PASSWORD_FILE}"
pass 'dlq commands use only the RabbitMQ secret'
MYSQL_APP_SECRET="${mysql_app_secret_backup}"
BACKEND_APP_SECRET="${backend_app_secret_backup}"
INFRA_SECRETS="${infra_secrets_backup}"
INFRA_RABBITMQ_URL_FILE="${rabbitmq_url_backup}"
INFRA_RABBITMQ_PASSWORD_FILE="${rabbitmq_password_backup}"
VERIFY_MODE=0
INFRA_SECRETS="${TEST_DIR}/missing-infra-secrets"
INFRA_RABBITMQ_URL_FILE="${TEST_DIR}/missing-rabbitmq-url"
if dlq_admin list >/dev/null 2>&1; then exit 1; fi
VERIFY_MODE=1
INFRA_SECRETS="${INFRA_ROOT}/secrets"
INFRA_RABBITMQ_URL_FILE="${INFRA_CONFIG_DIR}/rabbitmq-url"
pass 'dlq commands require RabbitMQ configuration'
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
