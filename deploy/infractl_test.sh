#!/usr/bin/env bash
# Command-stub checks for deploy/infractl. These do not touch real services.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infractl
source "${HERE}/infractl"

failures=0
check() {
  local name="$1"
  shift
  if "$@"; then
    echo "PASS ${name}"
  else
    echo "FAIL ${name}"
    failures=$((failures + 1))
  fi
}

expect_fail() {
  if "$@"; then
    return 1
  fi
  return 0
}

ss() { echo "LISTEN 0 128 127.0.0.1:26379 *:*"; }
check "port_check rejects an occupied port" expect_fail port_check 26379

ss() { echo "LISTEN 0 128 127.0.0.1:1234 *:*"; }
check "port_check accepts a free port" port_check 26379

D() { return 0; }
redis_health() { return 0; }
rabbit_health() { return 0; }
check "health succeeds when both components respond" health

redis_health() { return 1; }
check "health fails when Redis is unavailable" expect_fail health
redis_health() { return 0; }
rabbit_health() { return 1; }
check "health fails when RabbitMQ is unavailable" expect_fail health

order=""
prepare_layout() { order+="layout "; }
runtime_start() { order+="runtime "; }
ensure_images() { order+="images "; }
redis_start() { order+="redis "; }
rabbit_start() { order+="rabbit "; }
order=""
start >/dev/null
check "start prepares layout before runtime, images and brokers" test "${order}" = "layout runtime images redis rabbit "

order=""
rabbit_stop() { order+="rabbit "; }
redis_stop() { order+="redis "; }
runtime_stop() { order+="runtime "; }
stop >/dev/null
check "stop stops brokers before the runtime" test "${order}" = "rabbit redis runtime "

check "unknown command returns usage error" expect_fail main bogus

if grep -q 'instead of a literal' "${HERE}/infractl"; then
  echo "FAIL script contains placeholder text"
  failures=$((failures + 1))
else
  echo "PASS no placeholder text"
fi

if grep -q -- '--requirepass "\$(cat /run/secrets/redis-password)"' "${HERE}/infractl"; then
  echo "PASS Redis password is read from the mounted secret inside the container"
else
  echo "FAIL Redis run command does not read the mounted secret"
  failures=$((failures + 1))
fi

if grep -q 'IM_REDIS_PASSWORD=' "${HERE}/infractl"; then
  echo "FAIL Redis password value is passed through the environment"
  failures=$((failures + 1))
else
  echo "PASS Redis password is not passed through the backend environment"
fi

if grep -q 'RABBITMQ_DEFAULT_' "${HERE}/infractl"; then
  echo "FAIL RabbitMQ credentials are passed through legacy environment variables"
  failures=$((failures + 1))
else
  echo "PASS RabbitMQ credentials are not passed through the environment"
fi

if grep -q '10-defaults.conf,readonly' "${HERE}/infractl"; then
  echo "PASS RabbitMQ default credentials are mounted from a protected config file"
else
  echo "FAIL RabbitMQ default credential config is missing"
  failures=$((failures + 1))
fi

if grep -qE 'im-rebuild-mysql|im-rebuild-verify|im-rebuild-docker' "${HERE}/infractl"; then
  echo "FAIL script references v0.7 runtime unit names"
  failures=$((failures + 1))
else
  echo "PASS script does not control v0.7 runtime units"
fi

if [ "${failures}" -ne 0 ]; then
  echo "infractl checks failed: ${failures}"
  exit 1
fi
echo "infractl checks passed"
