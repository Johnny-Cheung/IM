#!/usr/bin/env bash
set -u

# 作为 RabbitMQ NotifyClose 自动退出机制之外的第二道保险。
# 建议由 systemd timer 每 30 秒执行一次；连续两次失败才重启，避免瞬时抖动。
container="${GOIM_CONTAINER:-goim-app}"
ready_url="${GOIM_READY_URL:-http://127.0.0.1:8080/ready}"
state_file="${GOIM_WATCHDOG_STATE_FILE:-/run/goim-watchdog.failures}"
log_file="${GOIM_WATCHDOG_LOG_FILE:-/var/log/goim-watchdog.log}"

if curl --fail --silent --show-error --max-time 5 "$ready_url" >/dev/null; then
  rm -f "$state_file"
  exit 0
fi

failures=0
if [[ -r "$state_file" ]]; then
  read -r failures < "$state_file" || failures=0
fi
failures=$((failures + 1))
printf '%s\n' "$failures" > "$state_file"

if (( failures < 2 )); then
  exit 0
fi

printf '[%s] /ready 连续 %d 次失败，重启 %s\n' "$(date -Is)" "$failures" "$container" >> "$log_file"
docker restart "$container" >> "$log_file" 2>&1
rm -f "$state_file"
