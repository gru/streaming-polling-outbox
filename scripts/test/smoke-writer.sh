#!/usr/bin/env bash
# Живая проверка Writer (этап 3, requirements.md раздел 6):
#   сборка → старт в Idle → прогон через PgBouncer → сверка с БД → метрики →
#   stop по команде → рестарт (снова Idle) → диагностический прогон direct.
# Выход с кодом 0 — все проверки пройдены.
set -uo pipefail
cd "$(dirname "$0")/../.."

if [[ ! -f .env ]]; then
  echo "ERROR: .env не найден. Скопируйте: cp .env.example .env"
  exit 1
fi
set -a; source .env; set +a

DC="docker compose"
WPORT="${WRITER_HTTP_PORT:-18080}"
BASE="http://localhost:$WPORT"
PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); printf '  \033[32m✔\033[0m %s\n' "$1"; }
bad() { FAIL=$((FAIL+1)); printf '  \033[31m✘\033[0m %s\n' "$1"; }
section() { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }

# psql к актуальному primary (через HAProxy) из контейнера pg1
psql_primary() {
  $DC exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" pg1 \
    psql -h haproxy -U postgres -d postgres -v ON_ERROR_STOP=1 "$@"
}

api() { # method path [json-body] → код HTTP
  local method=$1 path=$2 body=${3:-}
  if [[ -n "$body" ]]; then
    curl -s -o /tmp/smoke-writer-resp.json -w '%{http_code}' --max-time 10 \
      -X "$method" "$BASE$path" -H 'Content-Type: application/json' -d "$body"
  else
    curl -s -o /tmp/smoke-writer-resp.json -w '%{http_code}' --max-time 10 -X "$method" "$BASE$path"
  fi
}
jfield() { python3 -c "import json,sys; d=json.load(open('/tmp/smoke-writer-resp.json')); print($1)" 2>/dev/null; }
get_state() {
  curl -s --max-time 5 "$BASE/api/generation/state" | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])' 2>/dev/null
}
wait_healthy() {
  for _ in $(seq 1 45); do
    [[ "$($DC ps writer --format '{{.Health}}' 2>/dev/null)" == "healthy" ]] && return 0
    sleep 2
  done
  return 1
}
wait_finished() { # ждём state вне running|stopping; выводит финальный state
  for _ in $(seq 1 90); do
    local s; s=$(get_state)
    if [[ -n "$s" && "$s" != "running" && "$s" != "stopping" ]]; then echo "$s"; return 0; fi
    sleep 2
  done
  echo "timeout"; return 1
}

section "Writer: сборка и запуск"
# ВАЖНО: build только writer и up с --no-deps — иначе compose реконсилярует весь
# граф зависимостей (haproxy -> pg1-3) и ПЕРЕСОЗДАЁТ pg-контейнеры, одновременно
# роняя Patroni-кластер (флап промоушенов, рассинхрон failover-слотов).
$DC build writer >/dev/null 2>&1 && $DC up -d --no-deps writer >/dev/null 2>&1 \
  && ok "docker compose build writer && up -d --no-deps writer" || bad "не удалось собрать/запустить writer"
if wait_healthy; then ok "writer healthy (/health)"; else bad "writer не стал healthy за 90s"; fi
# перезагрузить конфиг Prometheus, чтобы подхватился job writer
$DC exec -T prometheus kill -HUP 1 >/dev/null 2>&1 || $DC restart prometheus >/dev/null 2>&1
sleep 2

section "Старт в Idle (requirements.md 6.1)"
state=$(get_state)
[[ "$state" == "idle" ]] && ok "state=idle после старта" || bad "ожидался idle, получен '$state'"

section "Прогон через PgBouncer: 500 сообщений × 256B (batch 50, parallelism 4; таблица остаётся пустой)"
peek_count() {
  psql_primary -Atc "SELECT count(*) FROM pg_logical_slot_peek_binary_changes('outbox_slot', NULL, NULL, 'proto_version','1','publication_names','outbox_publication')" 2>/dev/null
}
peek_before=$(peek_count); peek_before=${peek_before:-0}
code=$(api POST /api/generation/start '{"totalMessages":500,"messageSizeBytes":256,"batchSize":50,"parallelism":4}')
[[ "$code" == "202" ]] && ok "POST /start → 202 (runId=$(jfield 'd["runId"]')" || bad "POST /start → $code: $(cat /tmp/smoke-writer-resp.json)"
run_id=$(jfield 'd["runId"]')
final_state=$(wait_finished)
[[ "$final_state" == "completed" ]] && ok "прогон завершился: state=completed" || bad "прогон завершился: '$final_state'"

code=$(api GET /api/generation/results)
written=$(jfield 'd["writtenMessages"]'); ambiguous=$(jfield 'd["ambiguousUnresolved"]'); failed=$(jfield 'd["failedMessages"]')
reason=$(jfield 'd["stopReason"]'); rate=$(jfield 'round(d["actualRatePerSecond"],1)'); cp95=$(jfield 'round(d["commitP95Ms"],2)')
echo "  итоги: written=$written rate=${rate}msg/s commitP95=${cp95}ms ambiguous=$ambiguous failed=$failed reason=$reason"
[[ "$written" == "500" && "$ambiguous" == "0" && "$failed" == "0" && "$reason" == "TargetMessages" ]] \
  && ok "written=500, ambiguous=0, failed=0, stopReason=TargetMessages" \
  || bad "written=$written ambiguous=$ambiguous failed=$failed reason=$reason"

# Writer держит outbox пустым: INSERT+DELETE в одной транзакции — строки не остаются,
# но изменения попадают в WAL и видны в слоте (сообщение живёт в WAL, не в таблице)
db_rows=$(psql_primary -Atc "SELECT count(*) FROM public.outbox_messages WHERE body->>'runId' = '$run_id'")
[[ "$db_rows" == "0" ]] && ok "0 строк с runId прогона в таблице (INSERT+DELETE в одной транзакции)" || bad "в таблице $db_rows строк прогона (ожидалось 0)"
total_rows=$(psql_primary -Atc "SELECT count(*) FROM public.outbox_messages")
[[ "$total_rows" == "0" ]] && ok "outbox_messages пуста целиком" || bad "в таблице $total_rows строк (ожидалось 0)"
peek_after=$(peek_count); peek_after=${peek_after:-0}
if [[ "$peek_after" -gt "$peek_before" ]]; then
  ok "изменения прогона в WAL/слоте (peek: $peek_before → $peek_after) при пустой таблице"
else
  bad "peek не вырос ($peek_before → $peek_after) — изменения не попали в слот"
fi

code=$(api GET /api/generation/results/ids)
ids_count=$(jfield 'len(d["writtenIds"])')
amb_ids=$(jfield 'len(d["ambiguousIds"])')
[[ "$ids_count" == "500" && "$amb_ids" == "0" ]] && ok "/results/ids: 500 ожидаемых id, 0 неоднозначных" || bad "/results/ids: written=$ids_count ambiguous=$amb_ids"

section "Остановка по команде (runUntilStopped + rate)"
code=$(api POST /api/generation/start '{"runUntilStopped":true,"ratePerSecond":100,"batchSize":10,"messageSizeBytes":64}')
[[ "$code" == "202" ]] && ok "start (runUntilStopped) → 202" || bad "start → $code"
sleep 3
code=$(api POST /api/generation/stop)
[[ "$code" == "200" ]] && ok "POST /stop → 200" || bad "POST /stop → $code: $(cat /tmp/smoke-writer-resp.json)"
stop_state=$(jfield 'd["state"]'); stop_written=$(jfield 'd["writtenMessages"]')
[[ "$stop_state" == "stopped" ]] && ok "state=stopped после команды (written=$stop_written)" || bad "state после stop: '$stop_state'"
code=$(api POST /api/generation/stop)
[[ "$code" == "409" ]] && ok "повторный /stop → 409 (нет активного прогона)" || bad "повторный /stop → $code"

section "Метрики (/metrics, Prometheus)"
metrics=$(curl -s --max-time 5 "$BASE/metrics")
echo "$metrics" | grep -q '^# HELP writer_messages_written' || echo "$metrics" | grep -q 'writer_messages_written_total' \
  && ok "writer_messages_written_total присутствует" || bad "writer_messages_written_total не найден"
echo "$metrics" | grep -q 'writer_commit_duration' && ok "writer_commit_duration (histogram) присутствует" || bad "writer_commit_duration не найден"
echo "$metrics" | grep -q 'writer_generation_active' && ok "writer_generation_active (gauge) присутствует" || bad "writer_generation_active не найден"
ready=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$BASE/ready")
[[ "$ready" == "200" ]] && ok "/ready = 200" || bad "/ready = $ready"
for _ in $(seq 1 15); do
  targets=$(curl -s --max-time 5 http://localhost:9090/api/v1/targets | python3 -c '
import json, sys
t = json.load(sys.stdin)["data"]["activeTargets"]
w = [x for x in t if x["scrapeUrl"].endswith("writer:8080/metrics")]
print("%d %s" % (len(w), w[0]["health"] if w else "missing"))' 2>/dev/null)
  [[ "$targets" == "1 up" ]] && break
  sleep 2
done
[[ "$targets" == "1 up" ]] && ok "Prometheus скрапит writer:8080/metrics (health=up)" || bad "таргет writer в Prometheus: '$targets'"

section "Рестарт → снова Idle (состояние in-memory)"
$DC restart writer >/dev/null 2>&1
if wait_healthy; then ok "writer healthy после рестарта"; else bad "writer не поднялся после рестарта"; fi
state=$(get_state)
[[ "$state" == "idle" ]] && ok "state=idle после рестарта" || bad "после рестарта state='$state'"
code=$(api GET /api/generation/results)
[[ "$code" == "404" ]] && ok "результаты прошлого прогона не пережили рестарт (404)" || bad "/results после рестарта → $code"

section "Диагностический прогон direct (мимо PgBouncer, через HAProxy)"
code=$(api POST /api/generation/start '{"totalMessages":50,"messageSizeBytes":128,"batchSize":25,"parallelism":2,"connectionMode":"direct"}')
[[ "$code" == "202" ]] && ok "start (connectionMode=direct) → 202" || bad "start direct → $code"
direct_run_id=$(jfield 'd["runId"]')
final_state=$(wait_finished)
[[ "$final_state" == "completed" ]] && ok "direct-прогон завершён: state=completed" || bad "direct-прогон: '$final_state'"
api GET /api/generation/results >/dev/null
direct_written=$(jfield 'd["writtenMessages"]'); direct_mode=$(jfield 'd["connectionMode"]')
[[ "$direct_written" == "50" && "$direct_mode" == "direct" ]] && ok "direct: written=50, connectionMode=direct" || bad "direct: written=$direct_written mode=$direct_mode"
db_rows=$(psql_primary -Atc "SELECT count(*) FROM public.outbox_messages WHERE body->>'runId' = '$direct_run_id'")
[[ "$db_rows" == "0" ]] && ok "direct: 0 строк в таблице (пустая)" || bad "direct: в таблице $db_rows строк (ожидалось 0)"

printf '\n\033[1mИТОГ: PASS=%d FAIL=%d\033[0m\n' "$PASS" "$FAIL"
[[ "$FAIL" -eq 0 ]]
