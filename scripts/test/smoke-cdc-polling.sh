#!/usr/bin/env bash
# Живая проверка CDC Polling (этап 4, requirements.md 7, 10, 15):
#   сборка → health → положительные проверки sys_app → прогон Writer 500×256 →
#   доставка в Kafka (уникальные id, без потерь) → подтверждение LSN только
#   после delivery → метрики → рестарт (продолжает с confirmed LSN) →
#   настраиваемость batch size через env.
# Выход с кодом 0 — все проверки пройдены.
set -uo pipefail
cd "$(dirname "$0")/../.."

if [[ ! -f .env ]]; then
  echo "ERROR: .env не найден. Скопируйте: cp .env.example .env"
  exit 1
fi
set -a; source .env; set +a

DC="docker compose"
CPORT="${CDC_POLLING_HTTP_PORT:-18081}"
CBASE="http://localhost:$CPORT"
WPORT="${WRITER_HTTP_PORT:-18080}"
WBASE="http://localhost:$WPORT"
PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); printf '  \033[32m✔\033[0m %s\n' "$1"; }
bad() { FAIL=$((FAIL+1)); printf '  \033[31m✘\033[0m %s\n' "$1"; }
section() { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }

psql_primary() {
  $DC exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" pg1 \
    psql -h haproxy -U postgres -d outbox -v ON_ERROR_STOP=1 "$@"
}
psql_sysapp() {
  $DC exec -T -e PGPASSWORD="$SYS_APP_PASSWORD" pg1 \
    psql "host=pgbouncer port=6432 user=sys_app dbname=outbox" -v ON_ERROR_STOP=1 "$@"
}

cdc_get() { # path → тело ответа в /tmp/cdc-resp.json, код HTTP на stdout
  curl -s -o /tmp/cdc-resp.json -w '%{http_code}' --max-time 8 "$CBASE$1"
}
jget() { python3 -c "import json; d=json.load(open('/tmp/cdc-resp.json')); print($1)" 2>/dev/null; }
cdc_field() { # python-выражение над /api/cdc/state
  curl -s --max-time 5 "$CBASE/api/cdc/state" -o /tmp/cdc-resp.json
  jget "$1"
}
wait_healthy() {
  for _ in $(seq 1 45); do
    [[ "$($DC ps cdc-polling --format '{{.Health}}' 2>/dev/null)" == "healthy" ]] && return 0
    sleep 2
  done
  return 1
}
wait_writer_finished() {
  for _ in $(seq 1 90); do
    local s; s=$(curl -s --max-time 5 "$WBASE/api/generation/state" | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])' 2>/dev/null)
    if [[ -n "$s" && "$s" != "running" && "$s" != "stopping" ]]; then echo "$s"; return 0; fi
    sleep 2
  done
  echo "timeout"; return 1
}
# Консьюм всего topic; считаем сообщения прогона: "total unique"
kafka_runid_counts() {
  $DC exec -T kafka1 /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic outbox.events --from-beginning --timeout-ms 8000 \
    > /tmp/cdc-kafka.jsonl 2>/dev/null
  python3 - "$1" <<'PY'
import json, sys
run_id = sys.argv[1]
total, ids = 0, set()
for line in open('/tmp/cdc-kafka.jsonl'):
    line = line.strip()
    if not line:
        continue
    try:
        m = json.loads(line)
    except Exception:
        continue
    if isinstance(m, dict) and isinstance(m.get('body'), dict) and m['body'].get('runId') == run_id:
        total += 1
        ids.add(m.get('id'))
print(f"{total} {len(ids)}")
PY
}

section "CDC Polling: сборка и запуск (profile polling)"
# ВАЖНО: build только cdc-polling и up с --no-deps — иначе compose реконсиляция
# всего графа пересоздаст pg-контейнеры и уронит живой Patroni-кластер.
$DC build cdc-polling >/dev/null 2>&1 && $DC --profile polling up -d --no-deps cdc-polling >/dev/null 2>&1 \
  && ok "build + up -d --no-deps (profile polling)" || bad "не удалось собрать/запустить cdc-polling"
if wait_healthy; then ok "cdc-polling healthy (/health)"; else bad "cdc-polling не стал healthy за 90s"; fi
# перезагрузить конфиг Prometheus, чтобы подхватился job cdc-polling
$DC exec -T prometheus kill -HUP 1 >/dev/null 2>&1 || $DC restart prometheus >/dev/null 2>&1
sleep 2

section "sys_app через SECURITY DEFINER-функции (requirements.md 5.2)"
# status() не захватывает слот для декодирования — безопасен при работающем CDC.
# peek/advance от sys_app проверяет check-infra.sh (при остановленном CDC):
# конкурентный peek с живым CDC даёт 55006 object_in_use.
status=$(psql_sysapp -Atc "SELECT cdc.outbox_slot_status()::text" 2>&1)
[[ "$status" == *'"exists": true'* ]] && ok "cdc.outbox_slot_status() отвечает от sys_app (слот существует)" \
  || bad "cdc.outbox_slot_status() от sys_app: $status"
[[ "$status" == *'"wal_status": "reserved"'* ]] && ok "wal_status=reserved (WAL удерживается)" \
  || bad "wal_status не reserved: $status"
# peek-функция достижима от sys_app: при живом CDC конкурентный peek может либо
# пройти в «окно» между poll'ами (число строк), либо получить 55006 — оба исхода
# доказывают, что функция доступна минимальной роли
peek_busy=$(psql_sysapp -Atc "SELECT count(*) FROM cdc.peek_outbox_changes(10)" 2>&1)
if [[ "$peek_busy" =~ ^[0-9]+$ ]]; then
  ok "cdc.peek_outbox_changes() работает от sys_app (строк: $peek_busy — прошла в окно между poll'ами CDC)"
elif [[ "$peek_busy" == *"55006"* || "$peek_busy" == *"is active"* ]]; then
  ok "cdc.peek_outbox_changes() достижима от sys_app (слот занят CDC — 55006, ожидаемо)"
else
  bad "peek от sys_app при живом CDC: $peek_busy"
fi

section "Прогон Writer 500×256 → доставка CDC в Kafka (at-least-once)"
code=$(curl -s -o /tmp/cdc-start.json -w '%{http_code}' --max-time 10 -X POST "$WBASE/api/generation/start" \
  -H 'Content-Type: application/json' -d '{"totalMessages":500,"messageSizeBytes":256,"batchSize":50,"parallelism":4}')
[[ "$code" == "202" ]] && ok "Writer start → 202" || bad "Writer start → $code"
run_id=$(python3 -c "import json; print(json.load(open('/tmp/cdc-start.json'))['runId'])")
final_state=$(wait_writer_finished)
[[ "$final_state" == "completed" ]] && ok "Writer прогон завершён (runId=$run_id)" || bad "Writer: '$final_state'"

# Ждём, пока в Kafka появятся все 500 уникальных id прогона (дубликаты допустимы)
unique="0"; total="0"
for _ in $(seq 1 40); do
  read -r total unique <<< "$(kafka_runid_counts "$run_id")"
  [[ "${unique:-0}" == "500" ]] && break
  sleep 3
done
echo "  в Kafka по runId: всего=$total, уникальных=$unique (дублей=$(( ${total:-0} - ${unique:-0} )))"
[[ "$unique" == "500" ]] && ok "все 500 уникальных id в Kafka — потерь нет" || bad "уникальных id в Kafka: $unique (ожидалось 500)"

state=$(cdc_field 'd["state"]')
[[ "$state" == "running" ]] && ok "CDC state=running" || bad "CDC state='$state'"
delivered=$(cdc_field 'd["messagesDelivered"]')
kafka_errs=$(cdc_field 'd["kafkaErrors"]')
read_errs=$(cdc_field 'd["readErrors"]')
incomplete=$(cdc_field 'd["incompleteTransactionEvents"]')
[[ "${delivered:-0}" -ge 500 ]] && ok "CDC доставил ≥500 сообщений (delivered=$delivered)" || bad "delivered=$delivered"
[[ "$kafka_errs" == "0" && "$read_errs" == "0" && "$incomplete" == "0" ]] \
  && ok "ошибок нет (kafka=$kafka_errs read=$read_errs incomplete=$incomplete)" \
  || bad "kafkaErrors=$kafka_errs readErrors=$read_errs incomplete=$incomplete"

section "Подтверждение LSN — после Kafka delivery (requirements.md 7.2, 10)"
slot_confirmed=$(psql_primary -Atc "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='outbox_slot'")
state_confirmed=$(cdc_field 'd["confirmedLsn"]')
[[ -n "$slot_confirmed" && "$slot_confirmed" == "$state_confirmed" ]] \
  && ok "confirmed_flush_lsn слота == confirmedLsn CDC ($slot_confirmed)" \
  || bad "слот=$slot_confirmed, CDC=$state_confirmed"
# gauge подтверждённого LSN в /metrics совпадает со значением слота
# (строки метрик содержат label otel_scope_name — парсим по префиксу имени)
curl -s --max-time 5 "$CBASE/metrics" -o /tmp/cdc-metrics.txt
metrics_confirmed=$(python3 - "$slot_confirmed" <<'PY'
import re, sys
target = int(sys.argv[1].replace('/', ''), 16)
val = None
for line in open('/tmp/cdc-metrics.txt'):
    m = re.match(r'^cdc_polling_confirmed_lsn\S*\s+([0-9.eE+-]+)', line)
    if m:
        val = float(m.group(1))
print(f"{val} {target}")
PY
)
read -r mval mtarget <<< "$metrics_confirmed"
python3 -c "import sys; sys.exit(0 if abs(float('$mval' or -1) - float('$mtarget')) < 4096.0 else 1)" 2>/dev/null \
  && ok "gauge cdc_polling_confirmed_lsn == confirmed_flush_lsn ($mval)" \
  || bad "gauge confirmed_lsn=$mval, слот=$mtarget"

section "Метрики (/metrics, Prometheus)"
grep -q '^# HELP cdc_polling_messages_delivered' /tmp/cdc-metrics.txt && ok "cdc_polling_messages_delivered_total присутствует" || bad "messages_delivered не найден"
# порядок label'ов у OTel не фиксирован (otel_scope_name может идти первым) —
# ищем строку метрики и в ней label kind
grep '^cdc_polling_records_decoded_total' /tmp/cdc-metrics.txt | grep -q 'kind="I"' \
  && ok "records_decoded{kind=I} (Insert-записи) присутствует" || bad "records_decoded kind=I не найден"
grep '^cdc_polling_records_decoded_total' /tmp/cdc-metrics.txt | grep -q 'kind="D"' \
  && ok "records_decoded{kind=D} (Delete-записи игнорируются, но считаются)" || bad "records_decoded kind=D не найден"
grep -q 'cdc_polling_e2e_latency' /tmp/cdc-metrics.txt && ok "cdc_polling_e2e_latency (histogram) присутствует" || bad "e2e_latency не найден"
grep -q 'cdc_polling_wal_lag_bytes' /tmp/cdc-metrics.txt && ok "cdc_polling_wal_lag_bytes (gauge) присутствует" || bad "wal_lag_bytes не найден"
grep -q 'cdc_polling_state ' /tmp/cdc-metrics.txt && ok "cdc_polling_state (gauge) присутствует" || bad "state gauge не найден"
ready=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$CBASE/ready")
[[ "$ready" == "200" ]] && ok "/ready = 200" || bad "/ready = $ready"
for _ in $(seq 1 15); do
  targets=$(curl -s --max-time 5 http://localhost:9090/api/v1/targets | python3 -c '
import json, sys
t = json.load(sys.stdin)["data"]["activeTargets"]
w = [x for x in t if x["scrapeUrl"].endswith("cdc-polling:8080/metrics")]
print("%d %s" % (len(w), w[0]["health"] if w else "missing"))' 2>/dev/null)
  [[ "$targets" == "1 up" ]] && break
  sleep 2
done
[[ "$targets" == "1 up" ]] && ok "Prometheus скрапит cdc-polling:8080/metrics (health=up)" || bad "таргет cdc-polling: '$targets'"

section "Рестарт → продолжает с confirmed LSN (in-memory счётчики сбрасываются)"
$DC --profile polling restart cdc-polling >/dev/null 2>&1
if wait_healthy; then ok "cdc-polling healthy после рестарта"; else bad "cdc-polling не поднялся после рестарта"; fi
state=$(cdc_field 'd["state"]')
[[ "$state" == "running" ]] && ok "после рестарта state=running" || bad "после рестарта state='$state'"
confirmed_after=$(cdc_field 'd["confirmedLsn"]')
[[ -n "$confirmed_after" && "$confirmed_after" != "None" ]] && ok "позиция прочитана из слота после рестарта (confirmed=$confirmed_after)" || bad "confirmedLsn после рестарта пуст"

section "Настраиваемость: размер пачки через env (requirements.md 7.1)"
# Пересоздание контейнера с другим CDC_POLLING_POLL_BATCH_SIZE — без правки кода
CDC_POLLING_POLL_BATCH_SIZE=50 $DC --profile polling up -d --no-deps cdc-polling >/dev/null 2>&1
if wait_healthy; then ok "cdc-polling пересоздан с CDC_POLLING_POLL_BATCH_SIZE=50"; else bad "не поднялся с batch=50"; fi
batch=$(cdc_field 'd["config"]["pollBatchSize"]')
[[ "$batch" == "50" ]] && ok "pollBatchSize подхвачен из env (=50)" || bad "pollBatchSize=$batch (ожидалось 50)"
# малый прогон — доставка продолжает работать
code=$(curl -s -o /tmp/cdc-start.json -w '%{http_code}' --max-time 10 -X POST "$WBASE/api/generation/start" \
  -H 'Content-Type: application/json' -d '{"totalMessages":100,"messageSizeBytes":128,"batchSize":25,"parallelism":2}')
run2=$(python3 -c "import json; print(json.load(open('/tmp/cdc-start.json'))['runId'])")
wait_writer_finished >/dev/null
unique2="0"
for _ in $(seq 1 30); do
  read -r _t unique2 <<< "$(kafka_runid_counts "$run2")"
  [[ "${unique2:-0}" == "100" ]] && break
  sleep 3
done
[[ "$unique2" == "100" ]] && ok "с batch=50 доставлено 100/100 сообщений малого прогона" || bad "доставлено $unique2/100"
# возврат к дефолту из .env
$DC --profile polling up -d --no-deps cdc-polling >/dev/null 2>&1
wait_healthy >/dev/null
batch=$(cdc_field 'd["config"]["pollBatchSize"]')
[[ "$batch" == "100" ]] && ok "дефолт восстановлен (pollBatchSize=100)" || bad "после отката pollBatchSize=$batch"

printf '\n\033[1mИТОГ: PASS=%d FAIL=%d\033[0m\n' "$PASS" "$FAIL"
[[ "$FAIL" -eq 0 ]]
