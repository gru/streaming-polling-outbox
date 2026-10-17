#!/usr/bin/env bash
# Интеграционные тесты доставки (этап 7, requirements.md 15, 20):
# «проверь запись, чтение, доставку, количество сообщений и дубликаты».
#   T0 сборка/запуск KafkaValidator + выбор CDC-arm →
#   T1 запись (Writer 300×256 через PgBouncer) →
#   T2 доставка/чтение/количество/дубликаты — вердикт KafkaValidator
#      (fetchFromWriter, снапшот CDC, §15: точные счётчики из фактических данных) →
#   T3 рестарт валидатора = полная перемотка topic с начала (счётчики восстановлены) →
#   T4 запись в режиме direct (мимо PgBouncer) →
#   T5 rate-limited прогон + сохранение результатов (§13.4, collect-results.sh).
# Выход с кодом 0 — все проверки пройдены.
#
# Использование: integration-delivery.sh [--arm auto|polling|streaming]
#   auto (по умолчанию) — тестировать запущенный CDC; ровно один обязателен.
#   polling|streaming — переключить стенд на arm (другой останавливается;
#   профили изолированы, requirements.md 11).
set -uo pipefail
cd "$(dirname "$0")/../.."

if [[ ! -f .env ]]; then
  echo "ERROR: .env не найден. Скопируйте: cp .env.example .env"
  exit 1
fi
set -a; source .env; set +a

DC="docker compose"
ARM_ARG="auto"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --arm) ARM_ARG="${2:-}"; shift 2 ;;
    *) echo "неизвестный аргумент: $1"; exit 1 ;;
  esac
done
[[ "$ARM_ARG" == auto || "$ARM_ARG" == polling || "$ARM_ARG" == streaming ]] \
  || { echo "--arm должен быть auto|polling|streaming"; exit 1; }

WPORT="${WRITER_HTTP_PORT:-18080}"
WBASE="http://localhost:$WPORT"
VPORT="${KAFKA_VALIDATOR_HTTP_PORT:-18083}"
VBASE="http://localhost:$VPORT"
PPORT="${CDC_POLLING_HTTP_PORT:-18081}"
SPORT="${CDC_STREAMING_HTTP_PORT:-18082}"
PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); printf '  \033[32m✔\033[0m %s\n' "$1"; }
bad() { FAIL=$((FAIL+1)); printf '  \033[31m✘\033[0m %s\n' "$1"; }
section() { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }

psql_primary() {
  $DC exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" pg1 \
    psql -h haproxy -U postgres -d outbox -v ON_ERROR_STOP=1 "$@"
}

# ---------- helpers ----------

writer_start() { # json-спека → код HTTP на stdout, ответ в /tmp/int-start.json
  curl -s -o /tmp/int-start.json -w '%{http_code}' --max-time 10 -X POST "$WBASE/api/generation/start" \
    -H 'Content-Type: application/json' -d "$1"
}
wait_writer_finished() {
  for _ in $(seq 1 90); do
    local s; s=$(curl -s --max-time 5 "$WBASE/api/generation/state" | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])' 2>/dev/null)
    if [[ -n "$s" && "$s" != "running" && "$s" != "stopping" ]]; then echo "$s"; return 0; fi
    sleep 2
  done
  echo "timeout"; return 1
}
vget() { curl -s --max-time 8 "$VBASE$1"; }
vfield() { python3 -c "import json; d=json.load(open('$1')); print($2)" 2>/dev/null; }

wait_validator_ready() {
  for _ in $(seq 1 45); do
    [[ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 4 "$VBASE/ready")" == "200" ]] && return 0
    sleep 2
  done
  return 1
}
# ждём, пока валидатор насчитает expected уникальных id прогона (перемотка/догон)
wait_run_unique() { # runId expected → unique на stdout
  local u=""
  for _ in $(seq 1 60); do
    u=$(vget "/api/validation/runs/$1" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("uniqueIds", 0))' 2>/dev/null)
    [[ "$u" == "$2" ]] && break
    sleep 3
  done
  echo "${u:-0}"
}
validate_run() { # runId fetchFromWriter → код HTTP на stdout, вердикт в /tmp/int-verdict.json
  curl -s -o /tmp/int-verdict.json -w '%{http_code}' --max-time 20 -X POST "$VBASE/api/validation/validate" \
    -H 'Content-Type: application/json' -d "{\"runId\":\"$1\",\"fetchFromWriter\":$2}"
}
vd() { vfield /tmp/int-verdict.json "$1"; } # поле вердикта

cdc_state_field() { # поле /api/cdc/state текущего arm
  curl -s --max-time 5 "http://localhost:$1/api/cdc/state" | python3 -c "import json,sys; print(json.load(sys.stdin).get('$2'))" 2>/dev/null
}

# Проверки вердикта безотказного прогона (§15): количество/уникальные/дубликаты/
# потери; ambiguous-коммитов у безотказного прогона нет
assert_verdict_ok() { # expected_total expected_written
  local total=$1 written=$2
  [[ "$(vd 'd["verdict"]')" == "pass" ]] && ok "вердикт: pass" || bad "вердикт: $(vd 'd["verdict"]')"
  [[ "$(vd 'd["kafka"]["records"]')" == "$total" ]] && ok "kafka.records=$total" || bad "kafka.records=$(vd 'd["kafka"]["records"]') (ожидалось $total)"
  [[ "$(vd 'd["kafka"]["uniqueIds"]')" == "$written" ]] && ok "уникальных id=$written (§15)" || bad "uniqueIds=$(vd 'd["kafka"]["uniqueIds"]') (ожидалось $written)"
  [[ "$(vd 'd["kafka"]["duplicateRecords"]')" == "0" ]] && ok "дубликатов=0 (в безотказном прогоне их быть не должно)" \
    || bad "дубликатов=$(vd 'd["kafka"]["duplicateRecords"]') (допустимы, но в безотказном прогоне = 0)"
  [[ "$(vd 'd["missingWrittenIdsCount"]')" == "0" ]] && ok "отсутствующих id=0 (§15)" || bad "отсутствующих=$(vd 'd["missingWrittenIdsCount"]')"
  [[ "$(vd 'd["unexpectedExtraIdsCount"]')" == "0" ]] && ok "неожидаемых лишних id=0" || bad "лишних=$(vd 'd["unexpectedExtraIdsCount"]')"
  [[ "$(vd 'd["ambiguousDeliveredCount"]')" == "0" && "$(vd 'd["ambiguousNotDeliveredCount"]')" == "0" ]] \
    && ok "ambiguous-коммитов нет (безотказный прогон)" || bad "ambiguous: delivered=$(vd 'd["ambiguousDeliveredCount"]') notDelivered=$(vd 'd["ambiguousNotDeliveredCount"]')"
}

# ---------- T0: arm + сборка/запуск валидатора ----------

section "T0. CDC-arm и KafkaValidator: сборка и запуск (этап 7, §15)"

polling_running=0 streaming_running=0
[[ "$($DC ps cdc-polling --format '{{.State}}' 2>/dev/null)" == "running" ]] && polling_running=1
[[ "$($DC ps cdc-streaming --format '{{.State}}' 2>/dev/null)" == "running" ]] && streaming_running=1

if [[ "$ARM_ARG" == auto ]]; then
  if [[ $polling_running == 1 && $streaming_running == 1 ]]; then
    bad "запущены ОБА CDC-профиля (запрещено, requirements.md 11); остановите один"
    printf '\n\033[1mИТОГ: PASS=%d FAIL=%d\033[0m\n' "$PASS" "$FAIL"; exit 1
  fi
  if [[ $polling_running == 0 && $streaming_running == 0 ]]; then
    bad "не запущен ни один CDC; запустите: docker compose --profile streaming up -d --no-deps cdc-streaming"
    printf '\n\033[1mИТОГ: PASS=%d FAIL=%d\033[0m\n' "$PASS" "$FAIL"; exit 1
  fi
  ARM=$([[ $polling_running == 1 ]] && echo polling || echo streaming)
  ok "arm=auto → $ARM (ровно один CDC запущен, §11)"
else
  ARM="$ARM_ARG"
  other=$([[ "$ARM" == polling ]] && echo streaming || echo polling)
  arm_var="${ARM}_running" other_var="${other}_running"
  if [[ "${!other_var}" == "1" ]]; then
    $DC stop "cdc-$other" >/dev/null 2>&1 && ok "cdc-$other остановлен (изоляция профилей, §11)" || bad "не удалось остановить cdc-$other"
  else
    ok "cdc-$other не запущен"
  fi
  if [[ "${!arm_var}" != "1" ]]; then
    # ВАЖНО: build только нужного сервиса и up с --no-deps — полный up
    # реконсиляцией графа пересоздаст pg-контейнеры и уронит живой Patroni
    $DC build "cdc-$ARM" >/dev/null 2>&1 && $DC --profile "$ARM" up -d --no-deps "cdc-$ARM" >/dev/null 2>&1 \
      && ok "cdc-$ARM собран и запущен (profile $ARM)" || bad "не удалось запустить cdc-$ARM"
    for _ in $(seq 1 45); do
      [[ "$($DC ps "cdc-$ARM" --format '{{.Health}}' 2>/dev/null)" == "healthy" ]] && break
      sleep 2
    done
  fi
  [[ "$($DC ps "cdc-$ARM" --format '{{.Health}}' 2>/dev/null)" == "healthy" ]] \
    && ok "cdc-$ARM healthy" || bad "cdc-$ARM не healthy"
fi
CDC_PORT=$([[ "$ARM" == polling ]] && echo "$PPORT" || echo "$SPORT")
[[ "$(cdc_state_field "$CDC_PORT" state)" == "running" ]] && ok "CDC $ARM: state=running" || bad "CDC $ARM: state=$(cdc_state_field "$CDC_PORT" state)"
delivered_before=$(cdc_state_field "$CDC_PORT" messagesDelivered)

[[ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$WBASE/health")" == "200" ]] \
  && ok "writer /health = 200" || bad "writer /health недоступен"

# KafkaValidator: always-on сервис, но на живом стенде поднимаем явно ( --no-deps)
$DC build kafka-validator >/dev/null 2>&1 && $DC up -d --no-deps kafka-validator >/dev/null 2>&1 \
  && ok "kafka-validator: build + up -d --no-deps" || bad "не удалось собрать/запустить kafka-validator"
for _ in $(seq 1 45); do
  [[ "$($DC ps kafka-validator --format '{{.Health}}' 2>/dev/null)" == "healthy" ]] && break
  sleep 2
done
[[ "$($DC ps kafka-validator --format '{{.Health}}' 2>/dev/null)" == "healthy" ]] \
  && ok "kafka-validator healthy (/health)" || bad "kafka-validator не healthy"
wait_validator_ready && ok "/ready = 200 (шина запущена, consumer группы читает topic)" || bad "/ready не 200 за 90с"
state_json=$(vget /api/validation/state)
runs_before=$(echo "$state_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["runsSeen"])' 2>/dev/null)
echo "  перемотка topic: записей потреблено=$(echo "$state_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["recordsConsumed"])' 2>/dev/null), прогонов в памяти=$runs_before"

# ---------- T1. Запись (Writer через PgBouncer) ----------

section "T1. Запись: Writer 300×256 через PgBouncer (§20 «запись»)"
code=$(writer_start '{"totalMessages":300,"messageSizeBytes":256,"batchSize":50,"parallelism":4}')
[[ "$code" == "202" ]] && ok "start → 202" || bad "start → $code"
run1=$(python3 -c "import json; print(json.load(open('/tmp/int-start.json'))['runId'])")
final=$(wait_writer_finished)
[[ "$final" == "completed" ]] && ok "прогон завершён: completed (runId=$run1)" || bad "Writer: '$final'"
curl -s --max-time 5 "$WBASE/api/generation/results" -o /tmp/int-results.json
wr=$(vfield /tmp/int-results.json 'd["writtenMessages"]'); am=$(vfield /tmp/int-results.json 'd["ambiguousUnresolved"]'); fl=$(vfield /tmp/int-results.json 'd["failedMessages"]')
[[ "$wr" == "300" && "$am" == "0" && "$fl" == "0" ]] && ok "written=300 ambiguous=0 failed=0" || bad "written=$wr ambiguous=$am failed=$fl"
left=$(psql_primary -Atc "SELECT count(*) FROM public.outbox_messages WHERE body->>'runId' = '$run1'")
[[ "$left" == "0" ]] && ok "outbox-таблица пуста (INSERT+DELETE в одной транзакции)" || bad "в outbox осталось $left строк прогона"

# ---------- T2. Доставка/чтение/количество/дубликаты — вердикт валидатора ----------

section "T2. Доставка и сверка: KafkaValidator (§15, §20 «чтение, доставку, количество, дубликаты»)"
u=$(wait_run_unique "$run1" 300)
[[ "$u" == "300" ]] && ok "валидатор прочитал 300 уникальных id прогона" || bad "валидатор насчитал $u/300 (доставка/чтение не сошлись)"
code=$(validate_run "$run1" true)
[[ "$code" == "200" ]] && ok "validate (fetchFromWriter) → 200" || bad "validate → $code: $(cat /tmp/int-verdict.json)"
[[ "$(vd 'd["expected"]["source"]')" == "writer" && "$(vd 'd["expected"]["writtenCount"]')" == "300" ]] \
  && ok "ожидаемый набор взят у Writer (/results/ids, runId совпал)" || bad "expected: $(vd 'd["expected"]')"
assert_verdict_ok 300 300
[[ "$(vd 'd["cdc"]["arm"]')" == "$ARM" ]] && ok "вердикт обогащён снапшотом CDC (arm=$ARM)" || bad "cdc в вердикте: $(vd 'd["cdc"]')"
delivered_after=$(cdc_state_field "$CDC_PORT" messagesDelivered)
python3 -c "import sys; sys.exit(0 if $delivered_after >= ${delivered_before:-0} + 300 else 1)" 2>/dev/null \
  && ok "CDC messagesDelivered вырос: $delivered_before → $delivered_after (≥+300, «подтверждено CDC», §15)" \
  || bad "messagesDelivered $delivered_before → $delivered_after (<+300)"
cp /tmp/int-verdict.json "/tmp/int-verdict-$run1.json"

# ---------- T3. Рестарт валидатора = полная перемотка topic ----------

section "T3. Рестарт KafkaValidator → полная перемотка topic (счётчики восстановлены)"
$DC restart kafka-validator >/dev/null 2>&1
for _ in $(seq 1 45); do
  [[ "$($DC ps kafka-validator --format '{{.Health}}' 2>/dev/null)" == "healthy" ]] && break
  sleep 2
done
wait_validator_ready && ok "валидатор перезапущен и /ready=200" || bad "валидатор не готов после рестарта"
u=$(wait_run_unique "$run1" 300)
[[ "$u" == "300" ]] && ok "после рестарта счётчики прогона восстановлены перемоткой (300 unique)" \
  || bad "после рестарта $u/300 — перемотка не восстановила историю"
runs_after=$(vget /api/validation/state | python3 -c 'import json,sys; print(json.load(sys.stdin)["runsSeen"])' 2>/dev/null)
python3 -c "import sys; sys.exit(0 if $runs_after >= $runs_before else 1)" 2>/dev/null \
  && ok "все прогоны снова в памяти (runs: $runs_before → $runs_after)" || bad "runs после рестарта $runs_after < $runs_before"
code=$(validate_run "$run1" true)
[[ "$code" == "200" && "$(vd 'd["verdict"]')" == "pass" ]] && ok "повторный вердикт после рестарта: pass" || bad "повторная валидация: $code $(vd 'd["verdict"]')"

# ---------- T4. Запись в режиме direct (мимо PgBouncer) ----------

section "T4. Запись direct (мимо PgBouncer, через HAProxy) + валидация"
code=$(writer_start '{"totalMessages":100,"messageSizeBytes":128,"batchSize":25,"parallelism":2,"connectionMode":"direct"}')
[[ "$code" == "202" ]] && ok "start (direct) → 202" || bad "start (direct) → $code"
run4=$(python3 -c "import json; print(json.load(open('/tmp/int-start.json'))['runId'])")
final=$(wait_writer_finished)
[[ "$final" == "completed" ]] && ok "прогон direct завершён: completed" || bad "Writer direct: '$final'"
u=$(wait_run_unique "$run4" 100)
[[ "$u" == "100" ]] && ok "валидатор прочитал 100 уникальных id прогона direct" || bad "валидатор: $u/100"
code=$(validate_run "$run4" true)
[[ "$code" == "200" ]] && ok "validate (direct) → 200" || bad "validate (direct) → $code: $(cat /tmp/int-verdict.json)"
assert_verdict_ok 100 100

# ---------- T5. rate-limited прогон + сохранение результатов (§13.4) ----------

section "T5. Rate-limited прогон 200@50/с + сохранение результатов (§13.4, collect-results.sh)"
code=$(writer_start '{"totalMessages":200,"ratePerSecond":50,"messageSizeBytes":64,"batchSize":10,"parallelism":4}')
[[ "$code" == "202" ]] && ok "start (200@50/с) → 202" || bad "start → $code"
run5=$(python3 -c "import json; print(json.load(open('/tmp/int-start.json'))['runId'])")
final=$(wait_writer_finished)
[[ "$final" == "completed" ]] && ok "rate-limited прогон завершён: completed" || bad "Writer: '$final'"
u=$(wait_run_unique "$run5" 200)
[[ "$u" == "200" ]] && ok "валидатор прочитал 200 уникальных id" || bad "валидатор: $u/200"
code=$(validate_run "$run5" true)
[[ "$code" == "200" ]] && ok "validate → 200" || bad "validate → $code: $(cat /tmp/int-verdict.json)"
assert_verdict_ok 200 200
cp /tmp/int-verdict.json "/tmp/int-verdict-$run5.json"

# артефакты §13.4: result.json + timeseries.csv; вердикт валидатора кладём рядом
collector_out=$(./scripts/reports/collect-results.sh "$run5" --arm "$ARM" --notes "integration T5 (этап 7)" 2>&1)
echo "$collector_out" | sed 's/^/  /'
artifact_dir=$(echo "$collector_out" | grep '^артефакты:' | sed 's/^артефакты: //; s|/result\.json.*||')
if [[ -n "$artifact_dir" && -f "$artifact_dir/result.json" && -f "$artifact_dir/timeseries.csv" ]]; then
  ok "артефакты сохранены: $artifact_dir"
else
  bad "артефакты §13.4 не найдены (${artifact_dir:-пусто})"
fi
if [[ -n "$artifact_dir" && -f "$artifact_dir/result.json" ]]; then
  cr_missing=$(vfield "$artifact_dir/result.json" 'd["kafka"]["missing"]')
  cr_unique=$(vfield "$artifact_dir/result.json" 'd["kafka"]["uniqueIds"]')
  [[ "${cr_missing:-x}" == "0" && "${cr_unique:-x}" == "200" ]] \
    && ok "result.json: missing=0, uniqueIds=200 (независимый пересчёт коллектора совпал)" \
    || bad "result.json: missing=$cr_missing uniqueIds=$cr_unique"
  cp "/tmp/int-verdict-$run5.json" "$artifact_dir/validation-verdict.json" \
    && ok "вердикт валидатора сохранён рядом: validation-verdict.json" || true
fi

printf '\n\033[1mИТОГ: PASS=%d FAIL=%d\033[0m\n' "$PASS" "$FAIL"
[[ "$FAIL" -eq 0 ]]
