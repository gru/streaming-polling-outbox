#!/usr/bin/env bash
# ha-common.sh — разделяемая библиотека HA-харнесса этапа 8 (requirements.md 14).
# Source-ится из ha-suite.sh, НЕ запускается напрямую.
# Ожидает, что вызывающий скрипт уже: cd в корень репо, source .env, определил DC,
# задал WBASE/VBASE/PPORT/SPORT и букает после source: ARM + CDC_PORT.
HA_COMMON_SOURCED=1

ok()  { PASS=$((PASS+1)); printf '  \033[32m✔\033[0m %s\n' "$1"; }
bad() { FAIL=$((FAIL+1)); printf '  \033[31m✘\033[0m %s\n' "$1"; }
section() { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }
finish() {
  printf '\n\033[1mИТОГ: PASS=%d FAIL=%d\033[0m\n' "$PASS" "$FAIL"
  [[ "$FAIL" -eq 0 ]]
}

psql_primary() { # psql к актуальному primary через HAProxy; клиент — из любого ЖИВОГО
  # pg-контейнера (инъекции останавливают контейнеры — pg1 может быть недоступен)
  local via
  for via in pg1 pg2 pg3; do
    if [[ "$($DC ps "$via" --format '{{.State}}' 2>/dev/null)" == "running" ]]; then
      $DC exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" "$via" \
        psql -h haproxy -U postgres -d outbox -v ON_ERROR_STOP=1 "$@"
      return $?
    fi
  done
  return 1
}
psql_node() { # psql локально к конкретной ноде (работает и на replica)
  local node=$1; shift
  $DC exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" "$node" \
    psql -h localhost -U postgres -d outbox -v ON_ERROR_STOP=1 "$@"
}

now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }

# ---------- Patroni REST (8008/8009/8010 опубликованы на хосте; deviations 7б) ----------

patroni_rest_port() { case "$1" in pg1) echo 8008;; pg2) echo 8009;; pg3) echo 8010;; *) echo "";; esac; }

# Состояние кластера: "leader|sync_standby|running_n|members_n" (пустой leader — лидера нет).
# Опрашиваем все три REST-порта: во время failover нода за портом может быть мертва.
cluster_info() {
  local out p
  for p in 8008 8009 8010; do
    out=$(curl -s --max-time 3 "http://localhost:$p/cluster" | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    raise SystemExit
m = d.get("members", [])
leader = next((x["name"] for x in m if x.get("role") == "leader" and x.get("state") == "running"), "")
sync = next((x["name"] for x in m if x.get("role") == "sync_standby" and x.get("state") in ("streaming", "running")), "")
running = sum(1 for x in m if x.get("state") in ("running", "streaming"))
print(f"{leader}|{sync}|{running}|{len(m)}")' 2>/dev/null)
    [[ -n "$out" ]] && { echo "$out"; return 0; }
  done
  echo "||0|0"
}
leader_name()   { cluster_info | cut -d'|' -f1; }
sync_name()     { cluster_info | cut -d'|' -f2; }

# Ждём leader=имя, непустое и != old. На stdout — имя нового лидера (или пусто по таймауту).
wait_leader_change() { # old_leader timeout_s
  local old=$1 timeout=$2 info leader
  for _ in $(seq 1 "$timeout"); do
    info=$(cluster_info); leader=$(echo "$info" | cut -d'|' -f1)
    [[ -n "$leader" && "$leader" != "$old" ]] && { echo "$leader"; return 0; }
    sleep 1
  done
  echo ""; return 1
}
# Все 3 ноды running/streaming (после crash-рестарта нода может реинициализироваться).
wait_cluster_ready() { # timeout_s
  local timeout=$1 info
  for _ in $(seq 1 "$timeout"); do
    info=$(cluster_info)
    [[ "$(echo "$info" | cut -d'|' -f3)" == "3" && -n "$(echo "$info" | cut -d'|' -f1)" ]] && return 0
    sleep 2
  done
  return 1
}
# Ждём лидера (любого) — сразу после failover-инъекции.
wait_any_leader() { # timeout_s → имя лидера
  local timeout=$1 leader
  for _ in $(seq 1 "$timeout"); do
    leader=$(leader_name)
    [[ -n "$leader" ]] && { echo "$leader"; return 0; }
    sleep 1
  done
  echo ""; return 1
}

# Управляемый switchover (basic-auth; POST /switchover). Запрос — на REST КАНДИДАТА:
# реплика-посредник отвечает 503 "Switchover status unknown" (переключение при этом
# планируется через DCS, но исход не подтверждается) — кандидата/лидера отвечает 200.
patroni_switchover() { # leader candidate → HTTP-код
  local port; port=$(patroni_rest_port "$2")
  curl -s -o /tmp/ha-patroni-switchover.json -w '%{http_code}' --max-time 30 \
    -X POST "http://localhost:${port:-8008}/switchover" \
    -u "patroni:${PATRONI_API_PASSWORD:-patroni}" -H 'Content-Type: application/json' \
    -d "{\"leader\":\"$1\",\"candidate\":\"$2\"}"
}

# КРИТИЧНО после смены топологии Patroni (failover/switchover): на новом primary
# synchronized_standby_slots ссылается на СТАРЫЙ набор физических слотов — PG17
# держит логический декодинг (peek и стриминг) до подтверждения flush от ВСЕХ
# перечисленных standby, включая умершие → доставка молча висит в catchup.
# Обновляем список физическими слотами с активным walreceiver (то же делает
# 02-slot.sql при apply-bootstrap, но это быстрее и нужно ДО ожидания доставки).
refresh_sync_slots() { # на stdout — новый список слотов (пусто = ошибка)
  local slots attempt
  sleep 2
  for attempt in $(seq 1 10); do
    slots=$(psql_primary -Atc "SELECT string_agg(slot_name, ',') FROM pg_replication_slots WHERE slot_type='physical' AND active_pid IS NOT NULL" 2>/dev/null)
    if [[ -n "$slots" ]] \
      && psql_primary -Atc "ALTER SYSTEM SET synchronized_standby_slots TO '$slots'" >/dev/null 2>&1 \
      && psql_primary -Atc "SELECT pg_reload_conf()" >/dev/null 2>&1; then
      echo "$slots"; return 0
    fi
    # haproxy конвергирует на нового primary до ~10с после выборов
    sleep 3
  done
  return 1
}

# ---------- Слот (через защищённую функцию на актуальном primary) ----------

# Поле из cdc.outbox_slot_status() primary: exists|active|wal_status|confirmed_flush_lsn|
# restart_lsn|safe_wal_size|synced|current_wal_lsn (ключи как в функции, 04-cdc-polling.sql).
slot_field() {
  psql_primary -Atc "SELECT (cdc.outbox_slot_status() -> '$1')::text" 2>/dev/null \
    | tr -d '"' | tr -d '\n'
}
# Слот синхронизирован (synced=t) на standby-копии. Обязательный минимум для следующего
# failover — synced на текущем sync_standby (именно он станет новым primary).
wait_slot_synced() { # timeout_s → имя ноды с synced-копией (или пусто)
  local timeout=$1 leader sync node synced
  for _ in $(seq 1 $(( timeout / 2 ))); do
    leader=$(leader_name); sync=$(sync_name)
    for node in pg1 pg2 pg3; do
      [[ "$node" == "$leader" || "$node" != "$sync" ]] && continue
      synced=$(psql_node "$node" -Atc "SELECT synced FROM pg_replication_slots WHERE slot_name='outbox_slot'" 2>/dev/null)
      if [[ "$synced" == "t" ]]; then echo "$node (sync_standby)"; return 0; fi
    done
    sleep 2
  done
  echo ""; return 1
}

# ---------- Writer / KafkaValidator / CDC (конвенция integration-delivery.sh) ----------

writer_start() { curl -s -o /tmp/ha-start.json -w '%{http_code}' --max-time 10 \
  -X POST "$WBASE/api/generation/start" -H 'Content-Type: application/json' -d "$1"; }
wait_writer_finished() { # итоговое состояние на stdout; большой таймаут — ретраи при отказах
  local s
  for _ in $(seq 1 240); do
    s=$(curl -s --max-time 5 "$WBASE/api/generation/state" | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])' 2>/dev/null)
    if [[ -n "$s" && "$s" != "running" && "$s" != "stopping" ]]; then echo "$s"; return 0; fi
    sleep 2
  done
  echo "timeout"; return 1
}
writer_field() { python3 -c "import json; d=json.load(open('/tmp/ha-results.json')); print($1)" 2>/dev/null; }
writer_results() { curl -s --max-time 5 "$WBASE/api/generation/results" -o /tmp/ha-results.json; }
writer_state_field() { curl -s --max-time 5 "$WBASE/api/generation/state" | python3 -c "import json,sys; print(json.load(sys.stdin).get('$1'))" 2>/dev/null; }

vget()   { curl -s --max-time 8 "$VBASE$1"; }
vfield() { python3 -c "import json; d=json.load(open('$1')); print($2)" 2>/dev/null; }
wait_validator_ready() {
  for _ in $(seq 1 45); do
    [[ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 4 "$VBASE/ready")" == "200" ]] && return 0
    sleep 2
  done
  return 1
}
wait_run_unique() { # runId expected → unique на stdout
  local u=""
  for _ in $(seq 1 90); do
    u=$(vget "/api/validation/runs/$1" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("uniqueIds", 0))' 2>/dev/null)
    [[ "$u" == "$2" ]] && break
    sleep 3
  done
  echo "${u:-0}"
}
validate_run() { # runId fetchFromWriter → HTTP-код, вердикт в /tmp/ha-verdict.json
  curl -s -o /tmp/ha-verdict.json -w '%{http_code}' --max-time 30 -X POST "$VBASE/api/validation/validate" \
    -H 'Content-Type: application/json' -d "{\"runId\":\"$1\",\"fetchFromWriter\":$2}"
}
vd() { vfield /tmp/ha-verdict.json "$1"; }

# Запустить прогон и дождаться completed. На stdout — runId или "FAIL:<state>:<runId>".
run_writer() { # json-спека
  local code run final
  code=$(writer_start "$1")
  if [[ "$code" != "202" ]]; then echo "FAIL:http$code:"; return 1; fi
  run=$(python3 -c "import json; print(json.load(open('/tmp/ha-start.json'))['runId'])" 2>/dev/null)
  final=$(wait_writer_finished)
  if [[ "$final" == "completed" ]]; then echo "$run"; return 0; fi
  echo "FAIL:$final:$run"; return 1
}

# Вердикт отказоустойчивого прогона (§14+§15): пропусков и «лишних» быть не должно,
# дубликаты ДОПУСТИМЫ (at-least-once, requirements.md 10) — считаются и попадают в отчёт.
# written берётся из вердикта (fetchFromWriter): при отказах PG у Writer возможны
# ambiguous/failed — недоставленный ambiguous потерей не считается (§15).
assert_verdict_pass_dup() { # label
  local label=$1 written dup
  written=$(vd 'd["expected"]["writtenCount"]')
  [[ "$(vd 'd["verdict"]')" == "pass" ]] && ok "$label: вердикт pass" || bad "$label: вердикт $(vd 'd["verdict"]')"
  [[ "$(vd 'd["missingWrittenIdsCount"]')" == "0" ]] && ok "$label: доказанных потерь нет (missing=0, §15)" \
    || bad "$label: missing=$(vd 'd["missingWrittenIdsCount"]')"
  [[ "$(vd 'd["unexpectedExtraIdsCount"]')" == "0" ]] && ok "$label: лишних id нет" || bad "$label: лишних id=$(vd 'd["unexpectedExtraIdsCount"]')"
  dup=$(vd 'd["kafka"]["duplicateRecords"]')
  [[ "$(vd 'd["kafka"]["uniqueIds"]')" == "$written" ]] && ok "$label: уникальных id=$written" \
    || bad "$label: uniqueIds=$(vd 'd["kafka"]["uniqueIds"]') при written=$written"
  ok "$label: дубликатов=$dup (допустимо, at-least-once); ambiguous: доставлено=$(vd 'd["ambiguousDeliveredCount"]') неопределённо=$(vd 'd["ambiguousNotDeliveredCount"]')"
}

# Полный цикл: прогон → дождаться доставки → вердикт. На stdout — runId.
# Использовать только когда writer-прогон — последний завершённый (fetchFromWriter).
deliver_and_validate() { # json-спека label
  local spec=$1 label=$2 run u code
  run=$(run_writer "$spec")
  if [[ "$run" == FAIL:* ]]; then
    bad "$label: прогон не completed ($run)"
    run="${run##*:}"
  else
    ok "$label: прогон completed (runId=${run:0:8})"
  fi
  [[ -n "$run" ]] || { echo ""; return 1; }
  writer_results
  u=$(wait_run_unique "$run" "$(writer_field 'd["writtenMessages"]')")
  code=$(validate_run "$run" true)
  [[ "$code" == "200" ]] && ok "$label: validate → 200" || bad "$label: validate → $code: $(cat /tmp/ha-verdict.json)"
  assert_verdict_pass_dup "$label"
  cp /tmp/ha-verdict.json "/tmp/ha-verdict-$run.json"
  echo "$run"
}

# Полный цикл сверки после теста: дождаться, пока доставлено ВСЁ написанное
# (recovery может завершиться раньше, чем выйдет весь backlog), затем вердикт.
validate_after_delivery() { # run label
  local u written
  writer_results; written=$(writer_field 'd["writtenMessages"]')
  u=$(wait_run_unique "$1" "$written")
  [[ "$u" == "$written" ]] && ok "$2: доставка завершена ($u уникальных при written=$written)" \
    || bad "$2: доставлено $u из written=$written"
  validate_run "$1" true >/dev/null
  assert_verdict_pass_dup "$2"
  cp /tmp/ha-verdict.json "/tmp/ha-verdict-$1.json"
}

# Артефакты §13.4 + ha-report.json рядом. На stdout — ТОЛЬКО каталог артефактов
# (вывод коллектора уходит в stderr — его нельзя захватывать в переменную).
collect_artifacts() { # runId arm notes
  local collector_out artifact_dir
  collector_out=$(./scripts/reports/collect-results.sh "$1" --arm "$2" --notes "$3" 2>&1)
  echo "$collector_out" | sed 's/^/  /' >&2
  artifact_dir=$(echo "$collector_out" | grep '^артефакты:' | sed 's/^артефакты: //; s|/result\.json.*||')
  if [[ -n "$artifact_dir" && -f "$artifact_dir/result.json" ]]; then
    cp "/tmp/ha-verdict-$1.json" "$artifact_dir/validation-verdict.json" 2>/dev/null || true
    echo "$artifact_dir"
  else
    echo ""
  fi
}

# ---------- CDC-state ----------

cdc_expr() { # python-выражение над /api/cdc/state, например 'd["slot"]["walStatus"]'
  # 15с: при waiting_kafka/kafka-недоступности эндпоинт деградирует (пул потоков
  # занят ретраями producer'а) — 5с мало, ответы приходят, но медленно
  curl -s --max-time 15 "http://localhost:$CDC_PORT/api/cdc/state" \
    | python3 -c "import json,sys; d=json.load(sys.stdin); print($1)" 2>/dev/null
}
cdc_delivered() { cdc_expr 'd.get("messagesDelivered")'; }
wait_cdc_state() { # state timeout_s
  for _ in $(seq 1 "$2"); do
    [[ "$(cdc_expr 'd.get("state")')" == "$1" ]] && return 0
    sleep 1
  done
  return 1
}
# Диагностический дамп /api/cdc/state (обрезанный) — почему ожидание не сошлось.
cdc_state_raw() { curl -s --max-time 15 "http://localhost:$CDC_PORT/api/cdc/state" | head -c 300; }
# Известный дефект стенда (deviations 9н): стек KafkaFlow 4.2.0 нативно SIGSEGV
# (exit 139) при ПОЛНОЙ потере Kafka-брокеров после любой продукции — обе армы,
# вне управляемого кода. Рестарт-политика возвращает контейнер; at-least-once
# сохраняется (слот держит позицию). Тесты это учитывают: рестарты = наблюдение.
cdc_restarts() { docker inspect "streaming-polling-outbox-cdc-$ARM-1" --format '{{.RestartCount}}' 2>/dev/null || echo "?"; }
# Рост messagesDelivered выше base: на stdout — мс от t0 (или timeout).
wait_growth_ms() { # base timeout_s t0_ms
  local base=$1 timeout=$2 t0=$3 v
  for _ in $(seq 1 "$timeout"); do
    v=$(cdc_delivered); v=${v:-0}
    if python3 -c "import sys; sys.exit(0 if $v > $base else 1)" 2>/dev/null; then
      python3 -c "print(int(__import__('time').time()*1000) - $t0)"; return 0
    fi
    sleep 1
  done
  echo "timeout"; return 1
}
# Внешний recovery time при отказе «на ходу»: (1) доставка течёт — ждём застоя
# (3с без роста, отказ дошёл до конвейера), (2) меряем от t0 (инъекция) до
# возобновления роста messagesDelivered. На stdout — мс (no-stall/timeout — ошибка).
recovery_with_stall_ms() { # t0_ms timeout_s
  local t0=$1 timeout=$2 v base flat=0
  base=$(cdc_delivered); base=${base:-0}
  for _ in $(seq 1 "$timeout"); do
    v=$(cdc_delivered); v=${v:-0}
    if python3 -c "import sys; sys.exit(0 if $v > $base else 1)" 2>/dev/null; then
      base=$v; flat=0
    else
      flat=$((flat+1))
      [[ $flat -ge 3 ]] && break
    fi
    sleep 1
  done
  [[ $flat -ge 3 ]] || { echo "no-stall"; return 1; }
  for _ in $(seq 1 "$timeout"); do
    v=$(cdc_delivered); v=${v:-0}
    if python3 -c "import sys; sys.exit(0 if $v > $base else 1)" 2>/dev/null; then
      python3 -c "print(int(__import__('time').time()*1000) - $t0)"; return 0
    fi
    sleep 1
  done
  echo "timeout"; return 1
}

prom_query() { curl -sG --max-time 5 http://localhost:9090/api/v1/query --data-urlencode "query=$1"; }

# ---------- Kafka ----------

kafka_stop_all()  { $DC stop kafka1 kafka2 kafka3 >/dev/null 2>&1; }
kafka_start_all() { $DC start kafka1 kafka2 kafka3 >/dev/null 2>&1; }
wait_kafka_healthy() { # timeout_s
  local timeout=$1 svc
  for _ in $(seq 1 $(( timeout / 2 ))); do
    local all=1
    for svc in kafka1 kafka2 kafka3; do
      [[ "$($DC ps "$svc" --format '{{.Health}}' 2>/dev/null)" == "healthy" ]] || all=0
    done
    [[ "$all" == "1" ]] && return 0
    sleep 2
  done
  return 1
}

# ---------- Инъекции ----------

# Аварийная остановка ноды (crash): compose kill = SIGKILL. ВАЖНО: docker трактует
# API-kill как ручную остановку — рестарт-политика НЕ применяется, контейнер
# остаётся Exited(137). Возврат ноды в кластер — ЯВНЫЙ `docker compose start`
# (тесты делают это после выборов нового лидера).
kill_leader() { # leader_name
  $DC kill "$1" >/dev/null 2>&1
}

# ---------- Восстановление стенда после смены топологии / потери слота ----------

# Идемпотентный reconcile: чистит слот-остатки на standby, пересоздаёт слот на primary,
# если он инвалидирован/потерян (ЯВНОЕ операторское действие: сервисам автопересоздание
# с пропуском сообщений запрещено requirements.md 12.3 — и они его не делают), рестартует
# CDC, если он ушёл в терминальный error_slot, и проверяет canary-прогоном.
reconcile() { # notes-label
  local label=${1:-reconcile} st leader
  section "$label: reconcile кластера и слота"
  wait_cluster_ready 300 && ok "кластер: 3 ноды running/streaming" || bad "кластер не собрался за 300с"
  st=""
  for _ in $(seq 1 8); do
    st=$(psql_primary -Atc "SELECT coalesce((SELECT wal_status FROM pg_replication_slots WHERE slot_name='outbox_slot'), 'missing')" 2>/dev/null)
    [[ -n "$st" ]] && break
    sleep 4
  done
  if [[ "$st" == "missing" || "$st" == "lost" ]]; then
    if [[ "$st" == "lost" ]]; then
      psql_primary -Atc "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name='outbox_slot' AND active_pid IS NOT NULL" >/dev/null 2>&1 || true
      psql_primary -Atc "SELECT pg_drop_replication_slot('outbox_slot')" >/dev/null 2>&1 \
        && ok "инвалидированный слот удалён (wal_status=lost; потеря WAL зафиксирована в отчёте)" \
        || bad "не удалось удалить инвалидированный слот"
      # пауза: slotsync-воркеры standby удаляют осиротевшие копии имени слота —
      # иначе 02-slot.sql не сможет пересоздать слот / копия конфликтует по имени
      sleep 10
    else
      ok "слот отсутствует на primary (потерян при promotion)"
    fi
  else
    ok "слот на primary жив (wal_status=$st) — пересоздание не нужно"
  fi
  ./scripts/init/apply-bootstrap.sh >/dev/null 2>&1 \
    && ok "apply-bootstrap (02-slot/03-slot-reconcile идемпотентны)" \
    || bad "apply-bootstrap не прошёл (нода ещё реинициализируется? повторить позже)"
  if [[ "$(cdc_expr 'd.get("state")')" == "error_slot" ]]; then
    $DC --profile "$ARM" restart "cdc-$ARM" >/dev/null 2>&1
    for _ in $(seq 1 60); do
      [[ "$(cdc_expr 'd.get("state")')" == "running" ]] && break
      sleep 2
    done
    [[ "$(cdc_expr 'd.get("state")')" == "running" ]] && ok "CDC перезапущен после error_slot → running" \
      || bad "CDC не вернулся в running после рестарта"
  fi
  local synced
  synced=$(wait_slot_synced 240)
  [[ -n "$synced" ]] && ok "слот synced на standby ($synced)" || bad "слот не synced на standby за 240с"
  # canary: стенд снова полностью работоспособен
  deliver_and_validate '{"totalMessages":100,"messageSizeBytes":128,"batchSize":25,"parallelism":2}' "canary ($label)" >/dev/null
  [[ "$(vd 'd["verdict"]')" == "pass" ]] && ok "canary-прогон доставлен — стенд восстановлен" || bad "canary не прошёл: стенд не восстановлен"
}

# ---------- Отчёт ----------

# Собрать ha-report.json в каталог артефактов из переменных окружения:
# HA_TEST, HA_ARM, HA_RUN, HA_INJECTION, HA_RECOVERY_MS, HA_LEADER_CHANGE_MS,
# HA_BRANCH, HA_VERDICT, HA_DUPLICATES, HA_NOTES (+ опциональные HA_*_EXTRA строкой "k=v k=v").
ha_report() { # artifact_dir
  local dir=$1
  [[ -n "$dir" ]] || return 1
  HA_TEST="$HA_TEST" HA_ARM="$HA_ARM" HA_RUN="$HA_RUN" HA_INJECTION="$HA_INJECTION" \
  HA_RECOVERY_MS="${HA_RECOVERY_MS:-}" HA_LEADER_CHANGE_MS="${HA_LEADER_CHANGE_MS:-}" \
  HA_BRANCH="${HA_BRANCH:-}" HA_VERDICT="${HA_VERDICT:-}" HA_DUPLICATES="${HA_DUPLICATES:-}" \
  HA_NOTES="${HA_NOTES:-}" HA_EXTRA="${HA_EXTRA:-}" HA_REPORT_PATH="$dir/ha-report.json" \
  python3 - <<'PY'
import json, os
d = {
    "test": os.environ.get("HA_TEST", ""),
    "arm": os.environ.get("HA_ARM", ""),
    "runId": os.environ.get("HA_RUN", ""),
    "injection": os.environ.get("HA_INJECTION", ""),
    "recoveryMs": os.environ.get("HA_RECOVERY_MS", ""),
    "leaderChangeMs": os.environ.get("HA_LEADER_CHANGE_MS", ""),
    "branch": os.environ.get("HA_BRANCH", ""),
    "verdict": os.environ.get("HA_VERDICT", ""),
    "duplicates": os.environ.get("HA_DUPLICATES", ""),
    "notes": os.environ.get("HA_NOTES", ""),
}
for kv in os.environ.get("HA_EXTRA", "").split(";"):
    if "=" in kv:
        k, v = kv.split("=", 1)
        d[k.strip()] = v.strip()
with open(os.environ["HA_REPORT_PATH"], "w") as f:
    json.dump(d, f, ensure_ascii=False, indent=2, sort_keys=True)
PY
}
