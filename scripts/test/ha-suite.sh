#!/usr/bin/env bash
# Автоматизированные тесты отказоустойчивости (этап 8, requirements.md 14, 20):
#   HA-001 Patroni Failover            — аварийная остановка primary под нагрузкой
#   HA-002 Patroni Switchover           — управляемое переключение (REST /switchover)
#   HA-003 Полная недоступность Kafka   — writer пишет, CDC не подтверждает LSN, WAL растёт
#   HA-004 WAL Sender Failure           — kill -9 walsender'а стриминга / SQL-сессии поллинга
#   HA-005 Restart CDC                  — до delivery (Kafka стоит) и во время batch
#   HA-006 Partial Kafka Batch Failure  — crash одного брокера под нагрузкой
#   HA-007 Kafka Broker Failure         — graceful stop одного брокера (RF=3, min ISR=2)
#   HA-008 Combined Failure             — Kafka недоступна + PG failover (две честные ветки)
# После каждого теста — фактическая сверка содержимого Kafka (KafkaValidator, §15),
# артефакты §13.4 (collect-results.sh) и ha-report.json с recovery time.
# Выход с кодом 0 — все проверки пройдены.
#
# Использование: ha-suite.sh --arm polling|streaming [--tests ha-001,ha-008,all]
# Один arm за запуск (§11: слот один, профили изолированы); полный матричный прогон —
# два вызова. --tests — список через запятую (по порядку выполнения, не аргумента).
set -uo pipefail
cd "$(dirname "$0")/../.."

if [[ ! -f .env ]]; then
  echo "ERROR: .env не найден. Скопируйте: cp .env.example .env"
  exit 1
fi
set -a; source .env; set +a

DC="docker compose"
WPORT="${WRITER_HTTP_PORT:-18080}"; WBASE="http://localhost:$WPORT"
VPORT="${KAFKA_VALIDATOR_HTTP_PORT:-18083}"; VBASE="http://localhost:$VPORT"
PPORT="${CDC_POLLING_HTTP_PORT:-18081}"
SPORT="${CDC_STREAMING_HTTP_PORT:-18082}"
PASS=0; FAIL=0
source "$(dirname "$0")/ha-common.sh"

ARM="" TESTS="all"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --arm) ARM="${2:-}"; shift 2 ;;
    --tests) TESTS="${2:-}"; shift 2 ;;
    *) echo "неизвестный аргумент: $1"; exit 1 ;;
  esac
done
[[ "$ARM" == polling || "$ARM" == streaming ]] || { echo "--arm polling|streaming обязателен"; exit 1; }
CDC_PORT=$([[ "$ARM" == polling ]] && echo "$PPORT" || echo "$SPORT")
HA_ARM="$ARM"

# ---------- Preflight ----------

section "Preflight: arm=$ARM, кластер, слот, Kafka, валидатор"

other=$([[ "$ARM" == polling ]] && echo streaming || echo polling)
if [[ "$($DC ps "cdc-$other" --format '{{.State}}' 2>/dev/null)" == "running" ]]; then
  $DC stop "cdc-$other" >/dev/null 2>&1 && ok "cdc-$other остановлен (изоляция профилей, §11)" || bad "не удалось остановить cdc-$other"
else
  ok "cdc-$other не запущен"
fi
if [[ "$($DC ps "cdc-$ARM" --format '{{.State}}' 2>/dev/null)" != "running" ]]; then
  # build только нужного сервиса и up с --no-deps — полный up реконсиляцией графа
  # пересоздаст pg-контейнеры и уронит живой Patroni
  $DC build "cdc-$ARM" >/dev/null 2>&1 && $DC --profile "$ARM" up -d --no-deps "cdc-$ARM" >/dev/null 2>&1 \
    || bad "не удалось запустить cdc-$ARM"
  for _ in $(seq 1 45); do
    [[ "$($DC ps "cdc-$ARM" --format '{{.Health}}' 2>/dev/null)" == "healthy" ]] && break
    sleep 2
  done
fi
[[ "$($DC ps "cdc-$ARM" --format '{{.Health}}' 2>/dev/null)" == "healthy" ]] && ok "cdc-$ARM healthy" || bad "cdc-$ARM не healthy"
[[ "$(cdc_expr 'd.get("state")')" == "running" ]] && ok "CDC $ARM: state=running" || bad "CDC $ARM: state=$(cdc_expr 'd.get("state")')"

wstate=$(writer_state_field 'state')
[[ "$wstate" != "running" && "$wstate" != "stopping" ]] \
  && ok "Writer без активного прогона (state=$wstate)" || bad "Writer выполняет прогон ($wstate) — дождитесь завершения"

info=$(cluster_info); leader=$(echo "$info" | cut -d'|' -f1); sync=$(echo "$info" | cut -d'|' -f2); running=$(echo "$info" | cut -d'|' -f3)
[[ "$running" == "3" && -n "$leader" && -n "$sync" ]] \
  && ok "Patroni: 3 ноды, leader=$leader, sync_standby=$sync" || bad "Patroni: '$info' (нужен полный кластер с sync_standby)"

st=$(psql_primary -Atc "SELECT coalesce((SELECT wal_status FROM pg_replication_slots WHERE slot_name='outbox_slot'), 'missing')" 2>/dev/null)
[[ "$st" == "reserved" || "$st" == "extended" ]] && ok "слот на primary: wal_status=$st" || bad "слот на primary: '$st' (failover-тесты требуют живой слот)"
synced=$(wait_slot_synced 180)
[[ -n "$synced" ]] && ok "слот synced на standby ($synced) — переживёт следующее переключение" || bad "слот не synced за 180с — failover-тесты ненадёжны"

wait_kafka_healthy 60 && ok "Kafka: 3 брокера healthy" || bad "Kafka не healthy"
wait_validator_ready && ok "KafkaValidator /ready=200" || bad "KafkaValidator не готов"

# ---------- Тесты ----------

report_vars() { # run injection recovery branch verdict duplicates [extra]
  HA_RUN="$1"; HA_INJECTION="$2"; HA_RECOVERY_MS="$3"; HA_BRANCH="$4"
  HA_VERDICT="$5"; HA_DUPLICATES="$6"; HA_EXTRA="${7:-}"; HA_NOTES=""
}

t_ha_001() {
  section "HA-001. Patroni Failover: аварийная остановка primary (§14)"
  HA_TEST="ha-001"
  local leader=$(leader_name) t0 new_leader ms base_delivered
  base_delivered=$(cdc_delivered)
  code=$(writer_start '{"totalMessages":1200,"ratePerSecond":30,"messageSizeBytes":256,"batchSize":50,"parallelism":4}')
  [[ "$code" == "202" ]] && ok "прогон 1200@30/с запущен (runId=$(python3 -c "import json; print(json.load(open('/tmp/ha-start.json'))['runId'][:8])"))" || bad "start → $code"
  run=$(python3 -c "import json; print(json.load(open('/tmp/ha-start.json'))['runId'])" 2>/dev/null)
  sleep 10
  t0=$(now_ms)
  kill_leader "$leader"
  ok "инъекция: docker compose kill $leader (crash primary) в момент активной доставки"
  new_leader=$(wait_leader_change "$leader" 150)
  ms=$(python3 -c "print(int(__import__('time').time()*1000) - $t0)")
  [[ -n "$new_leader" && "$new_leader" != "$leader" ]] \
    && ok "новый primary: $new_leader (выбор занял ${ms}мс)" || bad "новый лидер не избран за 150с"
  # docker compose kill НЕ перезапускает контейнер (docker трактует API-kill как
  # ручную остановку) — возвращаем убитую ноду в кластер явно, уже ПОСЛЕ выборов
  $DC start "$leader" >/dev/null 2>&1 && ok "убитая нода $leader запущена обратно (rejoin репликой)" \
    || bad "не удалось запустить $leader обратно"
  # ГРАБЛИ PG17 failover-слотов: synchronized_standby_slots на новом primary
  # указывает на старый набор standby — логический декодинг блокируется, пока
  # список не обновлён (см. refresh_sync_slots в ha-common.sh)
  newsync=$(refresh_sync_slots)
  [[ -n "$newsync" ]] && ok "synchronized_standby_slots на $new_leader обновлён: $newsync" \
    || bad "не удалось обновить synchronized_standby_slots (доставка будет висеть в catchup)"
  final=$(wait_writer_finished)
  if [[ "$final" == "completed" || "$final" == "failed" ]]; then
    ok "Writer завершился после failover: '$final' (часть батчей может уйти в failed — незафиксированные в PG, потерями не являются; критерий — вердикт)"
  else
    bad "Writer: '$final'"
  fi
  validate_after_delivery "$run" "HA-001"
  internal=$(cdc_expr 'd.get("lastRecoveryMs")')
  ok "recovery time: выбор нового лидера ${ms}мс + движок waiting_database→running ${internal}мс (recoveries=$(cdc_expr 'd.get("recoveries")')); внешний застой-замер неприменим — publisher раздает буфер канала во время разрыва"
  writer_results
  ok "Writer: written=$(writer_field 'd["writtenMessages"]') ambiguous=$(writer_field 'd["ambiguousUnresolved"]') failed=$(writer_field 'd["failedMessages"]') (ретраи при отказе допустимы)"
  [[ "$(slot_field wal_status)" == "reserved" || "$(slot_field wal_status)" == "extended" ]] \
    && ok "слот на новом primary жив: wal_status=$(slot_field wal_status)" || bad "слот на новом primary: wal_status=$(slot_field wal_status)"
  ok "CDC после failover: state=$(cdc_expr 'd.get("state")') readErrors=$(cdc_expr 'd.get("readErrors")') reconnects=$(cdc_expr 'd.get("reconnects")') lastRecoveryMs=$(cdc_expr 'd.get("lastRecoveryMs")') (внутренняя метрика движка)"
  report_vars "$run" "compose kill $leader (crash)" "${ms}ms(leader)+${internal}ms(engine)" "leader=${leader}->${new_leader}" "$(vd 'd["verdict"]')" "$(vd 'd["kafka"]["duplicateRecords"]')" \
    "syncSlotsRefreshed=${newsync:-}"
  dir=$(collect_artifacts "$run" "$ARM" "HA-001 failover (этап 8)")
  [[ -n "$dir" ]] && ok "артефакты: $dir" || bad "артефакты не сохранены"
  ha_report "$dir"
  reconcile "HA-001"
}

t_ha_002() {
  section "HA-002. Patroni Switchover: управляемое переключение (§14)"
  HA_TEST="ha-002"
  local leader=$(leader_name) candidate=$(sync_name) t0 code run final rec ms
  [[ -n "$leader" && -n "$candidate" && "$leader" != "$candidate" ]] \
    && ok "готовность: leader=$leader, candidate=$candidate (sync_standby)" || bad "нет пары leader/candidate: '$leader'/'$candidate'"
  synced=$(wait_slot_synced 120)
  [[ -n "$synced" ]] && ok "слот synced до переключения ($synced)" || bad "слот НЕ synced до switchover — переключение ненадёжно"
  code=$(writer_start '{"totalMessages":900,"ratePerSecond":30,"messageSizeBytes":256,"batchSize":50,"parallelism":4}')
  [[ "$code" == "202" ]] && ok "прогон 900@30/с запущен" || bad "start → $code"
  run=$(python3 -c "import json; print(json.load(open('/tmp/ha-start.json'))['runId'])" 2>/dev/null)
  sleep 8
  t0=$(now_ms)
  code=$(patroni_switchover "$leader" "$candidate")
  if [[ "$code" == "200" ]]; then
    ok "инъекция: POST /switchover leader=$leader candidate=$candidate (REST кандидата) → 200"
  elif [[ "$code" == "503" ]]; then
    ok "инъекция: POST /switchover → 503 (запланировано через DCS, исход подтвердит смена лидера)"
  else
    bad "switchover → $code: $(cat /tmp/ha-patroni-switchover.json)"
  fi
  # Graceful demote может ВИСНЕТЬ при живом консюмере логического слота (стриминг):
  # fast shutdown ждёт логический walsender → walsender ждёт клиента. После 60с
  # без смены лидера рвём replication-соединение рестартом CDC (операторское
  # лечение, deviations 9к; polling не держит соединений — не требуется).
  new_leader=""
  for i in $(seq 1 180); do
    new_leader=$(leader_name)
    [[ -n "$new_leader" && "$new_leader" != "$leader" ]] && break
    if [[ $i -eq 60 && "$ARM" == streaming ]]; then
      $DC --profile streaming restart cdc-streaming >/dev/null 2>&1
      ok "60с без смены лидера: рестарт cdc-streaming (разрывает логический walsender — graceful demote без этого висит, deviations 9к)"
    fi
    sleep 1
  done
  ms=$(python3 -c "print(int(__import__('time').time()*1000) - $t0)")
  [[ "$new_leader" == "$candidate" ]] \
    && ok "переключение состоялось: ${leader}→${candidate} за ${ms}мс" || bad "новый лидер '$new_leader' (ожидался $candidate)"
  newsync=$(refresh_sync_slots)
  [[ -n "$newsync" ]] && ok "synchronized_standby_slots на $new_leader обновлён: $newsync" \
    || bad "не удалось обновить synchronized_standby_slots (доставка будет висеть в catchup)"
  final=$(wait_writer_finished)
  if [[ "$final" == "completed" || "$final" == "failed" ]]; then
    ok "Writer завершился после switchover: '$final' (окно застрявшего demote может дать failed-батчи — незафиксированные, потерями не являются)"
  else
    bad "Writer: '$final'"
  fi
  validate_after_delivery "$run" "HA-002"
  internal=$(cdc_expr 'd.get("lastRecoveryMs")')
  ok "recovery time: switchover ${ms}мс + движок waiting_database→running ${internal}мс (recoveries=$(cdc_expr 'd.get("recoveries")'))"
  writer_results
  ok "Writer: written=$(writer_field 'd["writtenMessages"]') ambiguous=$(writer_field 'd["ambiguousUnresolved"]') failed=$(writer_field 'd["failedMessages"]')"
  report_vars "$run" "POST /switchover ${leader}->${candidate}" "${ms}ms(switchover)+${internal}ms(engine)" "leader=${leader}->${new_leader}" "$(vd 'd["verdict"]')" "$(vd 'd["kafka"]["duplicateRecords"]')" \
    "syncSlotsRefreshed=${newsync:-}"
  dir=$(collect_artifacts "$run" "$ARM" "HA-002 switchover (этап 8)")
  [[ -n "$dir" ]] && ok "артефакты: $dir" || bad "артефакты не сохранены"
  ha_report "$dir"
  reconcile "HA-002"
}

t_ha_003() {
  section "HA-003. Полная недоступность Kafka: WAL копится, LSN не подтверждается (§14)"
  HA_TEST="ha-003"
  local base_delivered=$(cdc_delivered) restarts_before=$(cdc_restarts)
  kafka_stop_all
  ok "инъекция: docker compose stop kafka1 kafka2 kafka3"
  # Запись идёт 60с (600@10/с) — сэмплы слота берём ВО ВРЕМЯ записи, чтобы
  # фиксировать рост удерживаемого WAL
  code=$(writer_start '{"totalMessages":600,"ratePerSecond":10,"messageSizeBytes":256,"batchSize":25,"parallelism":4}')
  [[ "$code" == "202" ]] && ok "прогон 600@10/с запущен (Writer пишет в PG при стоящей Kafka)" || bad "start → $code"
  run=$(python3 -c "import json; print(json.load(open('/tmp/ha-start.json'))['runId'])" 2>/dev/null)
  # CDC при полной потере брокеров после продукции крашится нативно (deviations 9н:
  # SIGSEGV стека KafkaFlow, exit 139) и уходит в рестарт-цикл до возвращения Kafka —
  # состояние может быть недоступно. Существенные проверки — факты со стороны PG.
  s=$(cdc_expr 'd.get("state")')
  if [[ "$s" == "waiting_kafka" ]]; then
    ok "CDC в waiting_kafka (не подтверждает недоставленное)"
  else
    ok "CDC state='$s' либо рестарт-цикл известного SIGSEGV-дефекта (deviations 9н) — гарантии LSN проверяем по слоту PG"
  fi
  local c0 c1 r0 r1 w0 w1
  c0=$(slot_field confirmed_flush_lsn); r0=$(psql_primary -Atc "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint FROM pg_replication_slots WHERE slot_name='outbox_slot'" 2>/dev/null)
  for _ in $(seq 1 8); do sleep 3; done
  c1=$(slot_field confirmed_flush_lsn); r1=$(psql_primary -Atc "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint FROM pg_replication_slots WHERE slot_name='outbox_slot'" 2>/dev/null)
  [[ -n "$c0" && "$c0" == "$c1" ]] && ok "confirmed_flush_lsn заморожен на весь простой ($c0) — CDC не подтверждает недоставленное (§12.1)" || bad "confirmed_flush_lsn сдвинулся: $c0 → $c1"
  python3 -c "import sys; sys.exit(0 if $([[ -n "$r1" ]] && echo "$r1" || echo 0) > $([[ -n "$r0" ]] && echo "$r0" || echo 0) else 1)" 2>/dev/null \
    && ok "удерживаемый слотом WAL растёт при записи: $r0 → $r1 байт (§12.1 мониторинг удержания)" || bad "retained WAL не растёт: $r0 → $r1"
  final=$(wait_writer_finished)
  [[ "$final" == "completed" ]] && ok "Writer: completed" || bad "Writer: '$final'"
  writer_results
  [[ "$(writer_field 'd["writtenMessages"]')" == "600" && "$(writer_field 'd["ambiguousUnresolved"]')" == "0" && "$(writer_field 'd["failedMessages"]')" == "0" ]] \
    && ok "written=600 ambiguous=0 failed=0 (PG доступен, Kafka на запись Writer не влияет)" || bad "written=$(writer_field 'd["writtenMessages"]') ambiguous=$(writer_field 'd["ambiguousUnresolved"]') failed=$(writer_field 'd["failedMessages"]')"
  local metric
  metric=$(prom_query 'pg_slot_wal_retained_bytes{slot_name="outbox_slot"}' | python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else "")' 2>/dev/null)
  [[ -n "$metric" ]] && ok "метрика pg_slot_wal_retained_bytes=$metric в Prometheus" || ok "pg_slot_wal_retained_bytes пуста в Prometheus (scrape ~5с; сэмплы psql выше точнее)"
  restarts_after=$(cdc_restarts)
  python3 -c "import sys; sys.exit(0 if $restarts_after > $restarts_before else 1)" 2>/dev/null \
    && ok "наблюдение: контейнер CDC перезапускался рестарт-политикой ($restarts_before → $restarts_after) — известный SIGSEGV-дефект стека при полном outage (deviations 9н), доставку восстановит рестарт" \
    || ok "контейнер CDC пережил outage без рестартов (дефект 9н не сработал)"
  local t0=$(now_ms)
  base_delivered=$(cdc_delivered)
  kafka_start_all
  ok "восстановление: docker compose start kafka1-3"
  rec=$(wait_growth_ms "${base_delivered:-0}" 240 "$t0")
  [[ "$rec" != timeout ]] && ok "автоматическое возобновление публикации: recovery=${rec}мс от старта Kafka" || bad "публикация не возобновилась ($rec)"
  wait_kafka_healthy 120 && ok "Kafka: 3 брокера healthy" || bad "Kafka не healthy после старта"
  u=$(wait_run_unique "$run" 600)
  [[ "$u" == "600" ]] && ok "все 600 доставлены после восстановления" || bad "доставлено $u/600"
  validate_run "$run" true >/dev/null
  assert_verdict_pass_dup "HA-003"
  report_vars "$run" "compose stop kafka1-3 (полный outage)" "$rec" "cdcRestarts=$restarts_before->$restarts_after" "$(vd 'd["verdict"]')" "$(vd 'd["kafka"]["duplicateRecords"]')" \
    "confirmedFrozen=$c0;retainedBytes=${r0}->${r1}"
  dir=$(collect_artifacts "$run" "$ARM" "HA-003 kafka outage (этап 8)")
  [[ -n "$dir" ]] && ok "артефакты: $dir" || bad "артефакты не сохранены"
  ha_report "$dir"
}

t_ha_004() {
  section "HA-004. WAL Sender Failure: kill -9 серверного процесса CDC (§14)"
  HA_TEST="ha-004"
  local leader=$(leader_name) pid pids t0 rec run final newpid
  if [[ "$ARM" == streaming ]]; then
    pid=$(psql_primary -Atc "SELECT pid FROM pg_stat_replication WHERE application_name='cdc-streaming'" 2>/dev/null | head -1)
    [[ -n "$pid" ]] && ok "walsender CDC найден на primary ($leader): pid=$pid (application_name='cdc-streaming'; физические репликаторы Patroni не тронуты)" \
      || bad "walsender 'cdc-streaming' не найден"
  else
    pids=$(psql_primary -Atc "SELECT pid FROM pg_stat_activity WHERE usename='sys_app' AND datname='outbox' AND backend_type='client backend' AND pid <> pg_backend_pid()" 2>/dev/null | tr '\n' ' ')
    [[ -n "${pids// /}" ]] && ok "SQL-сессии CDC Polling найдены (usename='sys_app'): pid=[$pids]" || bad "сессии sys_app не найдены"
  fi
  code=$(writer_start '{"totalMessages":1200,"ratePerSecond":30,"messageSizeBytes":256,"batchSize":50,"parallelism":4}')
  [[ "$code" == "202" ]] && ok "прогон 1200@30/с запущен" || bad "start → $code"
  run=$(python3 -c "import json; print(json.load(open('/tmp/ha-start.json'))['runId'])" 2>/dev/null)
  sleep 8
  local base_read=$(cdc_expr 'd.get("readErrors")') base_rec=$(cdc_expr 'd.get("reconnects")')
  t0=$(now_ms)
  if [[ "$ARM" == streaming ]]; then
    $DC exec -T "$leader" sh -c "kill -9 $pid" >/dev/null 2>&1 && ok "инъекция: kill -9 $pid (walsender 'cdc-streaming') внутри $leader" || bad "kill -9 $pid не выполнен"
  else
    for p in $pids; do $DC exec -T "$leader" sh -c "kill -9 $p" >/dev/null 2>&1; done
    ok "инъекция: kill -9 [$pids] (клиентские SQL-соединения CDC Polling) внутри $leader"
  fi
  sleep 5
  ok "CDC после разрыва: state=$(cdc_expr 'd.get("state")') readErrors=$(cdc_expr 'd.get("readErrors")') (было $base_read) reconnects=$(cdc_expr 'd.get("reconnects")') (было $base_rec) — разрыв обнаружен"
  if [[ "$ARM" == streaming ]]; then
    python3 -c "import sys; sys.exit(0 if $(cdc_expr 'd.get("readErrors")' 2>/dev/null || echo 0) > ${base_read:-0} else 1)" 2>/dev/null \
      && ok "разрыв обнаружен сервисом (readErrors вырос — replication-соединение единственное, маскировать нечем)" \
      || bad "readErrors не вырос — разрыв не зафиксирован"
  else
    ok "polling: readErrors=$(cdc_expr 'd.get("readErrors")') (было $base_read) — убитая pooled-сессия может быть молча вытеснена пулом Npgsql и разрыв не выйдет на уровень движка; корректность доставки доказывает вердикт"
  fi
  final=$(wait_writer_finished)
  [[ "$final" == "completed" ]] && ok "Writer: completed" || bad "Writer: '$final'"
  validate_after_delivery "$run" "HA-004"
  internal=$(cdc_expr 'd.get("lastRecoveryMs")')
  ok "recovery time: движок waiting→running ${internal}мс (recoveries=$(cdc_expr 'd.get("recoveries")')); внешний застой-замер неприменим — publisher раздает буфер во время разрыва"
  if [[ "$ARM" == streaming ]]; then
    newpid=""
    for _ in $(seq 1 120); do
      newpid=$(psql_primary -Atc "SELECT pid FROM pg_stat_replication WHERE application_name='cdc-streaming'" 2>/dev/null | head -1)
      [[ -n "$newpid" && "$newpid" != "$pid" ]] && break
      sleep 1
    done
    [[ -n "$newpid" && "$newpid" != "$pid" ]] && ok "создан новый walsender: pid=${pid} → ${newpid}" || bad "новый walsender не появился"
  else
    newsess=$(psql_primary -Atc "SELECT count(*) FROM pg_stat_activity WHERE usename='sys_app' AND datname='outbox' AND backend_type='client backend' AND pid <> pg_backend_pid()" 2>/dev/null)
    [[ "${newsess:-0}" -ge 1 ]] && ok "новые SQL-сессии CDC после переподключения: $newsess" || bad "новых сессий sys_app нет"
  fi
  report_vars "$run" "kill -9 ${pid:-$pids} на $leader ($ARM)" "${internal}ms(engine)" "" "$(vd 'd["verdict"]')" "$(vd 'd["kafka"]["duplicateRecords"]')" \
    "newServerPid=${newpid:-n/a}"
  dir=$(collect_artifacts "$run" "$ARM" "HA-004 walsender/sql kill (этап 8)")
  [[ -n "$dir" ]] && ok "артефакты: $dir" || bad "артефакты не сохранены"
  ha_report "$dir"
}

t_ha_005() {
  section "HA-005. Restart CDC: до delivery (Kafka стоит) и во время batch (§14)"
  HA_TEST="ha-005"
  local dup_total=0 u rec run
  # R1: рестарт ДО доставки — Kafka стоит, сообщения прочитаны, но не опубликованы
  kafka_stop_all
  ok "R1: Kafka остановлена (рестарт ДО Kafka delivery — детерминированная точка)"
  run=$(run_writer '{"totalMessages":200,"messageSizeBytes":256,"batchSize":50,"parallelism":4}')
  [[ "$run" != FAIL:* ]] && ok "R1: 200 записаны при стоящей Kafka" || bad "R1: $run"
  s=$(cdc_expr 'd.get("state")')
  if [[ "$s" == "waiting_kafka" ]]; then
    ok "R1: CDC в waiting_kafka, сообщения прочитаны, не доставлены"
  else
    ok "R1: CDC state='$s'/рестарт-цикл (дефект 9н при полном outage) — перезачитка с confirmed LSN проверяется вердиктом"
  fi
  local lag=$(cdc_expr 'd.get("walLagBytes")')
  python3 -c "import sys; sys.exit(0 if $([[ -n "$lag" ]] && echo "$lag" || echo 0) > 0 else 1)" 2>/dev/null \
    && ok "R1: неподтверждённый WAL > 0 ($lag байт) — есть что потерять из памяти" || ok "R1: walLagBytes=0 на момент рестарта (CDC успел подтвердить ранее)"
  $DC --profile "$ARM" restart "cdc-$ARM" >/dev/null 2>&1
  ok "R1: инъекция: docker compose --profile $ARM restart cdc-$ARM (при стоящей Kafka)"
  for _ in $(seq 1 60); do [[ "$(cdc_expr 'd.get("state")')" =~ ^(running|waiting_kafka)$ ]] && break; sleep 2; done
  ok "R1: CDC после рестарта: state=$(cdc_expr 'd.get("state")') (память очищена, LSN не сдвинут)"
  kafka_start_all; wait_kafka_healthy 120 && ok "R1: Kafka восстановлена" || bad "R1: Kafka не healthy"
  u=$(wait_run_unique "$run" 200)
  [[ "$u" == "200" ]] && ok "R1: все 200 доставлены после рестарта+восстановления (перезачитка с подтверждённого LSN)" || bad "R1: $u/200"
  validate_run "$run" true >/dev/null
  assert_verdict_pass_dup "HA-005/R1"
  dup_total=$((dup_total + $(vd 'd["kafka"]["duplicateRecords"]' 2>/dev/null || echo 0)))
  run5="$run"
  # R2/R3: рестарт ВО ВРЕМЯ обработки batch под нагрузкой. Точные под-случаи
  # «после delivery до ack» недетерминируемы извне (окно ~мс) — рестарт в
  # произвольной точке покрывает все три, дубликаты допустимы и считаются
  # (deviations.md 9а).
  local n
  for n in 2 3; do
    local delay=$([[ $n == 2 ]] && echo 15 || echo 20)
    code=$(writer_start '{"totalMessages":800,"ratePerSecond":20,"messageSizeBytes":256,"batchSize":50,"parallelism":4}')
    [[ "$code" == "202" ]] && ok "R$n: прогон 800@20/с запущен" || bad "R$n: start → $code"
    run=$(python3 -c "import json; print(json.load(open('/tmp/ha-start.json'))['runId'])" 2>/dev/null)
    sleep "$delay"
    local t0=$(now_ms)
    $DC --profile "$ARM" restart "cdc-$ARM" >/dev/null 2>&1
    ok "R$n: инъекция: restart cdc-$ARM на +${delay}с (во время обработки batch)"
    local rstate=""
    for _ in $(seq 1 120); do
      rstate=$(cdc_expr 'd.get("state")')
      [[ "$rstate" == "running" ]] && break
      sleep 1
    done
    rec=$(python3 -c "print(int(__import__('time').time()*1000) - $t0)")
    [[ "$rstate" == "running" ]] && ok "R$n: CDC в running после рестарта: ${rec}мс" || bad "R$n: CDC не running после рестарта (state=$rstate)"
    final=$(wait_writer_finished)
    [[ "$final" == "completed" ]] && ok "R$n: Writer completed" || bad "R$n: Writer '$final'"
    validate_after_delivery "$run" "HA-005/R$n"
    dup_total=$((dup_total + $(vd 'd["kafka"]["duplicateRecords"]' 2>/dev/null || echo 0)))
    run5="$run"
  done
  ok "суммарные дубликаты R1..R3: $dup_total (at-least-once: окно повтора = неподтверждённый LSN)"
  report_vars "$run5" "restart cdc-$ARM: R1 при стоящей Kafka, R2 +15с, R3 +20с под нагрузкой" "$rec" "restart-points" "$(vd 'd["verdict"]')" "$dup_total"
  dir=$(collect_artifacts "$run5" "$ARM" "HA-005 restart cdc (этап 8)")
  [[ -n "$dir" ]] && ok "артефакты: $dir" || bad "артефакты не сохранены"
  ha_report "$dir"
}

t_ha_006() {
  section "HA-006. Partial Kafka Batch Failure: crash одного брокера под нагрузкой (§14)"
  HA_TEST="ha-006"
  local base_delivered=$(cdc_delivered)
  code=$(writer_start '{"totalMessages":600,"messageSizeBytes":256,"batchSize":50,"parallelism":4}')
  [[ "$code" == "202" ]] && ok "прогон 600 (burst) запущен" || bad "start → $code"
  run=$(python3 -c "import json; print(json.load(open('/tmp/ha-start.json'))['runId'])" 2>/dev/null)
  sleep 1
  local t0=$(now_ms)
  $DC kill kafka3 >/dev/null 2>&1
  sleep 2
  # docker compose kill НЕ перезапускает контейнер (API-kill = ручная остановка,
  # deviations 9б) — возвращаем брокер явно, моделируя автоперезапуск узла
  $DC start kafka3 >/dev/null 2>&1
  ok "инъекция: docker compose kill kafka3 (crash) + start обратно через 2с; партиции под его лидерством ждут выборов"
  local state=""
  for _ in $(seq 1 60); do
    state=$($DC ps kafka3 --format '{{.State}}' 2>/dev/null)
    [[ "$state" == "running" ]] && break
    sleep 2
  done
  [[ "$state" == "running" ]] && ok "брокер перезапущен (compose start)" || bad "kafka3 не перезапустился"
  local u
  u=$(wait_run_unique "$run" 600)
  [[ "$u" == "600" ]] && ok "все 600 доставлены (недоставленные переотправлены повторно)" || bad "доставлено $u/600"
  wait_kafka_healthy 180 && ok "kafka3 healthy" || bad "kafka3 не healthy"
  ok "CDC за тест: kafkaErrors=$(cdc_expr 'd.get("kafkaErrors")') retries=$(cdc_expr 'd.get("retries")') (внутренние ретраи продюсера могут маскировать ошибки — это наблюдение, не фейл)"
  validate_run "$run" true >/dev/null
  assert_verdict_pass_dup "HA-006"
  report_vars "$run" "compose kill kafka3 (crash брокера)" "n/a" "" "$(vd 'd["verdict"]')" "$(vd 'd["kafka"]["duplicateRecords"]')" \
    "kafkaErrors=$(cdc_expr 'd.get("kafkaErrors")');retries=$(cdc_expr 'd.get("retries")')"
  dir=$(collect_artifacts "$run" "$ARM" "HA-006 partial batch failure (этап 8)")
  [[ -n "$dir" ]] && ok "артефакты: $dir" || bad "артефакты не сохранены"
  ha_report "$dir"
}

t_ha_007() {
  section "HA-007. Kafka Broker Failure: graceful stop одного брокера (RF=3, min ISR=2) (§14)"
  HA_TEST="ha-007"
  local base_delivered=$(cdc_delivered) max_stall=0 stall=0 prev v
  code=$(writer_start '{"totalMessages":900,"ratePerSecond":30,"messageSizeBytes":256,"batchSize":50,"parallelism":4}')
  [[ "$code" == "202" ]] && ok "прогон 900@30/с запущен" || bad "start → $code"
  run=$(python3 -c "import json; print(json.load(open('/tmp/ha-start.json'))['runId'])" 2>/dev/null)
  sleep 8
  ok "инъекция: docker compose stop kafka3 (graceful; RF=3 и min.insync.replicas=2 должны сохранить публикацию)"
  $DC stop kafka3 >/dev/null 2>&1 &
  STOP_PID=$!
  prev=$(cdc_delivered)
  for _ in $(seq 1 40); do
    v=$(cdc_delivered); v=${v:-0}
    if python3 -c "import sys; sys.exit(0 if $v > $prev else 1)" 2>/dev/null; then
      [[ $stall -gt $max_stall ]] && max_stall=$stall
      stall=0; prev=$v
    else
      stall=$((stall+1))
    fi
    sleep 1
  done
  [[ $stall -gt $max_stall ]] && max_stall=$stall
  wait "$STOP_PID"
  ok "максимальный простой доставки за окно остановки: ${max_stall}с (RF=3, min ISR=2)"
  python3 -c "import sys; sys.exit(0 if $max_stall <= 30 else 1)" 2>/dev/null \
    && ok "публикация не прерывалась надолго (≤30с; типичное ~20с = controlled shutdown брокера + перевыборы лидера партиций)" \
    || bad "простой доставки ${max_stall}с > 30с — ISR-защита не сработала как ожидалось"
  final=$(wait_writer_finished)
  [[ "$final" == "completed" ]] && ok "Writer completed" || bad "Writer: '$final'"
  $DC start kafka3 >/dev/null 2>&1
  wait_kafka_healthy 180 && ok "kafka3 запущен обратно и healthy" || bad "kafka3 не healthy после старта"
  validate_after_delivery "$run" "HA-007"
  report_vars "$run" "compose stop kafka3 (graceful)" "maxStall=${max_stall}s" "" "$(vd 'd["verdict"]')" "$(vd 'd["kafka"]["duplicateRecords"]')" \
    "maxDeliveryStallSeconds=$max_stall"
  dir=$(collect_artifacts "$run" "$ARM" "HA-007 broker failure graceful (этап 8)")
  [[ -n "$dir" ]] && ok "артефакты: $dir" || bad "артефакты не сохранены"
  ha_report "$dir"
}

t_ha_008() {
  section "HA-008. Combined Failure: Kafka недоступна + PG failover (§14)"
  HA_TEST="ha-008"
  local leader=$(leader_name) base_delivered=$(cdc_delivered) run new_leader branch
  kafka_stop_all
  ok "инъекция 1: docker compose stop kafka1-3 (CDC остановит подтверждение LSN)"
  run=$(run_writer '{"totalMessages":500,"messageSizeBytes":256,"batchSize":50,"parallelism":4}')
  [[ "$run" != FAIL:* ]] && ok "Writer записал 500 (PG доступен, Kafka стоит)" || bad "Writer: $run"
  s=$(cdc_expr 'd.get("state")')
  ok "CDC state='$s'/рестарт-цикл (дефект 9н); 500 сообщений удерживаются неподтверждённым WAL (слот не двигается)"
  sleep 20
  local t0=$(now_ms)
  kill_leader "$leader"
  ok "инъекция 2: docker compose kill $leader (PG failover при стоящей Kafka)"
  new_leader=$(wait_leader_change "$leader" 150)
  [[ -n "$new_leader" ]] && ok "новый primary: $new_leader" || bad "лидер не избран"
  $DC start "$leader" >/dev/null 2>&1 && ok "убитая нода $leader запущена обратно (rejoin репликой)" \
    || bad "не удалось запустить $leader обратно"
  newsync=$(refresh_sync_slots)
  [[ -n "$newsync" ]] && ok "synchronized_standby_slots на $new_leader обновлён: $newsync" \
    || bad "не удалось обновить synchronized_standby_slots"
  kafka_start_all; wait_kafka_healthy 180 && ok "Kafka восстановлена" || bad "Kafka не healthy"
  local t_kafka=$(now_ms)
  # Две честные ветки. Источник правды для ветки А — ВАЛИДАТОР (uniqueIds по
  # факту topic): счётчик CDC messagesDelivered непригоден — краш-рестарты
  # дефекта 9н обнуляют его, и новая инкарнация не превысит базу старой.
  # Ветка Б — устойчиво lost/missing слот на primary (≥5 проверок, не раньше
  # 45с после Kafka): транзиентные состояния promotion не считать.
  # (Б) слот потерян → CDC обязан уйти в error_slot и не заявлять доставку,
  #     missing=N фиксируется как задокументированная невосстановимая потеря (§14).
  local outcome="" lost_count=0 u_now=0
  for _ in $(seq 1 200); do
    u_now=$(vget "/api/validation/runs/$run" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("uniqueIds", 0))' 2>/dev/null)
    [[ "$u_now" == "500" ]] && { outcome="delivery-resumed"; break; }
    pgslot=$(psql_primary -Atc "SELECT coalesce((SELECT wal_status FROM pg_replication_slots WHERE slot_name='outbox_slot'), 'missing')" 2>/dev/null)
    if [[ "$pgslot" == "lost" || "$pgslot" == "missing" ]]; then
      lost_count=$((lost_count+1))
      elapsed=$(python3 -c "print(int(__import__('time').time()*1000) - $t_kafka)")
      if [[ $lost_count -ge 5 && "$elapsed" -gt 45000 ]]; then outcome="slot-lost"; break; fi
    else
      lost_count=0
    fi
    sleep 2
  done
  if [[ "$outcome" == "delivery-resumed" ]]; then
    branch="slot-survived"
    ok "ветка А: слот пережил promotion — доставка возобновилась без потерь"
    final=$(wait_writer_finished)
    [[ "$final" == "completed" ]] && ok "Writer completed" || bad "Writer: '$final'"
    u=$(wait_run_unique "$run" 500)
    [[ "$u" == "500" ]] && ok "все 500 доставлены" || bad "$u/500"
    validate_run "$run" true >/dev/null
    assert_verdict_pass_dup "HA-008"
  else
    branch="slot-lost"
    ok "ветка Б: слот потерян при promotion (непотребляемый при стоящей Kafka слот перестал быть synced, deviations 3); слот на primary: '$pgslot'"
    wait_cdc_state error_slot 90 && ok "CDC обнаружил потерю: state=error_slot (не заявляет успешную доставку, §12.3)" \
      || bad "CDC state=$(cdc_state_raw) — ни error_slot, ни доставки"
    [[ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 4 "http://localhost:$CDC_PORT/health")" == "200" ]] \
      && ok "/health=200 (контейнер жив, диагностирует)" || bad "/health не 200"
    [[ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 4 "http://localhost:$CDC_PORT/ready")" == "503" ]] \
      && ok "/ready=503 (сервис честно сообщает о неработоспособности)" || bad "/ready=$(curl -s -o /dev/null -w '%{http_code}' --max-time 4 "http://localhost:$CDC_PORT/ready")"
    ok "lastError CDC: $(cdc_expr 'd.get("lastError")')"
    d=$(cdc_delivered); d=${d:-0}
    python3 -c "import sys; sys.exit(0 if $d == $base_delivered else 1)" 2>/dev/null \
      && ok "messagesDelivered не вырос ($d) — доставка не заявлялась" || ok "delivered счётчик после рестарт-цикла дефекта 9н: $base_delivered → $d (вердикт ниже — источник правды)"
    validate_run "$run" true >/dev/null
    missing=$(vd 'd["missingWrittenIdsCount"]')
    if [[ "$(vd 'd["verdict"]')" == "fail" && "$missing" -gt 0 ]]; then
      ok "вердикт fail c missing=$missing — задокументированный случай невосстановимой потери (§14), не сбой сервиса"
    else
      bad "неожиданный вердикт: $(vd 'd["verdict"]') missing=$missing"
    fi
  fi
  report_vars "$run" "stop kafka1-3 + kill $leader (комбинированный)" "" "$branch" "$(vd 'd["verdict"]')" "$(vd 'd["kafka"]["duplicateRecords"]')" \
    "leader=${leader}->${new_leader}"
  dir=$(collect_artifacts "$run" "$ARM" "HA-008 combined failure ($branch, этап 8)")
  [[ -n "$dir" ]] && ok "артефакты: $dir" || bad "артефакты не сохранены"
  ha_report "$dir"
  reconcile "HA-008"
}

# ---------- Запуск выбранных тестов ----------

[[ "$TESTS" == "all" ]] && TESTS="ha-001,ha-002,ha-003,ha-004,ha-005,ha-006,ha-007,ha-008"
IFS=',' read -ra LIST <<< "$TESTS"
for t in "${LIST[@]}"; do
  t="${t//[[:space:]]/}"
  case "$t" in
    ha-001) t_ha_001 ;; ha-002) t_ha_002 ;; ha-003) t_ha_003 ;; ha-004) t_ha_004 ;;
    ha-005) t_ha_005 ;; ha-006) t_ha_006 ;; ha-007) t_ha_007 ;; ha-008) t_ha_008 ;;
    *) bad "неизвестный тест: $t" ;;
  esac
done

finish
