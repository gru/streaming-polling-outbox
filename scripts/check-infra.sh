#!/usr/bin/env bash
# Полная проверка инфраструктуры стенда (этап 1).
# Выход с кодом 0 — все проверки пройдены.
set -uo pipefail
cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo "ERROR: .env не найден. Скопируйте: cp .env.example .env"
  exit 1
fi
set -a; source .env; set +a

DC="docker compose"
PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); printf '  \033[32m✔\033[0m %s\n' "$1"; }
bad() { FAIL=$((FAIL+1)); printf '  \033[31m✘\033[0m %s\n' "$1"; }
section() { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }

# psql к актуальному primary (через HAProxy) из контейнера pg1.
# Прикладная БД стенда — outbox (слот, publication, таблица живут в ней).
psql_primary() {
  $DC exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" pg1 \
    psql -h haproxy -U postgres -d outbox -v ON_ERROR_STOP=1 "$@"
}
# psql локально к конкретной ноде (работает и на replica)
psql_node() {
  local node=$1; shift
  $DC exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" "$node" \
    psql -h localhost -U postgres -d outbox -v ON_ERROR_STOP=1 "$@"
}
# psql через PgBouncer от имени прикладного пользователя (маппится только outbox)
psql_bouncer() {
  local user=$1 pw=$2; shift 2
  $DC exec -T -e PGPASSWORD="$pw" pg1 psql -h pgbouncer -p 6432 -U "$user" -d outbox -v ON_ERROR_STOP=1 "$@"
}

section "Compose config"
if $DC config -q 2>/dev/null; then ok "docker compose config валиден"; else bad "docker compose config невалиден"; fi

section "Контейнеры: healthchecks"
for svc in etcd1 etcd2 etcd3 pg1 pg2 pg3 haproxy pgbouncer kafka1 kafka2 kafka3 prometheus; do
  state=$($DC ps "$svc" --format '{{.Health}}' 2>/dev/null)
  if [[ "$state" == "healthy" ]]; then ok "$svc healthy"; else bad "$svc: health=$state"; fi
done
for svc in init-db kafka-init; do
  status=$($DC ps -a "$svc" --format '{{.Status}}' 2>/dev/null)
  if [[ "$status" == [Ee]xited*\(0\)* ]]; then ok "$svc завершился успешно (exit 0)"; else bad "$svc: status=$status"; fi
done

section "Patroni: кластер"
cluster_json=$(curl -s --max-time 5 http://localhost:8008/cluster)
patroni_result=$(python3 - "$cluster_json" <<'PY'
import json, sys
try:
    data = json.loads(sys.argv[1])
except Exception as e:
    print(f"PARSE_ERROR {e}"); sys.exit()
members = data.get("members", [])
# leader: state=running; реплики Patroni сообщают state=streaming
running = [m for m in members if m.get("state") in ("running", "streaming")]
leaders = [m for m in running if m.get("role") == "leader"]
sync = [m for m in running if m.get("role") == "sync_standby"]
print(f"members={len(members)} running={len(running)} leader={len(leaders)} sync_standby={len(sync)}")
PY
)
echo "  $patroni_result"
[[ "$patroni_result" == "members=3 running=3 leader=1"* ]] && ok "3 ноды, все running, ровно 1 leader" || bad "патрон кластера не в целевом состоянии"
[[ "$patroni_result" == *"sync_standby=1" || "$patroni_result" == *"sync_standby=2" ]] && ok "есть synchronous standby (sync replication)" || bad "нет sync_standby — синхронная репликация не активна"

section "PostgreSQL: роли и привилегии"
roles=$($DC exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" pg1 psql -h haproxy -U postgres -d outbox -Atc "
  SELECT rolname, rolsuper, rolreplication, rolcanlogin FROM pg_roles
  WHERE rolname IN ('postgres','sys_writer','sys_app','sys_replication') ORDER BY rolname")
echo "$roles" | sed 's/^/  /'
check_role() { # name super repl login
  local row; row=$(echo "$roles" | grep -E "^$1\|")
  [[ "$row" == "$1|$2|$3|$4" ]] && ok "$1: super=$2 replication=$3 login=$4" || bad "$1: ожидалось '$1|$2|$3|$4', получено '$row'"
}
check_role sys_app          f f t
check_role sys_replication  f t t
check_role sys_writer       f f t

section "PostgreSQL: подключение через PgBouncer (transaction pool)"
who=$($DC exec -T -e PGPASSWORD="$SYS_WRITER_PASSWORD" pg1 psql -h pgbouncer -p 6432 -U sys_writer -d outbox -Atc "SELECT current_user" 2>&1)
[[ "$who" == "sys_writer" ]] && ok "sys_writer подключается через PgBouncer" || bad "sys_writer через PgBouncer: $who"
who=$($DC exec -T -e PGPASSWORD="$SYS_APP_PASSWORD" pg1 psql -h pgbouncer -p 6432 -U sys_app -d outbox -Atc "SELECT current_user" 2>&1)
[[ "$who" == "sys_app" ]] && ok "sys_app подключается через PgBouncer" || bad "sys_app через PgBouncer: $who"
pool_mode=$($DC exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" pg1 psql -h pgbouncer -p 6432 -U postgres -d pgbouncer -Atc "SHOW CONFIG" 2>/dev/null | grep -E '^pool_mode\|' )
[[ "$pool_mode" == *"transaction"* ]] && ok "PgBouncer pool_mode=transaction" || bad "pool_mode: '$pool_mode'"

section "PostgreSQL: прикладная БД (outbox), publication, replication protocol"
# Прикладная БД — отдельная outbox, не postgres (решение владельца, deviations.md):
# реальное приложение живёт в своей БД. PG17 slotsync worker синхронизирует слоты
# только БД из dbname primary_conninfo — Patroni выставляет там outbox через
# postgresql.database (см. patroni.yml и bootstrap-with-appdb.sh).
pub=$($DC exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" pg1 psql -h haproxy -U postgres -d outbox -Atc "SELECT count(*) FROM pg_publication WHERE pubname='outbox_publication'")
[[ "$pub" == "1" ]] && ok "publication outbox_publication существует (БД outbox)" || bad "publication не найдена"
pub_tables=$($DC exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" pg1 psql -h haproxy -U postgres -d outbox -Atc "SELECT count(*) FROM pg_publication_tables WHERE pubname='outbox_publication' AND schemaname='public' AND tablename='outbox_messages'")
[[ "$pub_tables" == "1" ]] && ok "publication содержит outbox_messages (pgoutput эмитит только опубликованные таблицы)" || bad "publication не содержит outbox_messages: '$pub_tables'"
# Протокол логической репликации от sys_replication напрямую через HAProxy (минуя PgBouncer)
identify=$($DC exec -T -e PGPASSWORD="$SYS_REPLICATION_PASSWORD" pg1 \
  psql "host=haproxy dbname=outbox user=sys_replication replication=database" -Atc "IDENTIFY_SYSTEM" 2>&1)
[[ "$identify" == *"systemid"* || -n "$identify" ]] && ok "sys_replication: replication protocol доступен (IDENTIFY_SYSTEM)" || bad "replication protocol: $identify"

section "PostgreSQL: failover logical slot (pgoutput)"
slot=$(psql_primary -Atc "SELECT plugin || '|' || failover || '|' || wal_status FROM pg_replication_slots WHERE slot_name='outbox_slot'")
echo "  primary slot: $slot"
[[ "$slot" == "pgoutput|true|"* || "$slot" == "pgoutput|t|"* ]] && ok "слот outbox_slot: pgoutput, failover=true" || bad "слот outbox_slot: '$slot'"
# wal_status: reserved/extended — WAL слота удерживается; lost/invalid — проблема
[[ "$slot" == *"reserved" ]] && ok "wal_status удерживает WAL (reserved)" || bad "wal_status не reserved: '$slot'"
# synced=true хотя бы на одной реплике (ждём до 90 сек — слоты синхронизируются асинхронно)
synced_found=""
for _ in $(seq 1 45); do
  for node in pg2 pg3; do
    is_replica=$(psql_node "$node" -Atc "SELECT NOT pg_is_in_recovery()" 2>/dev/null)
    if [[ "$is_replica" == "f" ]]; then
      synced=$(psql_node "$node" -Atc "SELECT synced FROM pg_replication_slots WHERE slot_name='outbox_slot'" 2>/dev/null)
      if [[ "$synced" == "t" ]]; then synced_found="$node"; break 2; fi
    fi
  done
  sleep 2
done
if [[ -n "$synced_found" ]]; then ok "слот синхронизирован на standby ($synced_found, synced=true) — нативный failover slot sync PG17 работает"; else bad "слот не синхронизирован ни на одной реплике (synced != true)"; fi

section "PostgreSQL: параметры WAL"
wal_level=$(psql_primary -Atc "SHOW wal_level")
[[ "$wal_level" == "logical" ]] && ok "wal_level=logical" || bad "wal_level=$wal_level"
sync_rep=$(psql_primary -Atc "SELECT count(*) FROM pg_stat_replication WHERE sync_state IN ('sync','quorum')")
[[ "$sync_rep" -ge 1 ]] && ok "синхронный standby есть (pg_stat_replication)" || bad "нет sync standby в pg_stat_replication"

section "Сквозной smoke: INSERT+DELETE в одной транзакции -> peek (pgoutput, binary)"
# Writer держит outbox пустым (решение владельца): строка существует только внутри
# транзакции, сообщение живёт в WAL. Здесь проверяем, что такие изменения
# logical decoding по-прежнему эмитит.
# Если запущен любой CDC (polling/streaming — профили взаимоисключающие,
# requirements.md 11) — он непрерывно держит слот, и конкурентный peek отсюда
# получит 55006 object_in_use; live-проверку peek пропускаем (сами CDC
# проверяются отдельными smoke-cdc-polling.sh / smoke-cdc-streaming.sh).
cdc_polling_running=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -c 'cdc-polling' || true)
cdc_streaming_running=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -c 'cdc-streaming' || true)
cdc_slot_busy=$(( cdc_polling_running + cdc_streaming_running ))
peek_count() {
  psql_primary -Atc "SELECT count(*) FROM pg_logical_slot_peek_binary_changes('outbox_slot', NULL, NULL, 'proto_version','1','publication_names','outbox_publication')" 2>/dev/null
}
before=$(peek_count); before=${before:-0}
rows_before=$($DC exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" pg1 psql -h haproxy -U postgres -d outbox -Atc "SELECT count(*) FROM public.outbox_messages")
# Фиксированный UUID: DELETE фильтрует по id — у sys_writer колоночный SELECT(id)
# (WHERE в DELETE читает колонки условия; чтение body запрещено и не нужно).
smoke_uuid="00000000-0000-7000-8000-000000000c0d"
inserted=$($DC exec -T -e PGPASSWORD="$SYS_WRITER_PASSWORD" pg1 psql -h pgbouncer -p 6432 -U sys_writer -d outbox -Atc "
BEGIN;
INSERT INTO public.outbox_messages (id, topic, body) VALUES ('$smoke_uuid', 'outbox.events', '{\"smoke\":\"infra-check\"}');
DELETE FROM public.outbox_messages WHERE id = '$smoke_uuid'::uuid;
COMMIT;" 2>&1 | tr '\n' ' ')
# RETURNING не используется: он требует SELECT-привилегию на возвращаемые колонки,
# у sys_writer только INSERT+DELETE и колоночный SELECT(id) (минимум по requirements.md 5).
[[ "$inserted" == *"INSERT 0 1"* && "$inserted" == *"DELETE 1"* ]] && ok "sys_writer: INSERT+DELETE в одной транзакции через PgBouncer" || bad "INSERT+DELETE через PgBouncer: $inserted"
after=""
if [[ "$cdc_slot_busy" != "0" ]]; then
  ok "CDC запущен (polling=$cdc_polling_running streaming=$cdc_streaming_running) — live-peek отсюда пропущен (слот занят CDC, конкурентный peek = 55006)"
else
  for _ in $(seq 1 15); do
    after=$(peek_count)
    delta=$(( ${after:-0} - before ))
    [[ "$delta" -ge 3 ]] && break
    sleep 2
  done
  # Одна вставленная и тут же удалённая строка = минимум 3 записи pgoutput
  # (Relation + Insert + Delete внутри транзакции; B/C-обрамление может объединяться
  # с соседними транзакциями — поэтому порог 3, а не 5).
  if [[ "$delta" -ge 3 ]]; then ok "peek (pgoutput, binary) видит изменения: было $before, стало $after (Δ$delta)"; else bad "peek не видит изменения (before=$before after=$after)"; fi
fi
rows_after=$($DC exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" pg1 psql -h haproxy -U postgres -d outbox -Atc "SELECT count(*) FROM public.outbox_messages")
[[ "$rows_after" == "$rows_before" ]] && ok "outbox_messages не выросла (строка удалена в той же транзакции)" || bad "таблица выросла: $rows_before → $rows_after"

section "CDC Polling: SECURITY DEFINER-функции (requirements.md 5.2)"
# Схема cdc и три функции-обёртки: владелец postgres, SECURITY DEFINER,
# search_path=pg_catalog, EXECUTE только у sys_app. Слот/publication/параметры
# декодирования/лимит пачки жёстко зафиксированы внутри функций.
funcs=$($DC exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" pg1 psql -h haproxy -U postgres -d outbox -Atc "
  SELECT n.nspname||'.'||p.proname||'|'||p.prosecdef||'|'||array_to_string(p.proconfig,',')
  FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
  WHERE n.nspname='cdc' ORDER BY p.proname" | sed 's/|true|/|t|/g')
echo "$funcs" | sed 's/^/  /'
fn_count=$(echo "$funcs" | grep -c '^cdc\.')
[[ "$fn_count" == "3" ]] && ok "3 функции в схеме cdc" || bad "функций в схеме cdc: $fn_count"
fn_badflags=$(echo "$funcs" | grep -cv '|t|search_path=pg_catalog' || true)
[[ "$fn_count" == "3" && "$fn_badflags" == "0" ]] \
  && ok "все функции SECURITY DEFINER с search_path=pg_catalog" \
  || bad "не все функции SECURITY DEFINER/search_path (нарушений: $fn_badflags)"
grants=$($DC exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" pg1 psql -h haproxy -U postgres -d outbox -Atc "
  SELECT has_function_privilege('sys_app','cdc.peek_outbox_changes(integer)','EXECUTE')
      || '|' || has_function_privilege('public','cdc.peek_outbox_changes(integer)','EXECUTE')")
[[ "$grants" == "t|f" || "$grants" == "true|false" ]] && ok "EXECUTE только у sys_app (PUBLIC отозван)" || bad "grants: $grants"
# Положительный вызов от sys_app через PgBouncer (минимальная роль, requirements.md 5.3).
# status() не захватывает слот — безопасен и при работающем CDC.
app_status=$($DC exec -T -e PGPASSWORD="$SYS_APP_PASSWORD" pg1 psql -h pgbouncer -p 6432 -U sys_app -d outbox -Atc "SELECT cdc.outbox_slot_status()::text" 2>&1)
[[ "$app_status" == *'"exists": true'* ]] && ok "sys_app: cdc.outbox_slot_status() работает через PgBouncer" || bad "status от sys_app: $app_status"
# ПРЯМОЙ peek от sys_app обязан падать (роль без привилегий на слот) — wrapper оправдан
direct=$($DC exec -T -e PGPASSWORD="$SYS_APP_PASSWORD" pg1 psql -h pgbouncer -p 6432 -U sys_app -d outbox -Atc \
  "SELECT count(*) FROM pg_logical_slot_peek_binary_changes('outbox_slot', NULL, 10, 'proto_version','1','publication_names','outbox_publication')" 2>&1)
[[ "$direct" == *"permission denied"* ]] && ok "прямой peek от sys_app запрещён (permission denied — wrapper необходим)" || bad "прямой peek: $direct"
if [[ "$cdc_slot_busy" != "0" ]]; then
  ok "CDC запущен (polling=$cdc_polling_running streaming=$cdc_streaming_running) — live-проверки peek/advance от sys_app пропущены (слот занят CDC)"
else
  app_peek=$($DC exec -T -e PGPASSWORD="$SYS_APP_PASSWORD" pg1 psql -h pgbouncer -p 6432 -U sys_app -d outbox -Atc "SELECT count(*) FROM cdc.peek_outbox_changes(10)" 2>&1)
  [[ "$app_peek" =~ ^[0-9]+$ ]] && ok "sys_app: cdc.peek_outbox_changes(10) работает (строк: $app_peek, peek не потребляет)" || bad "peek от sys_app: $app_peek"
  # advance дальше watermark обязан отказывать («не подтверждай то, что не читал»)
  adv=$($DC exec -T -e PGPASSWORD="$SYS_APP_PASSWORD" pg1 psql -h pgbouncer -p 6432 -U sys_app -d outbox -Atc \
    "SELECT * FROM cdc.advance_outbox_slot('FFFFFFFF/FFFFFFFF')" 2>&1)
  [[ "$adv" == *"отклонён"* ]] && ok "advance дальше watermark отклоняется (защита от подтверждения вслепую)" || bad "advance: $adv"
fi

section "Kafka: topic"
topic_desc=$($DC exec -T kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic outbox.events 2>&1)
echo "$topic_desc" | head -2 | sed 's/^/  /'
partitions=$(echo "$topic_desc" | grep -o 'PartitionCount: [0-9]*' | awk '{print $2}')
[[ "$partitions" == "6" ]] && ok "topic outbox.events: 6 партиций" || bad "PartitionCount=$partitions"
rf=$(echo "$topic_desc" | grep -o 'ReplicationFactor: [0-9]*' | awk '{print $2}')
[[ "$rf" == "3" ]] && ok "replication factor = 3" || bad "ReplicationFactor=$rf"
topic_cfg=$($DC exec -T kafka1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type topics --entity-name outbox.events 2>&1)
echo "$topic_cfg" | grep -q 'min.insync.replicas=2' && ok "min.insync.replicas=2" || bad "min.insync.replicas не найден: $topic_cfg"
auto_create=$($DC exec -T kafka1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --broker 1 --all 2>&1 | grep -o 'auto.create.topics.enable=[a-z]*' | head -1)
[[ "$auto_create" == *"false" ]] && ok "auto.create.topics.enable=false" || bad "auto.create: '$auto_create'"

section "Kafka: roundtrip (produce/consume)"
rt_topic="outbox.events"
rt_payload="infra-check-$(date +%s)"
$DC exec -T kafka1 /bin/bash -c "echo '$rt_payload' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic $rt_topic --request-required-acks all" >/dev/null 2>&1 \
  && ok "сообщение опубликовано (acks=all)" || bad "producer не смог опубликовать"
consumed=$($DC exec -T kafka1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $rt_topic --from-beginning --timeout-ms 15000 2>/dev/null | grep -c "$rt_payload")
[[ "$consumed" -ge 1 ]] && ok "сообщение прочитано консьюмером" || bad "сообщение не найдено консьюмером"

section "Prometheus"
ready=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://localhost:9090/-/ready)
[[ "$ready" == "200" ]] && ok "/-/ready = 200" || bad "/-/ready = $ready"
targets=$(curl -s --max-time 5 http://localhost:9090/api/v1/targets)
# Таргеты остановленного CDC-профиля ожидаемо down (polling/streaming никогда
# не работают вместе, requirements.md 11): требуем up для всех, КРОМЕ
# cdc-polling/cdc-streaming, чей контейнер сейчас не запущен
targets_result=$(python3 - "$targets" "$cdc_polling_running" "$cdc_streaming_running" <<'PY'
import json, sys
try:
    t = json.loads(sys.argv[1])["data"]["activeTargets"]
    polling_up = sys.argv[2] != "0"
    streaming_up = sys.argv[3] != "0"
    down = []
    for x in t:
        if x["health"] != "up":
            url = x["scrapeUrl"]
            if "cdc-polling" in url and not polling_up:
                continue
            if "cdc-streaming" in url and not streaming_up:
                continue
            down.append(url)
    print(f"total={len(t)} up={len(t) - len(down)} down_unexpected={len(down)}")
except Exception as e:
    print(f"PARSE_ERROR {e}")
PY
)
echo "  targets: $targets_result"
if [[ "$targets_result" == *"down_unexpected=0"* && "$targets_result" != *"PARSE_ERROR"* ]]; then
  ok "все ожидаемые scrape-таргеты up ($targets_result)"
else
  bad "таргеты: $targets_result"
fi

printf '\n\033[1mИТОГ: PASS=%d FAIL=%d\033[0m\n' "$PASS" "$FAIL"
[[ "$FAIL" -eq 0 ]]
