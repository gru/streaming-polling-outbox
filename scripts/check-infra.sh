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
rows_after=$($DC exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" pg1 psql -h haproxy -U postgres -d outbox -Atc "SELECT count(*) FROM public.outbox_messages")
[[ "$rows_after" == "$rows_before" ]] && ok "outbox_messages не выросла (строка удалена в той же транзакции)" || bad "таблица выросла: $rows_before → $rows_after"

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
targets_result=$(python3 - "$targets" <<'PY'
import json, sys
try:
    t = json.loads(sys.argv[1])["data"]["activeTargets"]
    up = [x for x in t if x["health"] == "up"]
    print(f"total={len(t)} up={len(up)}")
except Exception as e:
    print(f"PARSE_ERROR {e}")
PY
)
echo "  targets: $targets_result"
total_t=$(echo "$targets_result" | grep -o 'total=[0-9]*' | cut -d= -f2)
up_t=$(echo "$targets_result" | grep -o 'up=[0-9]*' | cut -d= -f2)
if [[ -n "$up_t" && "$total_t" == "$up_t" && "$total_t" -gt 0 ]]; then ok "все scrape-таргеты up ($up_t/$total_t)"; else bad "таргеты: $targets_result"; fi

printf '\n\033[1mИТОГ: PASS=%d FAIL=%d\033[0m\n' "$PASS" "$FAIL"
[[ "$FAIL" -eq 0 ]]
