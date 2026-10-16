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

# psql к актуальному primary (через HAProxy) из контейнера pg1
psql_primary() {
  $DC exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" pg1 \
    psql -h haproxy -U postgres -d postgres -v ON_ERROR_STOP=1 "$@"
}
# psql локально к конкретной ноде (работает и на replica)
psql_node() {
  local node=$1; shift
  $DC exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" "$node" \
    psql -h localhost -U postgres -d postgres -v ON_ERROR_STOP=1 "$@"
}
# psql через PgBouncer от имени прикладного пользователя
psql_bouncer() {
  local user=$1 pw=$2; shift 2
  $DC exec -T -e PGPASSWORD="$pw" pg1 psql -h pgbouncer -p 6432 -U "$user" -d postgres -v ON_ERROR_STOP=1 "$@"
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
roles=$($DC exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" pg1 psql -h haproxy -U postgres -d postgres -Atc "
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
who=$($DC exec -T -e PGPASSWORD="$SYS_WRITER_PASSWORD" pg1 psql -h pgbouncer -p 6432 -U sys_writer -d postgres -Atc "SELECT current_user" 2>&1)
[[ "$who" == "sys_writer" ]] && ok "sys_writer подключается через PgBouncer" || bad "sys_writer через PgBouncer: $who"
who=$($DC exec -T -e PGPASSWORD="$SYS_APP_PASSWORD" pg1 psql -h pgbouncer -p 6432 -U sys_app -d postgres -Atc "SELECT current_user" 2>&1)
[[ "$who" == "sys_app" ]] && ok "sys_app подключается через PgBouncer" || bad "sys_app через PgBouncer: $who"
pool_mode=$($DC exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" pg1 psql -h pgbouncer -p 6432 -U postgres -d pgbouncer -Atc "SHOW CONFIG" 2>/dev/null | grep -E '^pool_mode\|' )
[[ "$pool_mode" == *"transaction"* ]] && ok "PgBouncer pool_mode=transaction" || bad "pool_mode: '$pool_mode'"

section "PostgreSQL: прикладная БД (postgres), publication, replication protocol"
# Прикладная БД — postgres: PG17 slotsync worker синхронизирует слоты только
# БД из primary_conninfo (см. infrastructure/postgres/init/01-db-publication.sql).
pub=$($DC exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" pg1 psql -h haproxy -U postgres -d postgres -Atc "SELECT count(*) FROM pg_publication WHERE pubname='outbox_publication'")
[[ "$pub" == "1" ]] && ok "publication outbox_publication существует" || bad "publication не найдена"
pub_tables=$($DC exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" pg1 psql -h haproxy -U postgres -d postgres -Atc "SELECT count(*) FROM pg_publication_tables WHERE pubname='outbox_publication'")
[[ "$pub_tables" == "0" ]] && ok "publication пустая (0 таблиц)" || bad "publication содержит $pub_tables таблиц"
# Протокол логической репликации от sys_replication напрямую через HAProxy (минуя PgBouncer)
identify=$($DC exec -T -e PGPASSWORD="$SYS_REPLICATION_PASSWORD" pg1 \
  psql "host=haproxy dbname=postgres user=sys_replication replication=database" -Atc "IDENTIFY_SYSTEM" 2>&1)
[[ "$identify" == *"systemid"* || -n "$identify" ]] && ok "sys_replication: replication protocol доступен (IDENTIFY_SYSTEM)" || bad "replication protocol: $identify"

section "PostgreSQL: failover logical slot (test_decoding)"
slot=$(psql_primary -Atc "SELECT plugin || '|' || failover || '|' || wal_status FROM pg_replication_slots WHERE slot_name='outbox_slot'")
echo "  primary slot: $slot"
[[ "$slot" == "test_decoding|true|"* || "$slot" == "test_decoding|t|"* ]] && ok "слот outbox_slot: test_decoding, failover=true" || bad "слот outbox_slot: '$slot'"
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

section "Сквозной smoke: pg_logical_emit_message -> peek (test_decoding)"
smoke_payload="infra-check-$(date +%s)"
lsn=$($DC exec -T -e PGPASSWORD="$SYS_WRITER_PASSWORD" pg1 psql -h pgbouncer -p 6432 -U sys_writer -d postgres -Atc "SELECT pg_logical_emit_message(true, 'outbox', '$smoke_payload')" 2>&1)
[[ "$lsn" == */* ]] && ok "sys_writer записал logical message (LSN $lsn) через PgBouncer" || bad "emit через PgBouncer: $lsn"
peeked=""
for _ in $(seq 1 15); do
  peeked=$(psql_primary -Atc "SELECT count(*) FROM pg_logical_slot_peek_changes('outbox_slot', NULL, NULL) WHERE data LIKE '%prefix: outbox%' AND data LIKE '%$smoke_payload%'" 2>/dev/null)
  [[ "$peeked" -ge 1 ]] && break
  sleep 2
done
[[ "$peeked" -ge 1 ]] && ok "peek (test_decoding) видит сообщение с payload" || bad "peek не видит сообщение (count=$peeked)"

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
