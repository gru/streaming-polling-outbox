#!/usr/bin/env bash
# Сбор результатов тестового прогона (этап 6, requirements.md 13.4):
# после каждого теста сохраняет JSON с итогами и CSV с основными временными
# рядами. Итоговые счётчики берутся из ФАКТИЧЕСКИХ данных прогонов
# (API Writer/CDC, Kafka consume по runId, слот из PostgreSQL) — Prometheus
# scrape не гарантирует сохранение кратковременных событий (13.4).
#
# Использование:
#   scripts/reports/collect-results.sh RUN_ID [--arm auto|polling|streaming]
#                                      [--notes "список отказов"]
#                                      [--out DIR]
# Артефакты: results/raw/<timestamp>-<runId>/{result.json,timeseries.csv}
# (results/raw/ в .gitignore — крупные артефакты не коммитим).
set -uo pipefail
cd "$(dirname "$0")/../.."

if [[ ! -f .env ]]; then
  echo "ERROR: .env не найден. Скопируйте: cp .env.example .env"
  exit 1
fi
set -a; source .env; set +a

DC="docker compose"
PROM="http://localhost:9090"
WBASE="http://localhost:${WRITER_HTTP_PORT:-18080}"
PPOLL="http://localhost:${CDC_POLLING_HTTP_PORT:-18081}"
PSTREAM="http://localhost:${CDC_STREAMING_HTTP_PORT:-18082}"

RUN_ID=${1:?usage: collect-results.sh RUN_ID [--arm auto|polling|streaming] [--notes "..."] [--out DIR]}
ARM=auto
NOTES=""
OUT_BASE="results/raw"
shift || true
while [[ $# -gt 0 ]]; do
  case "$1" in
    --arm)   ARM=$2; shift 2 ;;
    --notes) NOTES=$2; shift 2 ;;
    --out)   OUT_BASE=$2; shift 2 ;;
    *) echo "Неизвестный аргумент: $1" >&2; exit 1 ;;
  esac
done

# --- артефакты прогона: Writer -----------------------------------------------
curl -s --max-time 8 "$WBASE/api/generation/results" -o /tmp/cr-writer-results.json
curl -s --max-time 90 "$WBASE/api/generation/results/ids" -o /tmp/cr-writer-ids.json  # 90с: ids-набор 1M-прогонов ~40МБ
curl -s --max-time 8 "$WBASE/api/generation/state" -o /tmp/cr-writer-state.json
for f in /tmp/cr-writer-results.json /tmp/cr-writer-ids.json; do
  if ! python3 -c "import json; json.load(open('$f'))" 2>/dev/null; then
    echo "ERROR: Writer недоступен или прогона нет ($WBASE). Прогоните тест и повторите." >&2
    exit 2
  fi
done
res_runid=$(python3 -c "import json; print(json.load(open('/tmp/cr-writer-results.json'))['runId'])")
if [[ "$res_runid" != "$RUN_ID" ]]; then
  echo "ERROR: последний завершённый прогон Writer = '$res_runid', запрошен '$RUN_ID'. Коллектор работает с последним прогоном." >&2
  exit 2
fi

# --- arm: авто-детект по отвечающему CDC -------------------------------------
if [[ "$ARM" == "auto" ]]; then
  poll_code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 4 "$PPOLL/api/cdc/state" || true)
  stream_code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 4 "$PSTREAM/api/cdc/state" || true)
  if [[ "$poll_code" == "200" && "$stream_code" == "200" ]]; then
    echo "ERROR: отвечают ОБА CDC-сервиса — профили polling/streaming взаимоисключающие (requirements.md 11). Укажите --arm." >&2
    exit 2
  elif [[ "$poll_code" == "200" ]]; then ARM=polling
  elif [[ "$stream_code" == "200" ]]; then ARM=streaming
  else
    echo "ERROR: ни один CDC не отвечает ($PPOLL / $PSTREAM). Запустите профиль и повторите." >&2
    exit 2
  fi
fi
CBASE="$PPOLL"; [[ "$ARM" == "streaming" ]] && CBASE="$PSTREAM"
curl -s --max-time 8 "$CBASE/api/cdc/state" -o /tmp/cr-cdc-state.json
python3 -c "import json; json.load(open('/tmp/cr-cdc-state.json'))" 2>/dev/null || { echo "ERROR: /api/cdc/state недоступен ($CBASE)" >&2; exit 2; }
echo "== arm: $ARM ($CBASE)"

# --- слот из PostgreSQL --------------------------------------------------------
$DC exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" pg1 psql -h haproxy -U postgres -d outbox -Atc \
  "SELECT slot_name, confirmed_flush_lsn, restart_lsn, wal_status, active FROM pg_replication_slots WHERE slot_name='outbox_slot'" \
  > /tmp/cr-slot.txt 2>/dev/null || echo "" > /tmp/cr-slot.txt

# --- Kafka: фактические записи прогона по runId --------------------------------
echo "== читаю Kafka (outbox.events, --from-beginning; фильтр body.runId=$RUN_ID)..."
$DC exec -T kafka1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic outbox.events --from-beginning --timeout-ms 15000 \
  > /tmp/cr-kafka.jsonl 2>/dev/null

# --- версии компонентов ---------------------------------------------------------
docker compose exec -T -e PGPASSWORD="$POSTGRES_PASSWORD" pg1 psql -h haproxy -U postgres -d outbox -Atc "SHOW server_version" > /tmp/cr-ver-pg.txt 2>/dev/null
$DC exec -T pg1 patroni --version > /tmp/cr-ver-patroni.txt 2>/dev/null || echo "unknown" > /tmp/cr-ver-patroni.txt
$DC exec -T kafka1 /opt/kafka/bin/kafka-topics.sh --version > /tmp/cr-ver-kafka.txt 2>/dev/null || echo "unknown" > /tmp/cr-ver-kafka.txt
# в runtime-образе нет SDK: dotnet --version требует SDK и падает,
# версию рантайма даёт --list-runtimes (первая строка:AspNetCore.App 8.0.x)
$DC exec -T "cdc-$ARM" dotnet --list-runtimes 2>/dev/null | head -1 > /tmp/cr-ver-dotnet.txt || echo "unknown" > /tmp/cr-ver-dotnet.txt
$DC images --format json > /tmp/cr-images.jsonl 2>/dev/null || true

# --- per-container CPU/RAM: срез docker stats ----------------------------------
# (cAdvisor на Docker Desktop не отдаёт per-container — docs/deviations.md 7;
#  непрерывный сэмплер для тестов: scripts/reports/sample-docker-stats.sh)
docker stats --no-stream --format json > /tmp/cr-docker-stats.jsonl 2>/dev/null || true

git rev-parse HEAD > /tmp/cr-git.txt 2>/dev/null || echo "no-git" > /tmp/cr-git.txt

# --- сборка result.json + timeseries.csv ----------------------------------------
STAMP=$(date +%Y%m%d-%H%M%S)
OUT_DIR="$OUT_BASE/${STAMP}-${RUN_ID:0:8}"
mkdir -p "$OUT_DIR"

PROM="$PROM" ARM="$ARM" RUN_ID="$RUN_ID" NOTES="$NOTES" OUT_DIR="$OUT_DIR" \
python3 <<'PY'
import csv, json, os, re, subprocess, sys, time
import urllib.parse, urllib.request

arm, run_id = os.environ["ARM"], os.environ["RUN_ID"]
prom, out_dir = os.environ["PROM"], os.environ["OUT_DIR"]
notes = os.environ.get("NOTES", "")

def load(path, default=None):
    try:
        return json.load(open(path))
    except Exception:
        return default

def read(path, default=""):
    try:
        return open(path).read().strip()
    except Exception:
        return default

writer = load("/tmp/cr-writer-results.json", {})
writer_ids = load("/tmp/cr-writer-ids.json", {})
writer_state = load("/tmp/cr-writer-state.json", {})
cdc = load("/tmp/cr-cdc-state.json", {})

# ISO-строки Writer имеют до 7 знаков дробной части — fromisoformat старых
# python принимает только 3/6; нормализуем.
def parse_iso(s):
    if not s:
        return None
    s = re.sub(r"(\.\d{6})\d+", r"\1", s)
    try:
        from datetime import datetime
        return datetime.fromisoformat(s)
    except Exception:
        return None

start_dt = parse_iso(writer.get("startedAt"))
end_dt = parse_iso(writer.get("finishedAt"))
if start_dt is None or end_dt is None:
    print("ERROR: у прогона нет startedAt/finishedAt (прогон завершён?)", file=sys.stderr)
    sys.exit(3)
start_s, end_s = start_dt.timestamp(), end_dt.timestamp()

# --- Kafka: счётчики фактических записей прогона ---
total, kafka_ids = 0, set()
for line in open("/tmp/cr-kafka.jsonl"):
    line = line.strip()
    if not line:
        continue
    try:
        m = json.loads(line)
    except Exception:
        continue
    body = m.get("body") if isinstance(m, dict) else None
    if isinstance(body, dict) and body.get("runId") == run_id:
        total += 1
        kafka_ids.add(m.get("id"))

written_ids = set(writer_ids.get("writtenIds") or [])
duplicates = total - len(kafka_ids)
missing = sorted(written_ids - kafka_ids)

result = {
    "schema": "streaming-polling-outbox/results/v1",
    "collectedAt": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    "runId": run_id,
    "arm": arm,
    "notes": notes,
    "git": {"commit": read("/tmp/cr-git.txt")},
    "window": {
        "start": writer.get("startedAt"),
        "end": writer.get("finishedAt"),
        "durationSeconds": round(end_s - start_s, 3),
    },
    "load": writer.get("profile") or {},
    "writer": writer,
    "writerStateAtCollect": writer_state,
    "cdc": cdc,
    "slot": read("/tmp/cr-slot.txt"),
    "kafka": {
        "records": total,
        "uniqueIds": len(kafka_ids),
        "duplicates": duplicates,
        "expectedWritten": writer_ids.get("writtenCount"),
        "missing": len(missing),
        "missingIdsSample": missing[:20],
    },
    "failures": {
        "notes": notes,
        "writerErrors": writer.get("errors") or [],
        "writerReconnects": writer.get("reconnects"),
        "writerFailed": writer.get("failedMessages"),
        "writerAmbiguous": writer.get("ambiguousUnresolved"),
        "cdcReadErrors": cdc.get("readErrors"),
        "cdcKafkaErrors": cdc.get("kafkaErrors"),
        "cdcRetries": cdc.get("retries"),
        "cdcReconnects": cdc.get("reconnects"),
        "cdcLastError": cdc.get("lastError"),
        "cdcLastKafkaError": cdc.get("lastKafkaError"),
    },
    "recovery": {
        "recoveries": cdc.get("recoveries"),
        "lastRecoveryMs": cdc.get("lastRecoveryMs"),
        "e2eP50Ms": cdc.get("e2eP50Ms"), "e2eP95Ms": cdc.get("e2eP95Ms"), "e2eP99Ms": cdc.get("e2eP99Ms"),
    },
    "versions": {
        "postgres": read("/tmp/cr-ver-pg.txt"),
        "patroni": read("/tmp/cr-ver-patroni.txt").split(" ")[-1] if read("/tmp/cr-ver-patroni.txt") else "unknown",
        "kafka": read("/tmp/cr-ver-kafka.txt"),
        "dotnet": read("/tmp/cr-ver-dotnet.txt") or "unknown",
    },
}

# образы по сервисам (compose images --format json: массив или построчно)
images = {}
try:
    raw = read("/tmp/cr-images.jsonl")
    items = json.loads(raw) if raw.startswith("[") else [json.loads(x) for x in raw.splitlines() if x.strip()]
    for it in items:
        if isinstance(it, dict) and it.get("ContainerName"):
            images[it["ContainerName"]] = it.get("Repository", "") + ":" + it.get("Tag", "")
except Exception:
    pass
result["versions"]["images"] = images

# срез docker stats (per-container CPU/RAM на момент сбора)
def mem_bytes(usage):
    m = re.match(r"([\d.]+)\s*([KMGT]?I?B)", (usage or "").split("/")[0].strip(), re.I)
    if not m:
        return None
    mult = {"B": 1, "KB": 1e3, "KIB": 1024, "MB": 1e6, "MIB": 1024**2,
            "GB": 1e9, "GIB": 1024**3, "TB": 1e12, "TIB": 1024**4}
    return round(float(m.group(1)) * mult.get(m.group(2).upper(), 1))

stats = []
try:
    for line in open("/tmp/cr-docker-stats.jsonl"):
        line = line.strip()
        if not line:
            continue
        d = json.loads(line)
        stats.append({"container": d.get("Name"), "cpuPercent": d.get("CPUPerc"),
                      "memBytes": mem_bytes(d.get("MemUsage")), "memPercent": d.get("MemPerc"),
                      "pids": d.get("PIDs")})
except Exception:
    pass
result["dockerStatsSnapshot"] = stats

json.dump(result, open(os.path.join(out_dir, "result.json"), "w"),
          ensure_ascii=False, indent=2, sort_keys=True)

# --- timeseries.csv: query_range по окну прогона --------------------------------
lo, hi = start_s - 30, end_s + 30
step = 5

series = [  # (колонка CSV, PromQL)
    ("writer_messages_written", 'writer_messages_written_total{job="writer"}'),
    ("writer_commit_sum_ms", 'writer_commit_duration_milliseconds_sum{job="writer"}'),
    ("cdc_messages_delivered", f'cdc_{arm}_messages_delivered_total{{job="cdc-{arm}"}}'),
    ("cdc_e2e_sum_ms", f'cdc_{arm}_e2e_latency_milliseconds_sum{{job="cdc-{arm}"}}'),
    ("cdc_e2e_count", f'cdc_{arm}_e2e_latency_milliseconds_count{{job="cdc-{arm}"}}'),
    ("cdc_wal_lag_bytes", f'cdc_{arm}_wal_lag_bytes'),
    ("cdc_confirmed_lsn", f'cdc_{arm}_confirmed_lsn'),
    ("cdc_state", f'cdc_{arm}_state'),
    ("pg_up", 'pg_up'),
    ("pg_wal_position_bytes", 'pg_wal_position_bytes'),
    ("pg_wal_dir_bytes", 'pg_wal_dir_bytes'),
    ("pg_slot_wal_retained_bytes", 'pg_slot_wal_retained_bytes{slot_name="outbox_slot"}'),
    ("pg_slot_flush_lag_bytes", 'pg_slot_flush_lag_bytes{slot_name="outbox_slot"}'),
    ("pg_phys_replay_lag_max_bytes", 'max(pg_replication_replay_lag_bytes{application_name=~"pg2|pg3"})'),
    ("kafka_brokers", 'kafka_brokers'),
    ("kafka_outbox_offsets_sum", 'sum(kafka_topic_partition_current_offset{topic="outbox.events"})'),
    ("vm_cpu_cores", 'rate(container_cpu_usage_seconds_total{id="/",cpu="total"}[30s])'),
    ("vm_ram_working_set_bytes", 'container_memory_working_set_bytes{id="/"}'),
]
if arm == "streaming":
    series.append(("cdc_channel_depth", 'cdc_streaming_channel_depth'))

def query_range(q):
    url = prom + "/api/v1/query_range?" + urllib.parse.urlencode(
        {"query": q, "start": lo, "end": hi, "step": step})
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            d = json.load(r)
        out = {}
        for s in d.get("data", {}).get("result", []):
            for ts, val in s.get("values", []):
                out[int(float(ts))] = float(val)
        return out
    except Exception:
        return {}

cols = {name: query_range(q) for name, q in series}
all_ts = sorted({ts for m in cols.values() for ts in m} or
                [int(t) for t in range(int(lo), int(hi) + 1, step)])
csv_path = os.path.join(out_dir, "timeseries.csv")
with open(csv_path, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["timestamp"] + [name for name, _ in series])
    for ts in all_ts:
        w.writerow([time.strftime("%H:%M:%S", time.localtime(ts))] +
                   [cols[name].get(ts, "") for name, _ in series])

dur = result["window"]["durationSeconds"]
dur_disp = f"{dur * 1000:.0f}мс" if dur < 1 else f"{dur:.1f}с"
rate = (writer.get("writtenMessages") or 0) / dur if dur else 0
print(f"OK {out_dir}")
print(f"  окно {dur_disp} | writer {writer.get('writtenMessages')} сообщений "
      f"({rate:.0f} msg/s) | kafka: {total} записей, {len(kafka_ids)} уникальных, "
      f"дублей {duplicates}, missing {len(missing)} | e2e p50/p95/p99: "
      f"{cdc.get('e2eP50Ms')}/{cdc.get('e2eP95Ms')}/{cdc.get('e2eP99Ms')} ms")
if missing:
    print(f"  WARN: {len(missing)} отсутствующих id (первые: {missing[:5]})", file=sys.stderr)
PY
rc=$?
[[ $rc -ne 0 ]] && exit $rc
echo "артефакты: $OUT_DIR/result.json, $OUT_DIR/timeseries.csv"
exit 0
