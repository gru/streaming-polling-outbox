#!/usr/bin/env bash
# benchmark.sh — этап 9: изолированные benchmark-циклы A/B (requirements.md 11).
#   Цикл A (polling):  полный wipe → чистый стенд → одинаковые сценарии →
#                      артефакты/отчёт → полное удаление тестового состояния (§11 шаг 7).
#   Цикл B (streaming): инфраструктура заново → ТЕ ЖЕ сценарии с теми же параметрами →
#                      артефакты/отчёт (стенд остаётся поднятым — исходное состояние владельца).
# Метрики (§13.4 — точные итоги из фактических данных, Prometheus только дополнительно):
#   Writer (длительность, фактический rate, p50/p95/p99 commit/batch), CDC (пропускная
#   способность доставки по сэмплам 1с, e2e p50/p95/p99 + средний e2e по дельтам гистограммы,
#   счётчики ошибок/ретраев), полный цикл доставки до валидатора (drain), дубликаты,
#   per-role CPU/RAM по docker stats (cAdvisor не отдаёт per-container — deviations 7).
#
# Использование:
#   scripts/test/benchmark.sh --cycle all                # A(polling)+wipe+B(streaming)
#   scripts/test/benchmark.sh --cycle polling|streaming  # один цикл (polling завершится wipe)
#   Опции: --skip-wipe   не делать начальный wipe (стенд уже чистый/поднят под этот arm)
#          --only s1,s3  подмножество сценариев (имена без суффиксов, см. SCENARIOS)
# Артефакты: results/raw/<stamp>-benchmark-<arm>/{benchmark-report.json, docker-stats.csv,
#   runs/<stamp>-<runId8>/{result.json,timeseries.csv,validation-verdict.json},
#   scenarios/<имя>.json, cdc-samples/<имя>.tsv}
#   + компактная копия отчёта в results/benchmark/benchmark-<arm>.json (коммитится).
set -uo pipefail
cd "$(dirname "$0")/../.."

if [[ ! -f .env ]]; then
  echo "ERROR: .env не найден. Скопируйте: cp .env.example .env"
  exit 1
fi
set -a; source .env; set +a

DC="docker compose"
WBASE="http://localhost:${WRITER_HTTP_PORT:-18080}"
VBASE="http://localhost:${KAFKA_VALIDATOR_HTTP_PORT:-18083}"
PPORT="${CDC_POLLING_HTTP_PORT:-18081}"
SPORT="${CDC_STREAMING_HTTP_PORT:-18082}"

CYCLE_ARG="all"
SKIP_WIPE=0
ONLY=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --cycle) CYCLE_ARG="${2:-}"; shift 2 ;;
    --skip-wipe) SKIP_WIPE=1; shift ;;
    --only) ONLY="${2:-}"; shift 2 ;;
    *) echo "неизвестный аргумент: $1"; exit 1 ;;
  esac
done
[[ "$CYCLE_ARG" == all || "$CYCLE_ARG" == polling || "$CYCLE_ARG" == streaming ]] \
  || { echo "--cycle должен быть all|polling|streaming"; exit 1; }

ARM=""; CDC_PORT=""   # ha-common.sh требует их после source
source scripts/test/ha-common.sh
PASS=0; FAIL=0

# ---------- Сценарии (ОДИНАКОВЫЕ параметры для обеих арм, §11) ----------
# Постоянная нагрузка — минимум 5-10 минут (требование владельца, этап 9):
# короткие прогоны не выходят на установившийся режим (GC/checkpoint/окна scrape).
# Порядок фиксирован; warmup не входит в отчёт сценариев (JIT/пулы/прогрев валидатора).
SCEN_NAMES=(s1-steady-50 s2-steady-200 s3-burst s4-large-4k s5-throughput)
SCEN_DESCS=(
  "равномерная низкая нагрузка 30000@50/s (~10 мин)"
  "равномерная высокая нагрузка 60000@200/s (5 мин)"
  "backlog-drain: burst 20000 без rate-limit (короткий по смыслу: скорость выборки)"
  "крупные сообщения 15000@50/s x4096B (5 мин)"
  "max throughput: backlog 400000 без rate-limit — Writer вываливает его за секунды (замерено 67-108k msg/s), CDC выбирает на пределе; узкое место — сам CDC по построению. Объём подобран под МЕДЛЕННУЮ арму: polling дрейнит большой backlog устойчиво ~0.9k msg/s (замер цикла A: 400k за 425с), streaming ~11k (400k за 31с) — 1M у polling занял бы ~18 мин и не влезал в бюджет ожидания; валидатор держит >=10k подтв/с (400k за 37с, цикл B)"
)
SCEN_SPECS=(
  '{"totalMessages":30000,"ratePerSecond":50,"messageSizeBytes":256,"batchSize":50,"parallelism":4}'
  '{"totalMessages":60000,"ratePerSecond":200,"messageSizeBytes":256,"batchSize":50,"parallelism":4}'
  '{"totalMessages":20000,"messageSizeBytes":256,"batchSize":100,"parallelism":8}'
  '{"totalMessages":15000,"ratePerSecond":50,"messageSizeBytes":4096,"batchSize":50,"parallelism":4}'
  '{"totalMessages":400000,"messageSizeBytes":256,"batchSize":100,"parallelism":8}'
)
WARMUP_SPEC='{"totalMessages":500,"ratePerSecond":100,"messageSizeBytes":256,"batchSize":50,"parallelism":4}'

scenario_enabled() { # short-name без суффикса (s1, s3...)
  [[ -z "$ONLY" ]] && return 0
  local n short
  for n in "${SCEN_NAMES[@]}"; do
    short="${n%%-*}"
    [[ "$short" == "$1" ]] || continue
    [[ ",$ONLY," == *",$n,"* || ",$ONLY," == *",$short,"* ]] && return 0
  done
  return 1
}

# ---------- Сэмплирование CDC-доставки (1с) ----------
# ts_ms,state,delivered,wal_lag_bytes — растёт messagesDelivered = сообщения дошли до Kafka.
cdc_sampler() { # out_tsv
  local out=$1 line
  echo "ts_ms,state,delivered,wal_lag_bytes" > "$out"
  while [[ -e /tmp/bench-sampler-on ]]; do
    line=$(curl -s --max-time 10 "http://localhost:$CDC_PORT/api/cdc/state" | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    raise SystemExit
# без f-строк с \"-экранированием: системный python3 (3.9) их не парсит
print("{},{},{}".format(d.get("state", "?"), d.get("messagesDelivered", 0), d.get("walLagBytes", 0)))' 2>/dev/null)
    [[ -n "$line" ]] && echo "$(now_ms),$line" >> "$out"
    sleep 1
  done
}

# Средний e2e за сценарий по дельтам гистограммы Prometheus (sum/count — счётчики):
# точный per-сценарный средний e2e, в отличие от rolling-процентилей CDC (окно 4096).
prom_hist_pair() { # metric_base_without_suffix → "sum count" (или " ")
  local q sum count
  sum=$(prom_query "$1_sum{job=\"cdc-$ARM\"}" | python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else "")' 2>/dev/null)
  count=$(prom_query "$1_count{job=\"cdc-$ARM\"}" | python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else "")' 2>/dev/null)
  echo "${sum:- } ${count:- }"
}

# ---------- Ожидание доставки всего прогона (валидатор; бюджет больше, чем в ha-common) ----------
wait_run_unique_long() { # runId expected timeout_s → unique на stdout
  local u=""
  for _ in $(seq 1 "$3"); do
    u=$(vget "/api/validation/runs/$1" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("uniqueIds", 0))' 2>/dev/null)
    [[ "$u" == "$2" ]] && break
    sleep 1
  done
  echo "${u:-0}"
}

# ---------- Один сценарий ----------
# Глобальные входы: ARM, CYCLE_DIR, DOCKER_STATS_CSV (общий на цикл).
# Глобальные выходы: SCEN_JSON (путь), SCEN_RUN_ID, SCEN_WRITTEN.
run_scenario() { # idx
  local idx=$1 name desc spec run written t0 t1 t2 u code verdict
  name="${SCEN_NAMES[$idx]}"; desc="${SCEN_DESCS[$idx]}"; spec="${SCEN_SPECS[$idx]}"
  section "Сценарий $name: $desc"

  [[ "$(writer_state_field 'state')" != "running" ]] || { bad "$name: Writer ещё занят прошлым прогоном"; return 1; }

  curl -s --max-time 10 "http://localhost:$CDC_PORT/api/cdc/state" -o /tmp/bench-cdc-before.json
  local e2e_sum0 e2e_cnt0
  read -r e2e_sum0 e2e_cnt0 <<< "$(prom_hist_pair "cdc_${ARM}_e2e_latency_milliseconds")"

  mkdir -p "$CYCLE_DIR/scenarios" "$CYCLE_DIR/cdc-samples"
  touch /tmp/bench-sampler-on
  cdc_sampler "$CYCLE_DIR/cdc-samples/$name.tsv" &
  local sampler_pid=$!

  t0=$(now_ms)
  run=$(run_writer "$spec")
  t1=$(now_ms)
  if [[ "$run" == FAIL:* ]]; then
    bad "$name: прогон не completed ($run)"
    rm -f /tmp/bench-sampler-on; wait "$sampler_pid" 2>/dev/null
    return 1
  fi
  SCEN_RUN_ID="$run"
  ok "$name: Writer completed за $(( t1 - t0 ))мс (runId=${run:0:8})"
  writer_results
  cp /tmp/ha-results.json /tmp/bench-writer.json
  written=$(writer_field 'd["writtenMessages"]')
  SCEN_WRITTEN="$written"
  ok "$name: written=$written, фактический rate=$(python3 -c "print(f'{$written / max($(writer_field 'd["durationSeconds"]'), 0.001):.0f}')") msg/s"

  u=$(wait_run_unique_long "$run" "$written" 1800)
  t2=$(now_ms)
  rm -f /tmp/bench-sampler-on; wait "$sampler_pid" 2>/dev/null
  [[ "$u" == "$written" ]] \
    && ok "$name: доставка завершена: цикл $(( t2 - t0 ))мс, drain после Writer $(( t2 - t1 ))мс" \
    || bad "$name: доставлено $u из $written за 900с"

  curl -s --max-time 10 "http://localhost:$CDC_PORT/api/cdc/state" -o /tmp/bench-cdc-after.json
  code=$(validate_run "$run" true)
  if [[ "$code" == "200" ]]; then
    assert_verdict_pass_dup "$name"
  else
    bad "$name: validate → $code: $(cat /tmp/ha-verdict.json)"
  fi
  cp /tmp/ha-verdict.json "/tmp/ha-verdict-$run.json"

  local e2e_sum1 e2e_cnt1
  read -r e2e_sum1 e2e_cnt1 <<< "$(prom_hist_pair "cdc_${ARM}_e2e_latency_milliseconds")"

  # артефакты §13.4 (коллектор работает с последним завершённым прогоном — это наш run)
  local collector_out run_dir
  collector_out=$(./scripts/reports/collect-results.sh "$run" --arm "$ARM" \
    --notes "benchmark $name: $desc" --out "$CYCLE_DIR/runs" 2>&1)
  echo "$collector_out" | sed 's/^/  /' >&2
  run_dir=$(echo "$collector_out" | grep '^артефакты:' | sed 's/^артефакты: //; s|/result\.json.*||')
  [[ -n "$run_dir" ]] && cp "/tmp/ha-verdict-$run.json" "$run_dir/validation-verdict.json" \
    && ok "$name: артефакты прогона: $run_dir"

  # per-сценарный JSON: тайминги, дельты счётчиков, сэмплы доставки, CPU/RAM по ролям
  SCEN_JSON="$CYCLE_DIR/scenarios/$name.json"
  SCEN_NAME="$name" SCEN_DESC="$desc" SCEN_SPEC="$spec" SCEN_RUN_ID="$run" \
  SCEN_T0="$t0" SCEN_T1="$t1" SCEN_T2="$t2" \
  E2E_SUM0="$e2e_sum0" E2E_CNT0="$e2e_cnt0" E2E_SUM1="$e2e_sum1" E2E_CNT1="$e2e_cnt1" \
  CDC_BEFORE="/tmp/bench-cdc-before.json" CDC_AFTER="/tmp/bench-cdc-after.json" \
  WRITER_JSON="/tmp/bench-writer.json" VERDICT_JSON="/tmp/ha-verdict.json" \
  SAMPLES_TSV="$CYCLE_DIR/cdc-samples/$name.tsv" DOCKER_STATS_CSV="$DOCKER_STATS_CSV" \
  SCEN_OUT="$SCEN_JSON" BENCH_ARM="$ARM" python3 <<'PY'
import csv, json, os
from datetime import datetime

def load(p, d=None):
    try:
        return json.load(open(p))
    except Exception:
        return d

name = os.environ["SCEN_NAME"]
writer = load(os.environ["WRITER_JSON"], {})
before = load(os.environ["CDC_BEFORE"], {})
after = load(os.environ["CDC_AFTER"], {})
verdict = load(os.environ["VERDICT_JSON"], {})
t0, t1, t2 = (int(os.environ[k]) for k in ("SCEN_T0", "SCEN_T1", "SCEN_T2"))

def f(d, k):
    v = d.get(k)
    return v if isinstance(v, (int, float)) else 0

scen = {
    "name": name,
    "description": os.environ["SCEN_DESC"],
    "spec": json.loads(os.environ["SCEN_SPEC"]),
    "runId": os.environ["SCEN_RUN_ID"],
    "arm": os.environ.get("BENCH_ARM", ""),
    "timings": {
        "t0EpochMs": t0,
        "t2EpochMs": t2,
        "writerMs": t1 - t0,
        "deliveryTotalMs": t2 - t0,       # старт записи → всё подтверждено валидатором
        "drainMs": t2 - t1,               # Writer finished → всё доставлено
    },
    "writer": {
        "written": f(writer, "writtenMessages"),
        "failed": f(writer, "failedMessages"),
        "ambiguousUnresolved": f(writer, "ambiguousUnresolved"),
        "durationSeconds": writer.get("durationSeconds"),
        "writerRatePerSecond": round(f(writer, "writtenMessages") / max(writer.get("durationSeconds") or 1e-3, 1e-3), 1),
        "commitP50Ms": writer.get("commitP50Ms"), "commitP95Ms": writer.get("commitP95Ms"), "commitP99Ms": writer.get("commitP99Ms"),
        "batchP50Ms": writer.get("batchP50Ms"), "batchP95Ms": writer.get("batchP95Ms"), "batchP99Ms": writer.get("batchP99Ms"),
        "reconnects": f(writer, "reconnects"),
    },
    "verdict": {
        "verdict": verdict.get("verdict"),
        "missing": verdict.get("missingWrittenIdsCount"),
        "unexpectedExtra": verdict.get("unexpectedExtraIdsCount"),
        "duplicates": (verdict.get("kafka") or {}).get("duplicateRecords"),
        "uniqueIds": (verdict.get("kafka") or {}).get("uniqueIds"),
    },
    "cdc": {
        "deliveredDelta": f(after, "messagesDelivered") - f(before, "messagesDelivered"),
        "readDelta": f(after, "messagesRead") - f(before, "messagesRead"),
        "readErrors": f(after, "readErrors") - f(before, "readErrors"),
        "kafkaErrors": f(after, "kafkaErrors") - f(before, "kafkaErrors"),
        "retries": f(after, "retries") - f(before, "retries"),
        "reconnects": f(after, "reconnects") - f(before, "reconnects"),
        "walLagBytesAfter": after.get("walLagBytes"),
        "e2eP50Ms": after.get("e2eP50Ms"), "e2eP95Ms": after.get("e2eP95Ms"), "e2eP99Ms": after.get("e2eP99Ms"),
    },
}

# средний e2e за сценарий: дельты счётчиков гистограммы Prometheus
try:
    ds = float(os.environ["E2E_SUM1"]) - float(os.environ["E2E_SUM0"])
    dc = float(os.environ["E2E_CNT1"]) - float(os.environ["E2E_CNT0"])
    if dc > 0:
        scen["cdc"]["e2eMeanMs"] = round(ds / dc, 1)
        scen["cdc"]["e2eSamples"] = int(dc)
except Exception:
    pass

# сэмплы доставки: скорость = рост delivered. Каденс сэмплера ~1.5-2с (curl+python),
# поэтому peak — максимум СКОРОСТИ МЕЖДУ СОСЕДНИМИ сэмплами (нормировано к 1с),
# avg — активное окно (первый→последний рост); при <2 ростов (burst короче каденса)
# avg берётся overall = delivered/окно сценария. Разрешение = samplerRows.
try:
    rows = []
    with open(os.environ["SAMPLES_TSV"]) as fh:
        rd = csv.DictReader(fh)
        for r in rd:
            rows.append((int(r["ts_ms"]), int(r["delivered"])))
    grows = [(rows[i][0], rows[i][1]) for i in range(1, len(rows)) if rows[i][1] > rows[i-1][1]]
    if len(grows) >= 2:
        span_ms = grows[-1][0] - grows[0][0]
        if span_ms > 0:
            scen["cdc"]["deliveryRateAvgPerSecond"] = round(
                (grows[-1][1] - grows[0][1]) / (span_ms / 1000), 1)
    peak = 0.0
    for i in range(1, len(rows)):
        dt = (rows[i][0] - rows[i-1][0]) / 1000
        if dt > 0:
            peak = max(peak, (rows[i][1] - rows[i-1][1]) / dt)
    scen["cdc"]["deliveryRatePeakPerSecond"] = round(peak, 1)
    scen["cdc"]["samplerRows"] = len(rows)
except Exception:
    pass
# overall-скорость всегда доступна (не зависит от каденса сэмплера)
try:
    tot_s = max((t2 - t0) / 1000, 1e-3)
    scen["cdc"]["deliveryRateOverallPerSecond"] = round(
        scen["cdc"]["deliveredDelta"] / tot_s, 1)
except Exception:
    pass

# per-role CPU/RAM из docker stats (срез 5с по всему циклу → окно сценария).
# Burst-сценарии короче интервала сэмплера → окно расширяется до >=12с
# (паддинг фиксируется в resourceWindowMs; cpuAvg за паддинг занижен, cpuMax честен).
def role_stats(prefixes):
    pad_b, pad_a = 2000, 2000
    span = t2 - t0
    if span < 12000:
        extra = 12000 - span
        pad_b += extra // 2
        pad_a += extra - extra // 2
    lo, hi = t0 - pad_b, t2 + pad_a
    sums = []  # на срез: суммарный cpu% по роли
    mems = []
    try:
        with open(os.environ["DOCKER_STATS_CSV"]) as fh:
            rd = csv.DictReader(fh)
            buckets = {}
            for r in rd:
                try:
                    ts = datetime.strptime(r["timestamp"], "%Y-%m-%dT%H:%M:%S%z").timestamp() * 1000
                except Exception:
                    continue
                if not (lo <= ts <= hi):
                    continue
                if not any(r["container"].startswith(p) for p in prefixes):
                    continue
                b = buckets.setdefault(int(ts // 5000), {"cpu": 0.0, "mem": 0})
                try:
                    b["cpu"] += float(r["cpu_percent"] or 0)
                    b["mem"] += int(r["mem_bytes"] or 0)
                except Exception:
                    pass
            sums = [b["cpu"] for b in buckets.values()]
            mems = [b["mem"] for b in buckets.values()]
    except Exception:
        pass
    if not sums:
        return None
    return {
        "cpuAvgPercent": round(sum(sums) / len(sums), 1),
        "cpuMaxPercent": round(max(sums), 1),
        "memAvgBytes": round(sum(mems) / len(mems)),
        "memMaxBytes": max(mems),
        "slices": len(sums),
        "resourceWindowMs": hi - lo,
    }

proj = "streaming-polling-outbox"
scen["resources"] = {
    "cdc": role_stats([f"{proj}-cdc-"]),
    "postgresql": role_stats([f"{proj}-pg1", f"{proj}-pg2", f"{proj}-pg3"]),
    "kafka": role_stats([f"{proj}-kafka1", f"{proj}-kafka2", f"{proj}-kafka3"]),
}

json.dump(scen, open(os.environ["SCEN_OUT"], "w"), ensure_ascii=False, indent=2, sort_keys=True)

d, c, v = scen["timings"], scen["cdc"], scen["verdict"]
print(f"    → writer {scen['writer']['writerRatePerSecond']} msg/s"
      f" | доставка {d['deliveryTotalMs']}мс (drain {d['drainMs']}мс)"
      f" | cdc avg {c.get('deliveryRateAvgPerSecond','?')} пик {c.get('deliveryRatePeakPerSecond','?')} msg/s"
      f" | e2e p50/p95/p99 {c.get('e2eP50Ms','?')}/{c.get('e2eP95Ms','?')}/{c.get('e2eP99Ms','?')}мс"
      f" | дубликатов {v['duplicates']}")
PY
  [[ $? -eq 0 && -f "$SCEN_JSON" ]] && ok "$name: сводка: $SCEN_JSON" || bad "$name: сводка не собрана"
}

# ---------- Готовность свежего стенда ----------
wait_stand_ready() {
  local info slot_st i
  section "Готовность стенда (arm=$ARM)"
  wait_cluster_ready 600 && ok "Patroni: 3 ноды running/streaming, leader=$(leader_name)" \
    || { bad "кластер Patroni не собрался за 600с"; return 1; }
  slot_st=""
  for i in $(seq 1 75); do
    slot_st=$(slot_field wal_status)
    [[ "$slot_st" == "reserved" || "$slot_st" == "extended" ]] && break
    sleep 4
  done
  [[ "$slot_st" == "reserved" || "$slot_st" == "extended" ]] && ok "слот: wal_status=$slot_st" \
    || { bad "слот не готов (wal_status='$slot_st') за 300с"; return 1; }
  local synced
  synced=$(wait_slot_synced 600)
  [[ -n "$synced" ]] && ok "слот synced на standby ($synced)" || bad "слот не synced за 600с"
  wait_kafka_healthy 300 && ok "Kafka: 3 брокера healthy" || { bad "Kafka не healthy"; return 1; }
  for i in $(seq 1 3); do wait_validator_ready && break; sleep 5; done
  [[ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 4 "$VBASE/ready")" == "200" ]] \
    && ok "KafkaValidator /ready=200" || { bad "валидатор не готов"; return 1; }
  wait_cdc_state running 600 && ok "CDC $ARM: state=running" \
    || { bad "CDC не running: $(cdc_state_raw)"; return 1; }
  local wstate
  for i in $(seq 1 30); do
    wstate=$(curl -s --max-time 5 "$WBASE/api/generation/state" 2>/dev/null)
    [[ -n "$wstate" ]] && break
    sleep 2
  done
  [[ -n "$wstate" ]] && ok "Writer отвечает" || { bad "Writer недоступен"; return 1; }
}

# ---------- Итоговый отчёт цикла ----------
cycle_report() { # → benchmark-report.json + копия в results/benchmark/
  local total=0 verdict_ok=0 s j
  mkdir -p results/benchmark
  BENCH_ARM="$ARM" CYCLE_DIR="$CYCLE_DIR" CYCLE_STAMP="$CYCLE_STAMP" \
  REPORT="$CYCLE_DIR/benchmark-report.json" SUMMARY="results/benchmark/benchmark-$ARM.json" \
  python3 <<'PY'
import csv, glob, json, os

arm = os.environ["BENCH_ARM"]
cycle_dir = os.environ["CYCLE_DIR"]
scens = []
for p in sorted(glob.glob(os.path.join(cycle_dir, "scenarios", "*.json"))):
    try:
        scens.append(json.load(open(p)))
    except Exception:
        pass

# версии/гит — из первого собранного прогона
vers, git = {}, {}
runs = sorted(glob.glob(os.path.join(cycle_dir, "runs", "*", "result.json")))
if runs:
    r = json.load(open(runs[0]))
    vers = r.get("versions", {})
    git = r.get("git", {})

# ресурсы за весь блок сценариев [первый t0 .. последний t2]: честное сравнение
# CPU/RAM между армами на ОДИНАКОВОЙ работе (per-сценарные окна коротки для 5с-сэмплера)
def cycle_role_resources(prefixes, lo, hi):
    from datetime import datetime
    sums, mems = [], []
    try:
        with open(os.path.join(cycle_dir, "docker-stats.csv")) as fh:
            rd = csv.DictReader(fh)
            buckets = {}
            for r in rd:
                try:
                    ts = datetime.strptime(r["timestamp"], "%Y-%m-%dT%H:%M:%S%z").timestamp() * 1000
                except Exception:
                    continue
                if not (lo <= ts <= hi):
                    continue
                if not any(r["container"].startswith(p) for p in prefixes):
                    continue
                b = buckets.setdefault(int(ts // 5000), {"cpu": 0.0, "mem": 0})
                try:
                    b["cpu"] += float(r["cpu_percent"] or 0)
                    b["mem"] += int(r["mem_bytes"] or 0)
                except Exception:
                    pass
            sums = [b["cpu"] for b in buckets.values()]
            mems = [b["mem"] for b in buckets.values()]
    except Exception:
        pass
    if not sums:
        return None
    return {"cpuAvgPercent": round(sum(sums) / len(sums), 1),
            "cpuMaxPercent": round(max(sums), 1),
            "memAvgBytes": round(sum(mems) / len(mems)),
            "memMaxBytes": max(mems), "slices": len(sums)}

t0s = [s["timings"]["t0EpochMs"] for s in scens if s.get("timings", {}).get("t0EpochMs")]
t2s = [s["timings"]["t2EpochMs"] for s in scens if s.get("timings", {}).get("t2EpochMs")]
resources = {}
if t0s and t2s:
    proj = "streaming-polling-outbox"
    resources = {
        "window": {"fromMs": min(t0s), "toMs": max(t2s)},
        "cdc": cycle_role_resources([f"{proj}-cdc-"], min(t0s), max(t2s)),
        "postgresql": cycle_role_resources([f"{proj}-pg1", f"{proj}-pg2", f"{proj}-pg3"], min(t0s), max(t2s)),
        "kafka": cycle_role_resources([f"{proj}-kafka1", f"{proj}-kafka2", f"{proj}-kafka3"], min(t0s), max(t2s)),
        "writer": cycle_role_resources([f"{proj}-writer-"], min(t0s), max(t2s)),
    }

report = {
    "schema": "streaming-polling-outbox/benchmark/v1",
    "cycle": "A (polling)" if arm == "polling" else "B (streaming)",
    "arm": arm,
    "startedAt": os.environ["CYCLE_STAMP"],
    "git": git,
    "versions": vers,
    "resources": resources,
    "scenarios": scens,
    "totals": {
        "scenarios": len(scens),
        "verdictsPass": sum(1 for s in scens if s.get("verdict", {}).get("verdict") == "pass"),
        "messagesDelivered": sum(s.get("cdc", {}).get("deliveredDelta", 0) for s in scens),
        "duplicatesTotal": sum(s.get("verdict", {}).get("duplicates") or 0 for s in scens),
        "readErrors": sum(s.get("cdc", {}).get("readErrors", 0) for s in scens),
        "kafkaErrors": sum(s.get("cdc", {}).get("kafkaErrors", 0) for s in scens),
    },
}
json.dump(report, open(os.environ["REPORT"], "w"), ensure_ascii=False, indent=2, sort_keys=True)
json.dump(report, open(os.environ["SUMMARY"], "w"), ensure_ascii=False, indent=2, sort_keys=True)
print(f"  отчёт цикла: {os.environ['REPORT']}")
print(f"  компактная копия (коммитится): {os.environ['SUMMARY']}")
t = report["totals"]
print(f"  сценариев {t['scenarios']}, pass {t['verdictsPass']}/{t['scenarios']}, "
      f"доставлено {t['messagesDelivered']}, дубликатов {t['duplicatesTotal']}, "
      f"readErrors {t['readErrors']}, kafkaErrors {t['kafkaErrors']}")
PY
}

# ---------- Один цикл (§11) ----------
run_cycle() { # arm
  ARM=$1
  if [[ "$ARM" == "polling" ]]; then CDC_PORT=$PPORT; else CDC_PORT=$SPORT; fi
  CYCLE_STAMP=$(date +%Y-%m-%dT%H:%M:%S)
  CYCLE_DIR="results/raw/$(date +%Y%m%d-%H%M%S)-benchmark-$ARM"

  section "ЦИКЛ $([[ "$ARM" == "polling" ]] && echo A || echo B) (arm=$ARM): чистая инфраструктура (§11)"
  if [[ "$SKIP_WIPE" == "0" ]]; then
    ./scripts/reset/wipe.sh || { bad "wipe не удался"; return 1; }
  else
    ok "начальный wipe пропущен (--skip-wipe): стенд должен быть чистым и поднятым под arm=$ARM"
  fi
  mkdir -p "$CYCLE_DIR"
  $DC --profile "$ARM" up -d --build > "$CYCLE_DIR/compose-up.log" 2>&1 \
    && ok "docker compose --profile $ARM up -d --build (лог: $CYCLE_DIR/compose-up.log)" \
    || { bad "compose up не удался — см. $CYCLE_DIR/compose-up.log"; return 1; }
  wait_stand_ready || { bad "стенд не готов, цикл прерван"; return 1; }

  section "Warmup (не входит в сценарии: JIT, пулы, прогрев валидатора)"
  local wrun wu
  wrun=$(run_writer "$WARMUP_SPEC")
  if [[ "$wrun" != FAIL:* ]]; then
    writer_results
    wu=$(wait_run_unique_long "$wrun" "$(writer_field 'd["writtenMessages"]')" 300)
    validate_run "$wrun" true >/dev/null
    [[ "$(vd 'd["verdict"]')" == "pass" && "$wu" == "$(writer_field 'd["writtenMessages"]')" ]] \
      && ok "warmup: 500 сообщений доставлены, вердикт pass" \
      || bad "warmup не прошёл (вердикт $(vd 'd["verdict"]'), unique=$wu)"
  else
    bad "warmup-прогон не completed ($wrun)"
  fi

  # сэмплер docker stats на весь блок сценариев (срез 5с; cAdvisor не отдаёт per-container)
  DOCKER_STATS_CSV="$CYCLE_DIR/docker-stats.csv"
  ./scripts/reports/sample-docker-stats.sh "$DOCKER_STATS_CSV" 5 >/dev/null 2>&1 &
  local stats_pid=$!
  sleep 3

  local idx
  for idx in "${!SCEN_NAMES[@]}"; do
    scenario_enabled "${SCEN_NAMES[$idx]%%-*}" || continue
    run_scenario "$idx" || bad "сценарий ${SCEN_NAMES[$idx]} прерван"
  done

  kill "$stats_pid" 2>/dev/null
  wait "$stats_pid" 2>/dev/null

  section "Отчёт цикла (arm=$ARM)"
  cycle_report
}

# ---------- Сводка A vs B (если оба цикла в этом запуске) ----------
both_summary() {
  [[ -f results/benchmark/benchmark-polling.json && -f results/benchmark/benchmark-streaming.json ]] \
    || return 0
  python3 <<'PY'
import json
a = json.load(open("results/benchmark/benchmark-polling.json"))
b = json.load(open("results/benchmark/benchmark-streaming.json"))
out = {"schema": "streaming-polling-outbox/benchmark-summary/v1"}
rows = []
for sa in a.get("scenarios", []):
    sb = next((x for x in b.get("scenarios", []) if x["name"] == sa["name"]), None)
    if not sb:
        continue
    def pick(s, *path):
        v = s
        for k in path:
            v = (v or {}).get(k)
        return v
    rows.append({
        "scenario": sa["name"],
        "polling": {
            "deliveryTotalMs": pick(sa, "timings", "deliveryTotalMs"),
            "drainMs": pick(sa, "timings", "drainMs"),
            "cdcRateOverall": pick(sa, "cdc", "deliveryRateOverallPerSecond"),
            "cdcRateAvg": pick(sa, "cdc", "deliveryRateAvgPerSecond"),
            "cdcRatePeak": pick(sa, "cdc", "deliveryRatePeakPerSecond"),
            "e2eMeanMs": pick(sa, "cdc", "e2eMeanMs"),
            "e2eP95Ms": pick(sa, "cdc", "e2eP95Ms"),
            "duplicates": pick(sa, "verdict", "duplicates"),
        },
        "streaming": {
            "deliveryTotalMs": pick(sb, "timings", "deliveryTotalMs"),
            "drainMs": pick(sb, "timings", "drainMs"),
            "cdcRateOverall": pick(sb, "cdc", "deliveryRateOverallPerSecond"),
            "cdcRateAvg": pick(sb, "cdc", "deliveryRateAvgPerSecond"),
            "cdcRatePeak": pick(sb, "cdc", "deliveryRatePeakPerSecond"),
            "e2eMeanMs": pick(sb, "cdc", "e2eMeanMs"),
            "e2eP95Ms": pick(sb, "cdc", "e2eP95Ms"),
            "duplicates": pick(sb, "verdict", "duplicates"),
        },
    })
out["scenarios"] = rows
json.dump(out, open("results/benchmark/summary.json", "w"), ensure_ascii=False, indent=2, sort_keys=True)
print("  сводка A/B: results/benchmark/summary.json")
for r in rows:
    p, s = r["polling"], r["streaming"]
    print(f"  {r['scenario']:16} drain {str(p['drainMs']):>6}/{str(s['drainMs']):>6}мс"
          f" | rate avg {str(p['cdcRateAvg']):>7}/{str(s['cdcRateAvg']):>7}"
          f" | peak {str(p['cdcRatePeak']):>5}/{str(s['cdcRatePeak']):>5} msg/s"
          f" | e2e p95 {str(p['e2eP95Ms']):>7}/{str(s['e2eP95Ms']):>7}мс")
PY
}

# ---------- Запуск ----------
if [[ "$CYCLE_ARG" == "all" || "$CYCLE_ARG" == "polling" ]]; then
  run_cycle polling
  # §11 шаг 7 цикла A: полное удаление тестового состояния перед циклом B / после одиночного A
  section "Цикл A завершён: полное удаление тестового состояния (§11 шаг 7)"
  ./scripts/reset/wipe.sh && ok "стенд вытерт после цикла A"
  [[ "$CYCLE_ARG" == "polling" ]] \
    && echo "Стенд остановлен (§11). Поднять заново: docker compose --profile streaming up -d --build"
fi
if [[ "$CYCLE_ARG" == "all" || "$CYCLE_ARG" == "streaming" ]]; then
  [[ "$CYCLE_ARG" == "all" ]] && SKIP_WIPE=0   # только что вытерли — полный путь цикла B
  run_cycle streaming
fi

[[ "$CYCLE_ARG" == "all" ]] && { section "Сводка A/B"; both_summary; }

finish
