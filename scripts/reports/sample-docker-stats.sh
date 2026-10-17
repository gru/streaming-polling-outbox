#!/usr/bin/env bash
# Периодический сэмплер per-container CPU/RAM (docker stats -> CSV).
#
# Причина существования (этап 6): cAdvisor на Docker Desktop с containerd
# image store не может определить rw-layer контейнеров и не отдаёт
# per-container серии (docs/deviations.md 7) — per-container CPU/RAM для
# сравнения A/B (requirements.md 16) собирается этим скриптом.
#
# Использование:
#   scripts/reports/sample-docker-stats.sh результаты.csv [интервал_сек]   # цикл, Ctrl+C — стоп
#   scripts/reports/sample-docker-stats.sh результаты.csv --once           # один срез
set -uo pipefail
cd "$(dirname "$0")/../.."

out=${1:?usage: sample-docker-stats.sh OUT.csv [INTERVAL_SEC|--once]}
mode=${2:-5}

sample_once() {
  docker stats --no-stream --format json 2>/dev/null | python3 -c '
import json, sys, re, time

def mem_bytes(usage):
    # "123.4MiB / 4GiB" -> байты использованного
    used = usage.split("/")[0].strip()
    m = re.match(r"([\d.]+)\s*([KMGT]?i?B)", used, re.I)
    if not m:
        return None
    val = float(m.group(1))
    mult = {"B": 1, "KB": 1e3, "KIB": 1024, "MB": 1e6, "MIB": 1024**2,
            "GB": 1e9, "GIB": 1024**3, "TB": 1e12, "TIB": 1024**4}
    return round(val * mult.get(m.group(2).upper().replace("I", "I"), 1))

ts = time.strftime("%Y-%m-%dT%H:%M:%S%z")
rows = []
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    d = json.loads(line)
    cpu = d.get("CPUPerc", "").rstrip("%")
    rows.append((ts, d.get("Name", ""), cpu,
                 mem_bytes(d.get("MemUsage", "")) or "",
                 d.get("MemPerc", "").rstrip("%"), d.get("PIDs", "")))
for r in rows:
    print(",".join(str(x) for x in r))
'
}

if [[ ! -s "$out" ]]; then
  echo "timestamp,container,cpu_percent,mem_bytes,mem_percent,pids" > "$out"
fi

if [[ "$mode" == "--once" ]]; then
  sample_once >> "$out"
  echo "срез записан: $out ($(($(wc -l < "$out") - 1)) строк)"
  exit 0
fi

echo "сэмплер запущен: интервал ${mode}s, выход $out (Ctrl+C — остановить)"
while true; do
  sample_once >> "$out" || echo "WARN: docker stats не ответил" >&2
  sleep "$mode"
done
