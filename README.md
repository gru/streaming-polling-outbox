# PostgreSQL 17 Logical Outbox CDC Benchmark — исследовательский стенд

Сравнение двух способов доставки outbox-сообщений из PostgreSQL 17 в Apache
Kafka: **SQL Polling** (`pg_logical_slot_peek_binary_changes()` + advance)
против **Streaming Replication** (Npgsql Logical Replication API, `PgOutput`).
Writer делает `INSERT` в outbox-таблицу; CDC читает изменения через logical
slot (SELECT-polling таблицы не используется). Причина отказа от
`pg_logical_emit_message`: [docs/deviations.md](docs/deviations.md).

Требования: [requirements.md](requirements.md). Отклонения, обнаруженные
практикой (в т.ч. замена pgoutput → test_decoding): [docs/deviations.md](docs/deviations.md).

Состав инфраструктуры (этапы 1-2, готовы):

- **Patroni + PostgreSQL 17.2**, 3 ноды, синхронная репликация; DCS — etcd 3 ноды
- **Нативная синхронизация failover logical slots** PostgreSQL 17 (`sync_replication_slots`,
  слот `outbox_slot`, плагин `pgoutput`, `failover=true`, `synced=true` на standby)
- **Outbox-таблица `public.outbox_messages`** (id/topic/body/created_at) в publication
  `outbox_publication`; CHECK на `topic='outbox.events'` — произвольная маршрутизация запрещена
- **HAProxy** (маршрутизация на primary, stats :8404) + **PgBouncer** (transaction pool)
- **Kafka KRaft** 3 брокера, topic `outbox.events`: 6 партиций, RF=3, `min.insync.replicas=2`,
  `auto.create.topics.enable=false`
- **Prometheus** (pull, 5s) + healthchecks у всех сервисов
- Идемпотентная инициализация: роли `sys_writer` / `sys_app` / `sys_replication`
  (минимальные привилегии), БД, publication, слот

**Writer (этап 3, готов)** — ASP.NET Core 8 (`net8.0`), управляемый генератор нагрузки:
транзакционные INSERT-батчи в `public.outbox_messages` от `sys_writer`; по умолчанию через
PgBouncer, диагностический режим `direct` (через HAProxy, мимо PgBouncer) — без изменения
кода (env `WRITER_CONNECTION_MODE` или поле `connectionMode` в запросе старта). От Kafka не
зависит. Метрики OpenTelemetry → `/metrics`. UUID (`Guid` v7) и `created_at` задаёт клиент —
у `sys_writer` нет SELECT-привилегии, поэтому `INSERT ... RETURNING` недоступен; повтор батча
с теми же UUID после сбоя соединения делает неоднозначный commit разрешимым (unique violation
по PK = прошлый commit прошёл).

## Быстрый старт

```bash
cp .env.example .env            # при необходимости поменяйте пароли
docker compose up -d --build    # сборка Patroni/Writer-образов + запуск стенда
./scripts/check-infra.sh        # полная проверка инфраструктуры (все проверки должны быть ✔)
./scripts/test/smoke-writer.sh  # живая проверка Writer (прогон, БД, метрики, direct, рестарт)
```

Хост-порты: HAProxy (primary) — **5432**, PgBouncer — 6432, Writer HTTP API — **18080**,
Patroni REST — 8008/8009/8010, Kafka — 9092-9094, Prometheus — 9090, HAProxy stats — 8404.

## Writer: управление нагрузкой

HTTP API (Minimal API, порт `WRITER_HTTP_PORT`, по умолчанию 18080). После старта и после
рестарта контейнера Writer находится в **Idle** — ничего не генерирует, пока не пришёл start.

```bash
W=http://localhost:18080

# Запуск прогона: 10 MB объёмом (payload по 1 KB), скорость 2000 msg/s
curl -s -X POST $W/api/generation/start -H 'Content-Type: application/json' -d '{
  "totalPayloadBytes": 10485760, "messageSizeBytes": 1024,
  "ratePerSecond": 2000, "batchSize": 100, "parallelism": 4
}'

# Другие цели: totalMessages / durationSeconds / runUntilStopped (до явного stop).
# Все параметры опциональны (null → дефолты из docker-compose env WRITER__DEFAULTS__*).

# Текущее состояние (idle|running|stopping|completed|failed|stopped + прогресс)
curl -s $W/api/generation/state | python3 -m json.tool

# Остановка (мягкая: ждёт завершения in-flight батчей)
curl -s -X POST $W/api/generation/start -H 'Content-Type: application/json' \
  -d '{"runUntilStopped": true, "ratePerSecond": 100}' >/dev/null
curl -s -X POST $W/api/generation/stop | python3 -m json.tool

# Итоги последнего прогона: счётчики, p50/p95/p99 commit, фактическая скорость, ошибки
curl -s $W/api/generation/results | python3 -m json.tool

# Ожидаемый набор id прогона (для KafkaValidator)
curl -s $W/api/generation/results/ids > /tmp/writer-ids.json

# Health / readiness / метрики
curl -s $W/health; curl -s $W/ready; curl -s $W/metrics | grep writer_

# Диагностический прогон напрямую к primary (мимо PgBouncer)
curl -s -X POST $W/api/generation/start -H 'Content-Type: application/json' \
  -d '{"totalMessages": 1000, "connectionMode": "direct"}'
```

Тело сообщения: `body = {"eventType":"TestEvent","runId":"<run guid>","payload":"<ASCII строка messageSizeBytes>"}`;
`runId` уникален для прогона и используется валидатором Kafka (requirements.md разд. 15).
Прогон завершается по первой достигнутой цели из заданных (count / volume / duration);
сброс соединения не прерывает прогон — ретраи с exponential backoff, дубликатов в outbox
не возникает (PK-детект повторного commit), «неоднозначные» батчи честно считаются отдельно.
Unit-тесты: `docker run --rm -v "$PWD":/w -w /w mcr.microsoft.com/dotnet/sdk:8.0 dotnet test`.

## Полезные команды

Пароли в примерах (`postgres`, `writer_secret`, …) — значения по умолчанию
из `.env.example`; при смене подставляйте свои.

```bash
# Состояние Patroni-кластера (кто leader / sync_standby)
curl -s localhost:8008/cluster | python3 -m json.tool

# PostgreSQL через PgBouncer под прикладной ролью
docker compose exec -e PGPASSWORD=writer_secret pg1 \
  psql "host=pgbouncer port=6432 user=sys_writer dbname=postgres"

# Записать сообщение в outbox руками (sys_writer; без RETURNING — требует SELECT-привилегию;
# Writer генерирует UUID на клиенте и передаёт явно)
docker compose exec -e PGPASSWORD=writer_secret pg1 psql "host=pgbouncer port=6432 user=sys_writer dbname=postgres" \
  -c "INSERT INTO public.outbox_messages (id, topic, body) VALUES (gen_random_uuid(), 'outbox.events', '{\"eventType\":\"TestEvent\"}')"

# Посмотреть содержимое слота, не потребляя (бинарный pgoutput: B/R/I/C записи)
docker compose exec -e PGPASSWORD=postgres pg1 psql -h localhost -U postgres -d postgres \
  -c "SELECT lsn, xid FROM pg_logical_slot_peek_binary_changes('outbox_slot', NULL, 20, 'proto_version','1','publication_names','outbox_publication')"

# Состояние слотов на всех нодах (failover sync)
for n in pg1 pg2 pg3; do docker compose exec -e PGPASSWORD=postgres $n \
  psql -h localhost -U postgres -d postgres -Atc \
  "SELECT '$n '||plugin||' synced='||synced FROM pg_replication_slots WHERE slot_name='outbox_slot'"; done

# Kafka: описание topic
docker compose exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic outbox.events

# Повторная идемпотентная инициализация (роли/БД/publication/слот/userlist)
./scripts/init/apply-bootstrap.sh

# Полный сброс стенда (только ресурсы этого проекта)
./scripts/reset/wipe.sh
```

## Структура

```
docker-compose.yml              — вся инфраструктура + Writer
streaming-polling-outbox.sln    — .NET-решение (Writer + тесты)
.env.example                    — шаблон переменных окружения (cp .env.example .env)
AGENTS.md                       — инструкция для агентов (ограничения, грабли)
src/Writer/                     — Writer: ASP.NET Core 8, net8.0 (Minimal API + генератор)
tests/Writer.UnitTests/         — xunit-тесты критичной логики Writer
infrastructure/
  patroni/                      — Dockerfile (postgres:17.2 + patroni 4.1.5) и patroni.yml
  postgres/init/                — идемпотентный SQL-bootstrap (роли, outbox-таблица, publication, слот)
  haproxy/ pgbouncer/ prometheus/
scripts/
  check-infra.sh                — полная проверка стенда (exit 0 = всё зелёное)
  test/smoke-writer.sh          — живая проверка Writer (прогон/БД/метрики/direct/рестарт)
  init/apply-bootstrap.sh       — повторный запуск инициализации БД
  reset/wipe.sh                 — полный сброс (down -v)
docs/
  deviations.md                 — отклонения от ТЗ и их причины
```

## Что дальше (по плану requirements.md)

Этапы 4+: CDC Polling (`sys_app` + SECURITY DEFINER + разбор бинарного pgoutput + KafkaFlow),
CDC Streaming (`sys_replication`, Npgsql PgOutput), метрики CDC, HA-тесты TEST-HA-001..008,
benchmark и сравнительный отчёт. Отложено владельцем на поздний этап: Schema Registry + Avro-тело
сообщения (сейчас body — JSON `jsonb`).
