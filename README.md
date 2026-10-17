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
- **Прикладная БД `outbox`** (не `postgres` — реальная конфигурация приложения; отдельной
  БД требует и failover-синхронизация слота: PG17 slotsync worker синхронизирует слоты
  только БД из `dbname` в `primary_conninfo`; Patroni настроен официальной опцией
  `postgresql.database: outbox` + кастомный bootstrap-метод, патчей Patroni нет)
- **Нативная синхронизация failover logical slots** PostgreSQL 17 (`sync_replication_slots`,
  слот `outbox_slot`, плагин `pgoutput`, `failover=true`, `synced=true` на standby;
  пережил контролируемый switchover с сохранением restart_lsn)
- **Outbox-таблица `public.outbox_messages`** (id/topic/body/created_at) в publication
  `outbox_publication`; CHECK на `topic='outbox.events'` — произвольная маршрутизация запрещена
- **HAProxy** (маршрутизация на primary, stats :8404) + **PgBouncer** (transaction pool)
- **Kafka KRaft** 3 брокера, topic `outbox.events`: 6 партиций, RF=3, `min.insync.replicas=2`,
  `auto.create.topics.enable=false`
- **Prometheus** (pull, 5s) + healthchecks у всех сервисов
- Идемпотентная инициализация: роли `sys_writer` / `sys_app` / `sys_replication`
  (минимальные привилегии), БД, publication, слот

**Writer (этап 3, готов)** — ASP.NET Core 8 (`net8.0`), управляемый генератор нагрузки:
батчи «INSERT + DELETE тех же строк» в одной транзакции от `sys_writer` — **outbox-таблица
всегда пустая**, сообщение живёт только в WAL (logical decoding эмитит изменения удалённых
в той же транзакции строк — проверено). По умолчанию подключение через PgBouncer,
диагностический режим `direct` (через HAProxy, мимо PgBouncer) — без изменения кода
(env `WRITER_CONNECTION_MODE` или поле `connectionMode` в запросе старта). От Kafka не
зависит. Метрики OpenTelemetry → `/metrics`. UUID (`Guid` v7) и `created_at` задаёт клиент;
повтор батча с теми же UUID после сбоя соединения делает неоднозначный commit разрешимым
(unique violation по PK = прошлый commit прошёл).

**CDC Polling (этап 4, готов)** — `src/CdcPolling` + общая библиотека `src/CdcCommon`
(LSN/конверт/backoff/percentiles — общие с Streaming; бинарный парсер pgoutput —
только Polling). Цикл: **peek → разбор pgoutput → Kafka (KafkaFlow, acks=all,
идемпотентный producer) → advance**;
LSN подтверждается **только после** delivery-репорта Kafka (at-least-once, requirements.md 10):
при недоступной Kafka подтверждения нет и WAL копится; инвалидированный слот — терминальная
состояние ошибки с диагностикой, без молчаливого пересоздания. Читает от `sys_app` через
SECURITY DEFINER-функции `cdc.peek_outbox_changes` / `cdc.advance_outbox_slot` /
`cdc.outbox_slot_status` (слот/publication/параметры жёстко зашиты, advance дальше
последнего прочитанного peek запрещён watermark-таблицей `cdc.peek_state`; почему wrapper —
[docs/deviations.md](docs/deviations.md) §4). Insert-записи `public.outbox_messages` — события,
Delete-записи игнорируются. Kafka key = `id` (дубли at-least-once попадают в одну партицию —
консюмер дедуплицирует), value = конверт `{"id","topic","body","timestamp"}` (requirements.md 4.2).
Запускается в compose-профиле **`polling`** (Polling и Streaming никогда не работают на одном
слоте одновременно, requirements.md 11): `docker compose --profile polling up -d --build cdc-polling`.

## Быстрый старт

```bash
cp .env.example .env            # при необходимости поменяйте пароли
docker compose up -d --build    # сборка Patroni/Writer-образов + запуск стенда
./scripts/check-infra.sh        # полная проверка инфраструктуры (все проверки должны быть ✔)
./scripts/test/smoke-writer.sh  # живая проверка Writer (прогон, БД, метрики, direct, рестарт)
```

⚠️ `docker compose up -d --build` — команда **холодного старта**. На живом стенде её
повторный запуск (равно как `docker compose run init-db` без `--no-deps`) может пересоздать
pg-контейнеры и уронить Patroni-кластер; пересборка/перезапуск отдельного сервиса:
`docker compose build <svc> && docker compose up -d --no-deps <svc>`, полный рестарт —
`./scripts/reset/wipe.sh`.

Хост-порты: HAProxy (primary) — **5432**, PgBouncer — 6432, Writer HTTP API — **18080**,
CDC Polling HTTP API — **18081**, CDC Streaming HTTP API — **18082**, Patroni REST —
8008/8009/8010, Kafka — 9092-9094, Prometheus — 9090, HAProxy stats — 8404.

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

## CDC Polling: запуск и наблюдение

Запуск (профиль `polling`; обычный `docker compose up` его не поднимает):

```bash
docker compose --profile polling up -d --build cdc-polling
./scripts/test/smoke-cdc-polling.sh   # живая проверка: доставка 500 сообщений, LSN, метрики, рестарт, batch size
```

HTTP API (порт `CDC_POLLING_HTTP_PORT`, по умолчанию 18081):

```bash
C=http://localhost:18081

# Состояние: state, счётчики, lastReceived/confirmed LSN, WAL lag, слот, ошибки, p50/p95/p99 e2e
curl -s $C/api/cdc/state | python3 -m json.tool

# Health / readiness (ready = БД отвечает и слот валиден) / метрики
curl -s $C/health; curl -s $C/ready; curl -s $C/metrics | grep cdc_polling_
```

Состояния: `running` → (`waiting_kafka` | `waiting_database`) → `running`;
`error_slot` / `failed` — терминальные (диагностика в логах и `/api/cdc/state`,
восстановление — устранить причину и перезапустить контейнер; слот не пересоздаётся молча).

### Параметры (env, без правки кода)

| .env-переменная | Что задаёт | По умолчанию |
|---|---|---|
| `CDC_POLLING_POLL_BATCH_SIZE` | сколько logical changes брать одним peek (1..10000; потолок зафиксирован в SECURITY DEFINER-функции) | 100 |
| `CDC_POLLING_KAFKA_BATCH_SIZE` | прикладной batch публикации в Kafka (сообщений за цикл) | 100 |
| `CDC_POLLING_CONNECTION_MODE` | `pgbouncer` \| `direct` (через HAProxy, мимо PgBouncer) | pgbouncer |
| `CDC_POLLING_HTTP_PORT` | хост-порт HTTP API | 18081 |

Смена значения: правка `.env` + `docker compose --profile polling up -d --no-deps cdc-polling`
(контейнер пересоздаётся, позиция слота в БД — переживает). Это **разные** «батчи»:
внутренний batching Confluent-продюсера (`LingerMs`), прикладной batch Kafka, peek-пачка —
и отдельно подтверждение LSN, которое выполняется один раз на peek-выборку после доставки всего батча.

⚠️ env-префикс конфигурации самого сервиса — `CDCPOLLING__` (без подчёркивания):
провайдер .NET превращает в разделитель только двойные подчёркивания (подробнее —
deviations.md §5).
Прогон завершается по первой достигнутой цели из заданных (count / volume / duration);
сброс соединения не прерывает прогон — ретраи с exponential backoff, дубликатов в outbox
не возникает (PK-детект повторного commit), «неоднозначные» батчи честно считаются отдельно.
Unit-тесты: `docker run --rm -v "$PWD":/w -w /w mcr.microsoft.com/dotnet/sdk:8.0 dotnet test`.

## CDC Streaming: запуск и наблюдение

Ветка B бенчмарка (requirements.md 8): тот же слот и publication, но чтение —
**Npgsql Logical Replication API** (`PgOutputReplicationSlot`, pgoutput proto v1)
от `sys_replication` **напрямую через HAProxy** (replication-протокол PgBouncer не
проксирует). Цикл: **StartReplication → типизированные сообщения → Kafka (KafkaFlow,
acks=all, идемпотентный producer) → подтверждение replication feedback'ом**
(`SetReplicationStatus` + `SendStatusUpdate` — LSN `TransactionEndLsn` последнего
закоммиченного набора, и только после delivery-репортов Kafka — at-least-once,
requirements.md 8.1/10). Декодированные события идут из receiver-цикла в publisher-цикл
через ограниченный канал: заполненный канал останавливает чтение стрима (TCP
backpressure) — WAL копится, подтверждения нет; при высокой нагрузке публикуются
полные батчи, на малой — мгновенные флаши.

Отличия от Polling — таблица в [docs/deviations.md](docs/deviations.md) §6
(транспорт, подтверждение, почему у Streaming нет watermark-таблицы).

Запуск (профиль `streaming`; перед этим остановить Polling — профили изолированы,
requirements.md 11):

```bash
docker compose stop cdc-polling 2>/dev/null || true
docker compose --profile streaming up -d --build cdc-streaming
./scripts/test/smoke-cdc-streaming.sh   # живая проверка: доставка 500 сообщений, LSN, метрики, рестарт, batch size
```

HTTP API (порт `CDC_STREAMING_HTTP_PORT`, по умолчанию 18082):

```bash
S=http://localhost:18082

# Состояние: state, счётчики, lastReceived/confirmed LSN, WAL lag, глубина канала,
# слот, ошибки, p50/p95/p99 e2e
curl -s $S/api/cdc/state | python3 -m json.tool

# Health / readiness (ready = БД отвечает и слот валиден) / метрики
curl -s $S/health; curl -s $S/ready; curl -s $S/metrics | grep cdc_streaming_
```

Состояния — как у Polling: `running` → (`waiting_kafka` | `waiting_database`) →
`running`; `error_slot` / `failed` — терминальные. Разрыв replication-соединения
(failover/switchover — HAProxy рвёт TCP, `on-marked-down shutdown-sessions`)
даёт `waiting_database` + backoff + повторное подключение от `confirmed_flush`
слота (неподтверждённое передаётся повторно — дубликаты допустимы, консюмер
дедуплицирует по `id`).

### Параметры (env, без правки кода)

| .env-переменная | Что задаёт | По умолчанию |
|---|---|---|
| `CDC_STREAMING_KAFKA_BATCH_SIZE` | прикладной batch публикации (событий в одном flush-цикле) | 100 |
| `CDC_STREAMING_HTTP_PORT` | хост-порт HTTP API | 18082 |

Слот/publication/proto_version и endpoint подключения задаются в compose env
(`CDCSTREAMING__STREAMING__*`, `CDCSTREAMING__CONNECTION__*`) — значения стенда.
Смена batch: правка `.env` + `docker compose --profile streaming up -d --no-deps cdc-streaming`.

## Мониторинг инфраструктуры (этап 6, requirements.md §13.3)

Три exporter'а (always-on, без profile; порты наружу не публикуются — scrape
внутри сети, проверки с хоста через `http://localhost:9090/api/v1`):

| Требование §13.3 | Exporter | Метрики |
|---|---|---|
| PostgreSQL replication lag | postgres-exporter | `pg_replication_replay_lag_bytes{application_name}` (байты до replay; включает логический walsender `cdc-*` — это серверный WAL lag CDC) |
| Logical slot state | postgres-exporter | `pg_slot_active{slot_name}`, `pg_slot_wal_retained_bytes` (WAL retention), `pg_slot_flush_lag_bytes`, labels `wal_status`/`plugin`; `pg_slot_safe_wal_size_bytes` — появится при включении `max_slot_wal_keep_size` (WAL Pressure Test) |
| WAL retention | postgres-exporter | `pg_wal_position_bytes`, `pg_wal_dir_bytes`/`pg_wal_dir_files`, `pg_slot_wal_retained_bytes` |
| Kafka broker availability | kafka-exporter | `kafka_brokers` (=3, когда все живы) |
| Kafka topic offsets | kafka-exporter | `kafka_topic_partition_current_offset{topic,partition}` |
| Kafka consumer lag (если применимо) | kafka-exporter | `kafka_consumergroup_lag` — осмысленно с появлением консюмер-группы (KafkaValidator, этап 7) |
| CPU / RAM | cAdvisor | VM-root стенда (`container_*{id="/"}`); per-container — сэмплером ниже (см. ограничения) |

Подключение postgres-exporter: напрямую `haproxy:5432` от роли `sys_monitor`
(`pg_monitor`, INHERIT; без peek/advance/REPLICATION — мониторинг не может
двигать слот; пароль — `SYS_MONITOR_PASSWORD` в `.env`). Кастомные запросы —
`infrastructure/prometheus/postgres-exporter-queries.yaml`.

```bash
# Запуск (после первого up'а стенда поднимаются сами):
docker compose up -d --no-deps postgres-exporter kafka-exporter cadvisor
docker compose restart prometheus   # правка prometheus.yml: bind-mount держит старый inode

# Быстрые проверки из Prometheus:
curl -s -G localhost:9090/api/v1/query --data-urlencode 'query=pg_up' | jq .data.result[0].value
curl -s -G localhost:9090/api/v1/query --data-urlencode 'query=kafka_brokers' | jq .data.result[0].value
curl -s -G localhost:9090/api/v1/query --data-urlencode 'query=pg_slot_wal_retained_bytes{slot_name="outbox_slot"}' | jq .data.result[0].value
```

### Сохранение результатов тестов (requirements.md §13.4)

После каждого теста — JSON с итогами и CSV с временными рядами; счётчики
берутся из фактических данных (API Writer/CDC + Kafka consume по `runId` +
слот из БД), а не из Prometheus (scrape теряет кратковременные события):

```bash
# Сбор итогов последнего прогона (arm определяется по запущенному CDC):
./scripts/reports/collect-results.sh <runId> --notes "что происходило в тесте"
# → results/raw/<ts>-<runId>/result.json (нагрузка, счётчики, kafka
#   unique/duplicates/missing, отказы, recovery, версии, срез docker stats)
#   + timeseries.csv (19 рядов: writer/cdc/pg/kafka/VM с шагом 5с по окну прогона)

# Per-container CPU/RAM во время теста (фон, Ctrl+C — стоп):
./scripts/reports/sample-docker-stats.sh results/raw/<имя>/docker-stats.csv 5
```

Артефакты лежат в `results/raw/` (в .gitignore). Per-container CPU/RAM —
сэмплером, потому что cAdvisor на Docker Desktop (containerd image store) не
видит контейнеры — подробно в docs/deviations.md §7.

## Полезные команды

Пароли в примерах (`postgres`, `writer_secret`, …) — значения по умолчанию
из `.env.example`; при смене подставляйте свои.

```bash
# Состояние Patroni-кластера (кто leader / sync_standby)
curl -s localhost:8008/cluster | python3 -m json.tool

# PostgreSQL через PgBouncer под прикладной ролью (прикладная БД — outbox)
docker compose exec -e PGPASSWORD=writer_secret pg1 \
  psql "host=pgbouncer port=6432 user=sys_writer dbname=outbox"

# Записать сообщение в outbox руками тем же паттерном, что Writer
# (INSERT+DELETE в одной транзакции: таблица остаётся пустой, изменение уходит в WAL;
# без RETURNING — оно требует SELECT-привилегию, у sys_writer только колоночный SELECT(id))
docker compose exec -e PGPASSWORD=writer_secret pg1 psql "host=pgbouncer port=6432 user=sys_writer dbname=outbox" -c "
BEGIN;
INSERT INTO public.outbox_messages (id, topic, body) VALUES ('00000000-0000-7000-8000-000000000c0d', 'outbox.events', '{\"eventType\":\"TestEvent\"}');
DELETE FROM public.outbox_messages WHERE id = '00000000-0000-7000-8000-000000000c0d';
COMMIT;"

# Посмотреть содержимое слота, не потребляя (бинарный pgoutput: B/R/I/C записи).
# Через haproxy — на актуальный primary (peek/get работают только на primary).
# ВНИМАНИЕ: при запущенном CDC (polling/streaming) конкурентный peek даст 55006 (слот занят CDC)
docker compose exec -e PGPASSWORD=postgres pg1 psql -h haproxy -U postgres -d outbox \
  -c "SELECT lsn, xid FROM pg_logical_slot_peek_binary_changes('outbox_slot', NULL, 20, 'proto_version','1','publication_names','outbox_publication')"

# То же от минимальной роли sys_app через защищённую функцию (счётчик записей)
docker compose exec -e PGPASSWORD=app_secret pg1 \
  psql "host=pgbouncer port=6432 user=sys_app dbname=outbox" \
  -c "SELECT count(*) FROM cdc.peek_outbox_changes(100)"

# Диагностика слота для CDC (wal_status/confirmed_flush_lsn/текущий WAL LSN)
docker compose exec -e PGPASSWORD=app_secret pg1 \
  psql "host=pgbouncer port=6432 user=sys_app dbname=outbox" \
  -Atc "SELECT cdc.outbox_slot_status()"

# Продвинуть слот (get вместо peek потребляет и двигает LSN/xmin).
# Нужен перед failover/switchover-экспериментами, пока CDC остановлен: непотребляемый
# слот перестаёт синхронизироваться на стендбаях и не переживает promotion —
# документированное поведение PG17, см. docs/deviations.md раздел 3.
# При работающем CDC это делает сам сервис (advance после Kafka delivery)
docker compose exec -e PGPASSWORD=postgres pg1 psql -h haproxy -U postgres -d outbox \
  -Atc "SELECT count(*) FROM pg_logical_slot_get_binary_changes('outbox_slot', NULL, NULL, 'proto_version','1','publication_names','outbox_publication')"

# Состояние слотов на всех нодах (failover sync)
for n in pg1 pg2 pg3; do docker compose exec -e PGPASSWORD=postgres $n \
  psql -h localhost -U postgres -d outbox -Atc \
  "SELECT '$n '||plugin||' synced='||synced FROM pg_replication_slots WHERE slot_name='outbox_slot'"; done

# Kafka: описание topic
docker compose exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic outbox.events

# Prometheus: таргеты и мгновенные метрики (§19 «просмотр Prometheus»)
curl -s localhost:9090/api/v1/targets | python3 -c 'import json,sys; [print(t["labels"]["job"], t["health"]) for t in json.load(sys.stdin)["data"]["activeTargets"]]'

# Сохранение результатов теста (§19 «сохранение результатов»)
./scripts/reports/collect-results.sh <runId> --notes "описание теста"

# Повторная идемпотентная инициализация (роли/БД/publication/слот/userlist)
./scripts/init/apply-bootstrap.sh

# Полный сброс стенда (только ресурсы этого проекта)
./scripts/reset/wipe.sh
```

## Структура

```
docker-compose.yml              — вся инфраструктура + Writer + CDC (profiles polling/streaming)
streaming-polling-outbox.sln    — .NET-решение (Writer, CdcCommon, CdcPolling, CdcStreaming + тесты)
.env.example                    — шаблон переменных окружения (cp .env.example .env)
AGENTS.md                       — инструкция для агентов (ограничения, грабли)
src/Writer/                     — Writer: ASP.NET Core 8, net8.0 (Minimal API + генератор)
src/CdcCommon/                  — общая библиотека CDC: LSN, парсер бинарного pgoutput, конверт, backoff
src/CdcPolling/                 — CDC Polling: peek → pgoutput → Kafka (KafkaFlow) → advance
src/CdcStreaming/               — CDC Streaming: Npgsql PgOutput → Kafka (KafkaFlow) → replication feedback
tests/Writer.UnitTests/         — xunit-тесты критичной логики Writer
tests/CdcPolling.UnitTests/     — xunit-тесты: golden-байты pgoutput, LSN, конверт, backoff, опции
tests/CdcStreaming.UnitTests/   — xunit-тесты: карта колонок/сборка события, LSN-конверсия, опции
infrastructure/
  patroni/                      — Dockerfile (postgres:17.2 + patroni 4.1.5) и patroni.yml
  postgres/init/                — идемпотентный SQL-bootstrap (роли, outbox-таблица, publication,
                                   слот, функции CDC Polling, гранты CDC Streaming, роль мониторинга)
  haproxy/ pgbouncer/ prometheus/  (+ postgres-exporter-queries.yaml — кастомные запросы §13.3)
scripts/
  check-infra.sh                — полная проверка стенда (exit 0 = всё зелёное)
  test/smoke-writer.sh          — живая проверка Writer (прогон/БД/метрики/direct/рестарт)
  test/smoke-cdc-polling.sh     — живая проверка CDC Polling (доставка/LSN/метрики/рестарт/batch size)
  test/smoke-cdc-streaming.sh   — живая проверка CDC Streaming (доставка/feedback/метрики/рестарт/batch size)
  init/apply-bootstrap.sh       — повторный запуск инициализации БД
  reset/wipe.sh                 — полный сброс (down -v)
  reports/collect-results.sh    — сбор результатов теста: result.json + timeseries.csv (§13.4)
  reports/sample-docker-stats.sh — сэмплер per-container CPU/RAM (docker stats → CSV)
results/raw/                    — артефакты прогонов (gitignored)
docs/
  deviations.md                 — отклонения от ТЗ и их причины
```

## Что дальше (по плану requirements.md)

Далее: KafkaValidator и integration-тесты (§15), HA-тесты TEST-HA-001..008
(в т.ч. TEST-HA-004 — убийство walsender для стриминга) и WAL Pressure Test
(§12.2 — метрика `pg_slot_safe_wal_size_bytes` под него уже готова),
benchmark-циклы A/B (полный wipe между ними, requirements.md 11) и
сравнительный отчёт. Отложено владельцем на поздний этап: Schema Registry +
Avro-тело сообщения (сейчас body — JSON `jsonb`).
