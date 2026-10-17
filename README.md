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
CDC Polling HTTP API — **18081**, CDC Streaming HTTP API — **18082**, KafkaValidator
HTTP API — **18083**, Patroni REST — 8008/8009/8010, Kafka — 9092-9094, Prometheus —
9090, HAProxy stats — 8404.

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
| Logical slot state | postgres-exporter | `pg_slot_active{slot_name}`, `pg_slot_wal_retained_bytes` (WAL retention), `pg_slot_flush_lag_bytes`, labels `wal_status`/`plugin`; `pg_slot_safe_wal_size_bytes` — пустая при `max_slot_wal_keep_size=-1` (появится, только если лимит включат) |
| WAL retention | postgres-exporter | `pg_wal_position_bytes`, `pg_wal_dir_bytes`/`pg_wal_dir_files`, `pg_slot_wal_retained_bytes` |
| Kafka broker availability | kafka-exporter | `kafka_brokers` (=3, когда все живы) |
| Kafka topic offsets | kafka-exporter | `kafka_topic_partition_current_offset{topic,partition}` |
| Kafka consumer lag (если применимо) | kafka-exporter | `kafka_consumergroup_lag` — применимо только к группам, коммитящим оффсеты; группа kafka-validator сознательно не коммитит (полная перемотка topic при каждом старте), поэтому её lag не возникает — см. deviations §8 и раздел «KafkaValidator» ниже |
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

## KafkaValidator и integration-тесты (этап 7, requirements.md §15)

[KafkaValidator](src/KafkaValidator) — отдельный validation tool (§15):
«живой индекс» содержимого topic `outbox.events`. KafkaFlow-консьюмер в группе
`kafka-validator` с `auto.offset.reset=earliest` и **без сохранения оффсетов**
(`WithoutStoringOffsets` → NullOffsetCommitter/NullOffsetManager): каждый
(ре)старт перематывает topic с начала и полностью восстанавливает счётчики всех
прогонов. Это ключевая корректность: закоммиченные оффсеты после рестарта
«съели» бы историю — и валидатор ложно сообщал бы о потерях. Побочный эффект:
`kafka_consumergroup_lag` для группы не возникает (нет закоммиченных оффсетов),
§13.3 «если применимо» — docs/deviations.md §8.

| Требование §15 | Где смотреть |
|---|---|
| Общее количество Kafka records | `kafka.records` вердикта / `GET /api/validation/runs/{runId}` |
| Количество уникальных `id` | `kafka.uniqueIds` |
| Количество дубликатов | `kafka.duplicateRecords` (= records − uniqueIds; at-least-once, допустимо) |
| Количество отсутствующих `id` | `missingWrittenIdsCount` — **доказанная потеря CDC** (Writer доказал commit, записи нет) |
| Неопределённые коммиты Writer — отдельно | `ambiguousDeliveredCount` / `ambiguousNotDeliveredCount` (не потеря: исход commit не определён) |
| Записано Writer | `expected.writtenCount` — из `GET /api/generation/results/ids` (fetchFromWriter) или явным набором id |
| Подтверждено CDC | `cdc.messagesDelivered` + `cdc.confirmedLsn` — снапшот `/api/cdc/state` отвечающего arm в вердикте |
| Уникальный идентификатор прогона в body | `body.runId` (кладёт Writer); ожидаемый набор id сохраняется `/api/generation/results/ids` |
| Не полагаться только на Prometheus | все счётчики — из фактических записей topic; метрики без runId-label (§13.3) |

Вердикт: `pass` ⇔ нет доказанных потерь (`missingWrittenIdsCount=0`) и нет
неожидаемых лишних id (`unexpectedExtraIdsCount=0`); дубликаты вердикт не фейлят.

```bash
# Запуск на живом стенде (always-on сервис, паролей не требует):
docker compose build kafka-validator && docker compose up -d --no-deps kafka-validator
docker compose restart prometheus   # новый scrape-job: bind-mount держит старый inode

V=http://localhost:18083
curl -s $V/api/validation/state                    # состояние, счётчики потребления
curl -s $V/api/validation/runs | python3 -m json.tool   # все прогоны topic (runId → records/unique/dupes)
# Вердикт последнего прогона Writer (набор id берётся у Writer автоматически):
curl -s -X POST $V/api/validation/validate -H 'Content-Type: application/json' \
  -d '{"runId":"<runId>","fetchFromWriter":true}' | python3 -m json.tool
# Вердикт старого прогона — явным сохранённым набором id:
curl -s -X POST $V/api/validation/validate -H 'Content-Type: application/json' \
  -d "{\"runId\":\"$RID\",\"writtenIds\":$(curl -s localhost:18080/api/generation/results/ids | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["writtenIds"]))')}"
```

Сброс состояния — только рестарт контейнера (`docker compose restart
kafka-validator`): перемотка пересчитает всё заново (проверяется тестом T3).

### Integration-тесты (этап 7, §20: «запись, чтение, доставку, количество, дубликаты»)

Живой харнесс `scripts/test/integration-delivery.sh` (конвенция smoke-скриптов;
standalone-раннер .NET-тестов не разворачивается — локальный dotnet v10,
тесты ходят в sdk-контейнере вне сети стенда, deviations §8). Секции:
T1 запись 300×256 через PgBouncer → T2 доставка/сверка вердиктом валидатора
(fetchFromWriter + снапшот CDC) → T3 рестарт валидатора = полная перемотка →
T4 запись direct (мимо PgBouncer) → T5 rate-limited 200@50/с + артефакты
§13.4 (`collect-results.sh` + `validation-verdict.json` рядом с result.json).

```bash
./scripts/test/integration-delivery.sh                 # тестирует запущенный CDC-arm
./scripts/test/integration-delivery.sh --arm polling   # сам переключит стенд (stop streaming → up polling)
```

Контрактные тесты формата (Writer → конверт → валидатор, без стенда):
`tests/Integration/` — `dotnet test` (в sdk-контейнере, как остальные).

## HA-тесты (этап 8, requirements.md §14)

Автоматизированные сценарии отказоустойчивости для обеих CDC-арм. После каждой
инъекции — фактическая сверка содержимого Kafka вердиктом валидатора (§15:
missing=0, unexpected=0 — обязательные; дубликаты допустимы, at-least-once §10),
артефакты §13.4 + `ha-report.json` (инъекция, recovery time, ветка исхода,
счётчики движка) в каталог прогона.

| Тест | Инъекция | Ключевые проверки |
|---|---|---|
| HA-001 Failover | `docker compose kill <leader>` под нагрузкой 1200@30/с | выборы лидера, переподключение CDC, written==unique, missing=0, recovery |
| HA-002 Switchover | `POST /switchover` (Patroni REST, basic-auth) | готовность standby + synced-слот ДО, отсутствие пропусков, recovery |
| HA-003 Kafka outage | `stop kafka1-3`, запись 600@10/с | writer пишет, `confirmed_flush_lsn` заморожен, удерживаемый WAL растёт, автопродолжение |
| HA-004 WAL Sender Failure | `kill -9` pid walsender'а (`application_name='cdc-streaming'`) / SQL-сессий (`usename='sys_app'`) | readErrors вырос (стриминг), новый walsender/сессии, missing=0 |
| HA-005 Restart CDC | рестарт контейнера: R1 до delivery (Kafka стоит), R2/R3 на +15/+20с прогона | перечитка с confirmed LSN, дубликаты считаются, missing=0 |
| HA-006 Partial Batch Failure | `kill kafka3` в момент публикации | недоставленное переотправлено, missing=0 |
| HA-007 Broker Failure | `stop kafka3` (graceful) под нагрузкой | доставка не прерывается надолго (RF=3, min.insync.replicas=2) |
| HA-008 Combined | Kafka стоит + `kill <leader>` | две честные ветки: слот выжил (pass) / потерян (`error_slot`, missing=N — задокументированная невосстановимая потеря, §14) |

WAL Pressure Test (§12.2) исключён решением владельца — не детерминируется
разумными средствами (см. deviations §9п).

```bash
./scripts/test/ha-suite.sh --arm streaming            # HA-001..008 по очереди
./scripts/test/ha-suite.sh --arm polling --tests ha-003,ha-005
```

Важные механики харнесса (подробности — deviations §9):

- **`synchronized_standby_slots` обновляется сразу после смены лидера** —
  без этого PG17 держит логический декодинг (обе армы) в ожидании flush от
  умершего standby, и доставка молча висит в `catchup` (главная грабля этапа).
- **Graceful switchover при стриминговом CDC виснет, пока жив консюмер слота**:
  fast shutdown ждёт логический walsender, а тот — отключения клиента; харнесс
  после 60с рестартует cdc-streaming как операторское лечение (polling не
  держит соединений — дедлок невозможен; crash-failover не затронут).
- **haproxy перечитывает DNS pg-нод динамически** (`resolvers docker` в
  haproxy.cfg): IP нод меняются после kill/start циклов, статический резолв
  молча уводил все бекенды в Layer4-timeout DOWN (открытие этапа 8,
  deviations §9о). Смена конфига — `docker compose restart haproxy`.
- **Полный outage Kafka переводит все KafkaFlow-сервисы (CDC + валидатор) в
  рестарт-циклы** — нативный SIGSEGV (exit 139) стека KafkaFlow 4.2.0,
  воспроизведён и изолирован (deviations §9н); гарантии §12.1/§14 при этом
  проверяемы по слоту PG (confirmed_flush заморожен) и вердиктом после
  восстановления. Graceful stop одного брокера: доставка не прерывается,
  типичный простой ~20с (перевыборы лидера партий, HA-007).
- **`docker compose kill` не перезапускает контейнер** (API-kill = ручная
  остановка для рестарт-политики) — харнесс возвращает ноду `compose start`'ом
  после выборов.
- **Recovery time** = время выборов лидера (REST `/cluster`) + собственная
  метрика движка `lastRecoveryMs` (waiting→running); для HA-003 — внешний
  замер от старта Kafka до первого роста messagesDelivered.
- Харнесс переставляет ПРОФИЛИ arm сам (`stop` другого CDC, `up -d --no-deps`
  своего); полный матричный прогон — два вызова (по одному на arm).
- Каждый тест завершается `reconcile`: apply-bootstrap (02-slot/03-reconcile),
  рестарт CDC при `error_slot`, ожидание synced-копии, canary-прогон.

## Benchmark-циклы A/B (этап 9, requirements.md §11)

Изолированное сравнение арм: цикл A (polling) и цикл B (streaming) на
**полностью пересозданном** стенде (`wipe.sh` → `up -d --build` под нужным
профилем), одинаковые сценарии нагрузки, warmup-прогон вне отчёта (JIT, пулы,
прогрев валидатора). После цикла A стенд вытирается (§11 шаг 7), после
цикла B остаётся поднятым (arm streaming).

```bash
./scripts/test/benchmark.sh --cycle all     # A(polling) → wipe → B(streaming) + сводка
./scripts/test/benchmark.sh --cycle polling # только цикл A (завершается wipe)
./scripts/test/benchmark.sh --cycle streaming --only s1,s5   # подмножество сценариев
```

| Сценарий | Нагрузка | Что измеряет |
|---|---|---|
| s1-steady-50 | 2000@50/с, 256Б | латентность при равномерном потоке (e2e p50/p95/p99) |
| s2-steady-200 | 3000@200/с, 256Б | поведение при 4-кратном ускорении потока |
| s3-burst | 2000 без rate-limit | реакция на всплеск: drain, дубликаты, пик скорости CDC |
| s4-large-4k | 1000@50/с, 4096Б | декодинг/публикация крупных тел |
| s5-throughput | 8000 без rate-limit | максимальная пропускная способность конвейера |

Метрики на сценарий (§13.4 — точные значения из фактических данных, не scrape):
Writer (длительность, фактический msg/s, p50/p95/p99 commit и batch), полный
цикл доставки `deliveryTotalMs` и `drainMs` (Writer finished → всё подтверждено
валидатором), скорость CDC по сэмплам 1с (avg/peak msg/s), e2e-латентность CDC
(rolling-процентили + точный средний за сценарий по дельтам гистограммы
Prometheus), дельты счётчиков (errors/retries/reconnects), вердикт валидатора
(missing=0, unexpected=0 обязательны; дубликаты считаются), CPU/RAM по ролям
(cdc/postgresql/kafka) из docker-stats. Итог цикла — `benchmark-report.json`.

Артефакты: `results/raw/<stamp>-benchmark-<arm>/` (полные: отчёт, docker-stats,
runs/ с result.json+timeseries.csv+validation-verdict.json, cdc-samples/),
компактные копии для сравнения — `results/benchmark/benchmark-{polling,streaming}.json`
и `results/benchmark/summary.json` (коммитятся). Методология и ограничения —
deviations §10.

Важно: `wipe.sh` опускает ОБА профиля CDC (`--profile polling --profile
streaming down -v`) — без профилей compose не трогает сервисы с профилем,
контейнер CDC переживает wipe и после нового up занимает слот нового кластера
(грабли этапа 9).

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

# KafkaValidator: проверка доставки последнего прогона (requirements.md 15):
# записи/уникальные/дубликаты/потери + снапшот CDC; runId — из ответа старта Writer
curl -s -X POST http://localhost:18083/api/validation/validate -H 'Content-Type: application/json' \
  -d '{"runId":"<runId>","fetchFromWriter":true}' | python3 -m json.tool

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
src/KafkaValidator/             — KafkaValidator: consumer-индекс topic + вердикты доставки (§15)
tests/Writer.UnitTests/         — xunit-тесты критичной логики Writer
tests/CdcPolling.UnitTests/     — xunit-тесты: golden-байты pgoutput, LSN, конверт, backoff, опции
tests/CdcStreaming.UnitTests/   — xunit-тесты: карта колонок/сборка события, LSN-конверсия, опции
tests/KafkaValidator.UnitTests/ — xunit-тесты: вердикт (потери/дубликаты/ambiguous), реестр прогонов, опции
tests/Integration/              — контрактные тесты формата Writer → конверт → валидатор (§18)
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
  test/integration-delivery.sh  — integration-тесты этапа 7: запись/чтение/доставка/количество/дубликаты (§15)
  test/ha-common.sh             — разделяемая библиотека HA-харнессов этапа 8 (source, не запускается)
  test/ha-suite.sh              — HA-тесты TEST-HA-001..008 для обеих арм (§14)
  test/benchmark.sh             — benchmark-циклы A/B этапа 9 (§11): wipe → сценарии → отчёты
  init/apply-bootstrap.sh       — повторный запуск инициализации БД
  reset/wipe.sh                 — полный сброс (down -v, ОБА профиля CDC — иначе армы переживают wipe)
  reports/collect-results.sh    — сбор результатов теста: result.json + timeseries.csv (§13.4)
  reports/sample-docker-stats.sh — сэмплер per-container CPU/RAM (docker stats → CSV)
results/raw/                    — артефакты прогонов (gitignored)
results/benchmark/              — компактные benchmark-отчёты A/B + summary.json (коммитятся)
docs/
  deviations.md                 — отклонения от ТЗ и их причины
```

## Что дальше (по плану requirements.md)

Далее: сравнительный отчёт (этап 10) — сводные таблицы/выводы по данным
`results/benchmark/` (этапы 1–9 выполнены: benchmark-циклы A/B —
`scripts/test/benchmark.sh`, WAL Pressure Test исключён решением владельца,
deviations §9п). Отложено владельцем на поздний этап: Schema Registry +
Avro-тело сообщения (сейчас body — JSON `jsonb`).
