# PostgreSQL 17 Logical Outbox CDC Benchmark — исследовательский стенд

Сравнение двух способов доставки outbox-сообщений из PostgreSQL 17 в Apache
Kafka: **SQL Polling** (`pg_logical_slot_peek_binary_changes()` + advance)
против **Streaming Replication** (Npgsql Logical Replication API, `PgOutput`).
Writer делает `INSERT` в outbox-таблицу; CDC читает изменения через logical
slot (SELECT-polling таблицы не используется). Причина отказа от
`pg_logical_emit_message`: [docs/deviations.md](docs/deviations.md).

Требования: [requirements.md](requirements.md). Отклонения, обнаруженные
практикой (в т.ч. замена pgoutput → test_decoding): [docs/deviations.md](docs/deviations.md).

Состав инфраструктуры (этап 1, готов):

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
  (минимальные привилегии), БД, пустая publication `outbox_publication`, слот

## Быстрый старт

```bash
cp .env.example .env            # при необходимости поменяйте пароли
docker compose up -d --build    # сборка Patroni-образа + запуск стенда
./scripts/check-infra.sh        # полная проверка (все проверки должны быть ✔)
```

Хост-порты: HAProxy (primary) — **5432**, PgBouncer — 6432, Patroni REST —
8008/8009/8010, Kafka — 9092-9094, Prometheus — 9090, HAProxy stats — 8404.

## Полезные команды

Пароли в примерах (`postgres`, `writer_secret`, …) — значения по умолчанию
из `.env.example`; при смене подставляйте свои.

```bash
# Состояние Patroni-кластера (кто leader / sync_standby)
curl -s localhost:8008/cluster | python3 -m json.tool

# PostgreSQL через PgBouncer под прикладной ролью
docker compose exec -e PGPASSWORD=writer_secret pg1 \
  psql "host=pgbouncer port=6432 user=sys_writer dbname=postgres"

# Записать сообщение в outbox (как это будет делать Writer)
docker compose exec -e PGPASSWORD=writer_secret pg1 psql "host=pgbouncer port=6432 user=sys_writer dbname=postgres" \
  -c "INSERT INTO public.outbox_messages (id, topic, body) VALUES (gen_random_uuid(), 'outbox.events', '{\"eventType\":\"TestEvent\"}') RETURNING id"

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
docker-compose.yml              — вся инфраструктура
.env.example                    — шаблон переменных окружения (cp .env.example .env)
AGENTS.md                       — инструкция для agентов (ограничения, грабли)
infrastructure/
  patroni/                      — Dockerfile (postgres:17.2 + patroni 4.1.5) и patroni.yml
  postgres/init/                — идемпотентный SQL-bootstrap (роли, outbox-таблица, publication, слот)
  haproxy/ pgbouncer/ prometheus/
scripts/
  check-infra.sh                — полная проверка стенда (exit 0 = всё зелёное)
  init/apply-bootstrap.sh       — повторный запуск инициализации БД
  reset/wipe.sh                 — полный сброс (down -v)
docs/
  deviations.md                 — отклонения от ТЗ и их причины
```

## Что дальше (по плану requirements.md)

Этапы 3+: Writer (ASP.NET Core 8, `sys_writer`, INSERT в `outbox_messages`, через PgBouncer), CDC Polling (`sys_app` + SECURITY DEFINER + разбор бинарного pgoutput + KafkaFlow), CDC Streaming (`sys_replication`, Npgsql PgOutput), метрики OpenTelemetry, HA-тесты TEST-HA-001..008, benchmark и сравнительный отчёт.
