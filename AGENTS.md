# AGENTS.md

Исследовательский стенд: сравнение двух способов доставки outbox-сообщений из PostgreSQL 17 в Kafka — **SQL Polling** (`pg_logical_slot_peek_binary_changes()` + advance) vs **Npgsql Streaming Replication** (`PgOutput`). Доставка — logical replication через слот; SELECT-polling таблицы запрещён. Git-репозиторий, ветка `main`.

## Документы (читать перед работой)

- `requirements.md` — **единый авторитетный spec** (объединяет `prd.md` и `prd-2.md`; правила prd-2 по WAL retention имеют приоритет). Все детали: роли, HA-тесты TEST-HA-001…008, метрики, план этапов 1–10, критерии готовности, риски RISK-001…010.
- `prd.md`, `prd-2.md` — исходные PRD; использовать только как справку, при расхождениях главенствует `requirements.md`.
- `docs/deviations.md` — отклонения от ТЗ, обнаруженные практикой (в т.ч. pgoutput → test_decoding).
- `README.md` — быстрый старт и полезные команды.

## Текущее состояние

**Этапы 1-2 (инфраструктура) выполнены и проверены** (`scripts/check-infra.sh` — все проверки зелёные): Patroni 4.1.5 + PostgreSQL 17.2 ×3 (синхронная репликация, failover-слот синхронизируется), etcd ×3, HAProxy, PgBouncer, Kafka KRaft 3.9.0 ×3 (topic `outbox.events`), Prometheus, идемпотентная инициализация (роли, outbox-таблица `public.outbox_messages` в publication, pgoutput-слот).

**Этап 3 (Writer) выполнен и проверен** (`scripts/test/smoke-writer.sh` — 24/24, `tests/Writer.UnitTests` — 27/27): ASP.NET Core 8 `src/Writer`, INSERT-батчи от `sys_writer` (PgBouncer по умолчанию / direct через HAProxy), управление HTTP API `:18080` (`/api/generation/start|stop|state|results|results/ids`, `/health`, `/ready`, `/metrics` OTel), старт/рестарт в Idle, неоднозначный commit разрешается идемпотентным ретраем (PK-conflict = прошлый commit прошёл). Полная целевая структура репозитория — `requirements.md` раздел 18. Отложено владельцем на поздний этап: Schema Registry + Avro-тело (сейчас body — JSON). Далее: этапы 4+ (CDC Polling/Streaming).

## Критические жёсткие ограничения (requirements.md, раздел 2)

Нарушение любого из них ломает цель исследования:

- Механика CDC — **outbox-таблица + pgoutput** (финальное решение владельца; история и причины — docs/deviations.md раздел 1: pgoutput не передаёт logical messages ни в одной версии PostgreSQL — связка из ТЗ доказанно неработоспособна; test_decoding отклонён как не-production). Запрет ТЗ на outbox-таблицу снят решением владельца, но SELECT-polling таблицы по-прежнему запрещён: чтение только через logical slot (peek/advance и стриминг).
- Никакой outbox-таблицы и никакого polling таблицы вместо logical replication.
- KafkaFlow — основной API публикации (Confluent.Kafka допустим только как внутренняя зависимость).
- .NET 8 строго (`net8.0`), PostgreSQL строго 17, всё в Docker Compose.
- Никакого SUPERUSER для прикладных ролей; LSN подтверждается только после Kafka delivery (at-least-once, дубликаты допустимы, потери нет).
- Не использовать Patroni permanent replication slots для logical slots — failover slots синхронизирует сам PostgreSQL 17.
- Не скрывать потери WAL / ошибки; не подменять реальные результаты тестов предполагаемыми цифрами.

## Ключевые технические грабли

- **Остатки от предыдущих прогонов/раскладок НЕ чистятся кодом инициализации.** Init-скрипты только создают объекты идемпотентно (CREATE IF NOT EXISTS). Если есть подозрение, что в стенде что-то осталось от прошлого прогона (чужая БД, слот с другим плагином, лишние роли) — не дописывать миграции в init, а полностью очистить стенд: `./scripts/reset/wipe.sh` (docker compose down -v) и поднять заново.
- `max_slot_wal_keep_size` считается от **фактически измеренного объёма WAL** (разница LSN), НЕ от размера payload; в нормальных тестах `-1`, лимит только в отдельном WAL Pressure Test.
- **pgoutput не эмитит logical messages** (ни в одной версии PG, проверено на 17.2/18.4) — поэтому доставка идёт через INSERT в `public.outbox_messages` (publication `outbox_publication` содержит эту таблицу: pgoutput эмитит только опубликованные таблицы). Слот `outbox_slot`: плагин `pgoutput`.
- Одна вставленная строка в pgoutput = несколько протокольных записей (Relation + Insert, плюс B/C-обрамление транзакции): не считать каждую бинарную запись отдельным событием (requirements.md 7.1 — теперь это буквально применимо).
- Жизненный цикл строк outbox (очистка после публикации) не определён до этапа 4; в тестовых циклах строки накапливаются, между циклами — wipe.
- Прикладная БД — **`postgres`** (не отдельная `outbox`): PG17 slotsync worker синхронизирует слоты только БД из `primary_conninfo`, а Patroni хардкодит там `dbname=postgres`.
- Кластерные параметры (`wal_level` и др. из `CMDLINE_OPTIONS` Patroni) применяются только через `bootstrap.dcs.postgresql.parameters` (DCS); member-local `postgresql.parameters` для них игнорируются — иначе standby падает с FATAL slotsync worker'а.
- Publication не фильтрует logical messages — не считать её границей доступа.
- Polling читает через `peek` (не `get_changes` — тот потребляет до подтверждения Kafka); advance — только через SECURITY DEFINER-функции с зафиксированным слотом.
- Streaming подключается к PostgreSQL напрямую, минуя PgBouncer; Writer — по умолчанию через PgBouncer (`pool_mode = transaction`).
- Разделять понятия: внутренний batching Kafka producer ≠ прикладной batch ≠ подтверждение LSN.
- Инвалидированный слот ≠ пересоздать молча: явная ошибка с диагностикой.
- После рестарта стенда на репликах остаются несинхронизированные остатки слота (slotsync worker падает с "same name slot exists") — их роняет 03-slot-reconcile.sql из init-db.
- `synchronized_standby_slots` принимает только ФИЗИЧЕСКИЕ слоты; пишется через ALTER SYSTEM локально — после смены primary перезапустить apply-bootstrap.sh.
- PgBouncer: userlist с plaintext-паролями генерируется init-db из .env (SCRAM pass-through с верификаторами из pg_authid на этой сборке не работает); хост-порт HAProxy 5432 (занявший его раньше airo-postgres остановлен вручную, policy unless-stopped).
- **Writer (этап 3)**: UUID (v7, свой `MessageFactory.CreateUuidV7` — `Guid.CreateVersion7` только в .NET 9; поля Guid-span little-endian, version-нибл в байте 7) и `created_at` задаёт клиент — у `sys_writer` только INSERT, `RETURNING` требует SELECT. Один батч = один multi-row INSERT в одной транзакции; ретрай тех же UUID после сбоя: `unique_violation` по PK на повторе = «прошлый commit прошёл» (так разрешается неоднозначный commit 6.3, дублей в outbox нет). У источника PgBouncer `No Reset On Close=true`. Мягкий stop прерывает только ожидание между ретраями — начавшийся commit доигрывается; жёсткая отмена (shutdown хоста) оставляет батч «неоднозначным». Сборка/тесты .NET — в контейнере `mcr.microsoft.com/dotnet/sdk:8.0` (локальный dotnet — 10-й); хост-порт Writer 18080 (8080 занят контейнером airo-host другого проекта).

## Роли PostgreSQL

`sys_writer` (запись, NOREPLICATION), `sys_app` (polling через SECURITY DEFINER-функции), `sys_replication` (REPLICATION, streaming), `postgres` (админ). Пароли через env, не в коде.
