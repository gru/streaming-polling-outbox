# AGENTS.md

Исследовательский стенд: сравнение двух способов доставки transactional logical messages из PostgreSQL 17 в Kafka — **SQL Polling** (`pg_logical_slot_peek_binary_changes()`) vs **Npgsql Streaming Replication** (`pgoutput`). НЕ git-репозиторий.

## Документы (читать перед работой)

- `requirements.md` — **единый авторитетный spec** (объединяет `prd.md` и `prd-2.md`; правила prd-2 по WAL retention имеют приоритет). Все детали: роли, HA-тесты TEST-HA-001…008, метрики, план этапов 1–10, критерии готовности, риски RISK-001…010.
- `prd.md`, `prd-2.md` — исходные PRD; использовать только как справку, при расхождениях главенствует `requirements.md`.

## Текущее состояние

Кода нет — только требования. Целевая структура репозитория описана в `requirements.md` раздел 18 (`src/Writer`, `src/Cdc.Polling`, `src/Cdc.Streaming`, `src/Shared`, `src/KafkaValidator`, `infrastructure/`, `scripts/`, `tests/`, `results/`, `docs/`).

## Критические жёсткие ограничения (requirements.md, раздел 2)

Нарушение любого из них ломает цель исследования:

- Плагин CDC-слота — **test_decoding, а не pgoutput** (отклонение согласовано с владельцем: pgoutput не передаёт logical messages НИ В ОДНОЙ версии PostgreSQL — проверено на 17.2/18.4 и по release notes; детали docs/deviations.md). Архитектура ТЗ сохранена: transactional messages через `pg_logical_emit_message`, без outbox-таблицы.
- Никакой outbox-таблицы и никакого polling таблицы вместо logical replication.
- KafkaFlow — основной API публикации (Confluent.Kafka допустим только как внутренняя зависимость).
- .NET 8 строго (`net8.0`), PostgreSQL строго 17, всё в Docker Compose.
- Никакого SUPERUSER для прикладных ролей; LSN подтверждается только после Kafka delivery (at-least-once, дубликаты допустимы, потери нет).
- Не использовать Patroni permanent replication slots для logical slots — failover slots синхронизирует сам PostgreSQL 17.
- Не скрывать потери WAL / ошибки; не подменять реальные результаты тестов предполагаемыми цифрами.

## Ключевые технические грабли

- `max_slot_wal_keep_size` считается от **фактически измеренного объёма WAL** (разница LSN), НЕ от размера payload; в нормальных тестах `-1`, лимит только в отдельном WAL Pressure Test.
- **pgoutput не эмитит logical messages** (ни в одной версии PG) — слот `outbox_slot` на плагине `test_decoding`; Polling парсит текст `pg_logical_slot_peek_changes`, Streaming использует generic Npgsql API, не `PgOutputReplicationSlot`.
- Прикладная БД — **`postgres`** (не отдельная `outbox`): PG17 slotsync worker синхронизирует слоты только БД из `primary_conninfo`, а Patroni хардкодит там `dbname=postgres`.
- Кластерные параметры (`wal_level` и др. из `CMDLINE_OPTIONS` Patroni) применяются только через `bootstrap.dcs.postgresql.parameters` (DCS); member-local `postgresql.parameters` для них игнорируются — иначе standby падает с FATAL slotsync worker'а.
- Publication не фильтрует logical messages — не считать её границей доступа.
- Polling читает через `peek` (не `get_changes` — тот потребляет до подтверждения Kafka); advance — только через SECURITY DEFINER-функции с зафиксированным слотом.
- Streaming подключается к PostgreSQL напрямую, минуя PgBouncer; Writer — по умолчанию через PgBouncer (`pool_mode = transaction`).
- Разделять понятия: внутренний batching Kafka producer ≠ прикладной batch ≠ подтверждение LSN.
- Инвалидированный слот ≠ пересоздать молча: явная ошибка с диагностикой.
- После рестарта стенда на репликах остаются несинхронизированные остатки слота (slotsync worker падает с "same name slot exists") — их роняет 03-slot-reconcile.sql из init-db.
- `synchronized_standby_slots` принимает только ФИЗИЧЕСКИЕ слоты; пишется через ALTER SYSTEM локально — после смены primary перезапустить apply-bootstrap.sh.
- PgBouncer: userlist с plaintext-паролями генерируется init-db из .env (SCRAM pass-through с верификаторами из pg_authid на этой сборке не работает); хост-порт HAProxy 15432 (5432 занят другим проектом).

## Роли PostgreSQL

`sys_writer` (запись, NOREPLICATION), `sys_app` (polling через SECURITY DEFINER-функции), `sys_replication` (REPLICATION, streaming), `postgres` (админ). Пароли через env, не в коде.
