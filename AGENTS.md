# AGENTS.md

Исследовательский стенд: сравнение двух способов доставки transactional logical messages из PostgreSQL 17 в Kafka — **SQL Polling** (`pg_logical_slot_peek_binary_changes()`) vs **Npgsql Streaming Replication** (`pgoutput`). НЕ git-репозиторий.

## Документы (читать перед работой)

- `requirements.md` — **единый авторитетный spec** (объединяет `prd.md` и `prd-2.md`; правила prd-2 по WAL retention имеют приоритет). Все детали: роли, HA-тесты TEST-HA-001…008, метрики, план этапов 1–10, критерии готовности, риски RISK-001…010.
- `prd.md`, `prd-2.md` — исходные PRD; использовать только как справку, при расхождениях главенствует `requirements.md`.

## Текущее состояние

Кода нет — только требования. Целевая структура репозитория описана в `requirements.md` раздел 18 (`src/Writer`, `src/Cdc.Polling`, `src/Cdc.Streaming`, `src/Shared`, `src/KafkaValidator`, `infrastructure/`, `scripts/`, `tests/`, `results/`, `docs/`).

## Критические жёсткие ограничения (requirements.md, раздел 2)

Нарушение любого из них ломает цель исследования:

- Только `pgoutput` в обоих CDC-режимах; без Debezium/Kafka Connect; без альтернативных output plugins.
- Никакой outbox-таблицы и никакого polling таблицы вместо logical replication.
- KafkaFlow — основной API публикации (Confluent.Kafka допустим только как внутренняя зависимость).
- .NET 8 строго (`net8.0`), PostgreSQL строго 17, всё в Docker Compose.
- Никакого SUPERUSER для прикладных ролей; LSN подтверждается только после Kafka delivery (at-least-once, дубликаты допустимы, потери нет).
- Не использовать Patroni permanent replication slots для logical slots — failover slots синхронизирует сам PostgreSQL 17.
- Не скрывать потери WAL / ошибки; не подменять реальные результаты тестов предполагаемыми цифрами.

## Ключевые технические грабли

- `max_slot_wal_keep_size` считается от **фактически измеренного объёма WAL** (разница LSN), НЕ от размера payload; в нормальных тестах `-1`, лимит только в отдельном WAL Pressure Test.
- Количество бинарных записей `pgoutput` ≠ количество пользовательских logical messages (границы транзакций begin/commit).
- Publication не фильтрует logical messages — не считать её границей доступа.
- Polling читает через `peek` (не `get_changes` — тот потребляет до подтверждения Kafka); advance — только через SECURITY DEFINER-функции с зафиксированным слотом/publication.
- Streaming подключается к PostgreSQL напрямую, минуя PgBouncer; Writer — по умолчанию через PgBouncer (`pool_mode = transaction`).
- Разделять понятия: внутренний batching Kafka producer ≠ прикладной batch ≠ подтверждение LSN.
- Инвалидированный слот ≠ пересоздать молча: явная ошибка с диагностикой.

## Роли PostgreSQL

`sys_writer` (запись, NOREPLICATION), `sys_app` (polling через SECURITY DEFINER-функции), `sys_replication` (REPLICATION, streaming), `postgres` (админ). Пароли через env, не в коде.
