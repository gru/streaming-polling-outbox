-- Logical replication slot с failover=true (requirements.md раздел 4.4).
-- Создаётся явно после готовности кластера, идемпотентно.
--
-- Слот — объект конкретной БД: создаём в прикладной БД outbox (решение владельца,
-- docs/deviations.md). PG17 slotsync worker синхронизирует слоты только БД из
-- dbname primary_conninfo на стендбае — Patroni пишет туда outbox через
-- postgresql.database (см. patroni.yml), поэтому synced-копии слота появляются
-- на стендбаях автоматически.
--
-- Плагин — pgoutput: CDC читает изменения outbox-таблицы (см. 01-db-publication.sql).
-- Publication указывается в опциях чтения (peek / START_REPLICATION), а не при
-- создании слота. failover=true включает нативную синхронизацию слота на standby
-- силами PostgreSQL 17 (sync_replication_slots + hot_standby_feedback включены
-- в patroni.yml). Patroni permanent slots не используются (requirements.md).

\connect outbox

SELECT format('SELECT pg_create_logical_replication_slot(slot_name => %L, plugin => %L, failover => true)', 'outbox_slot', 'pgoutput')
WHERE NOT EXISTS (SELECT FROM pg_replication_slots WHERE slot_name = 'outbox_slot') \gexec

-- synchronized_standby_slots на primary принимает ТОЛЬКО физические слоты
-- (PG17: это слоты стендбаев, за WAL которых primary следит перед recycle,
-- чтобы failover логического слота не терял данные). Patroni сам этот параметр
-- не выставляет — собираем список из фактических физических слотов.
-- Замечание: ALTER SYSTEM пишет postgresql.auto.conf локально; после смены
-- primary повторный запуск scripts/init/apply-bootstrap.sh обновит значение.
SELECT format('ALTER SYSTEM SET synchronized_standby_slots = %L',
              (SELECT string_agg(slot_name, ',' ORDER BY slot_name)
               FROM pg_replication_slots WHERE slot_type = 'physical'))
WHERE (SELECT string_agg(slot_name, ',' ORDER BY slot_name)
       FROM pg_replication_slots WHERE slot_type = 'physical') IS NOT NULL \gexec
SELECT pg_reload_conf();
