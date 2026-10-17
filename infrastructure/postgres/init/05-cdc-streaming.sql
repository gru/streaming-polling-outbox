-- Этап 5 (requirements.md 8): гранты для CDC Streaming.
--
-- Стриминг читает слот replication-протоколом от роли sys_replication
-- (создана в 00-roles.sql: LOGIN NOSUPERUSER REPLICATION). SQL-функции
-- peek/advance ему НЕ грантуются — requirements.md 8: «Не используй polling
-- SQL-функций в этом режиме» (слот двигается сервером по replication
-- feedback, а не явным advance).
--
-- Статус слота стриминг получает той же функцией, что и Polling —
-- cdc.outbox_slot_status() (SECURITY DEFINER, search_path=pg_catalog,
-- read-only): форма ответа едина между двумя CDC-сервисами, привилегии
-- минимальны (EXECUTE на read-only функцию вместо прямого доступа к
-- pg_replication_slots). GRANT идемпотентен.

\connect outbox

GRANT USAGE ON SCHEMA cdc TO sys_replication;
GRANT EXECUTE ON FUNCTION cdc.outbox_slot_status() TO sys_replication;
