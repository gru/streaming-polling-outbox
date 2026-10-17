-- Этап 6 (requirements.md 13.3): роль только для мониторинга.
--
-- postgres_exporter подключается напрямую через HAProxy (всегда к primary;
-- PgBouncer не используется — экспортеру достаточно одного собственного
-- соединения). Роль читает статистику кластера, но не имеет ни прав на
-- peek/advance-функции cdc, ни REPLICATION: мониторинг не может потреблять
-- или двигать слот.
--
-- pg_monitor — встроенная роль PostgreSQL (не SUPERUSER): объединяет
-- pg_read_all_settings, pg_read_all_stats и pg_stat_scan_tables. Её достаточно
-- для pg_stat_replication и pg_replication_slots, из которых берутся
-- replication lag, состояние logical slot и WAL retention (кастомные запросы
-- infrastructure/prometheus/postgres-exporter-queries.yaml).
--
-- Идемпотентно: пароль/атрибуты приводятся к заданному состоянию ALTER'ом.
--
-- Привилегии pg_monitor обязаны действовать при логине (pg_ls_waldir,
-- pg_stat_replication): роль — INHERIT, а членство — WITH INHERIT TRUE.
-- Грабли PG16+: у членства своя опция inherit, она фиксируется в момент
-- GRANT по атрибуту роли на тот момент; позже ALTER ROLE ... INHERIT её
-- НЕ обновляет (первый грант при NOINHERIT-роли записал inherit=false —
-- permission denied на каждом scrape, проверено на стенде).

SELECT format('CREATE ROLE sys_monitor LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION INHERIT PASSWORD %L',
              :'sys_monitor_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sys_monitor') \gexec

ALTER ROLE sys_monitor LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION INHERIT PASSWORD :'sys_monitor_password';

GRANT pg_monitor TO sys_monitor WITH INHERIT TRUE;

\connect outbox

GRANT CONNECT ON DATABASE outbox TO sys_monitor;
