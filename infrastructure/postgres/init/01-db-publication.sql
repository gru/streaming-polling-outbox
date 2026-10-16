-- Прикладная схема и publication (requirements.md разделы 3.1, 4.3).
-- Идемпотентно. Выполняется от имени postgres на primary (через HAProxy).
--
-- ВАЖНОЕ ОТКЛОНЕНИЕ ОТ ПЕРВИЧНОГО ПЛАНА (документируется):
-- прикладной БД является `postgres`, а не отдельная БД `outbox`.
-- Причина: в PostgreSQL 17 slotsync worker синхронизирует logical-слоты только
-- той БД, которая указана в primary_conninfo (dbname). Patroni формирует
-- primary_conninfo со своей maintenance-БД (postgres) и не позволяет её
-- переопределить без смены БД самого Patroni (postgresql.database), что ломает
-- первый bootstrap. Слот и publication в `postgres` синхронизируются штатно.

-- Удаляем БД past-итерации, если осталась от предыдущей раскладки.
-- (SELECT pg_terminate_backend(pid) ... \gexec здесь не годится: \gexec
-- выполняет бы результат вызова 't' как SQL.)
DROP DATABASE IF EXISTS outbox;

-- Отзыв избыточных прав PUBLIC, затем явные минимальные GRANT (requirements.md 5.1).
-- CONNECT: writer пишет logical messages; app вызывает polling-функции;
-- replication подключается по протоколу логической репликации.
REVOKE ALL ON DATABASE postgres FROM PUBLIC;
GRANT CONNECT ON DATABASE postgres TO sys_writer;      -- запись pg_logical_emit_message()
GRANT CONNECT ON DATABASE postgres TO sys_app;         -- вызов SECURITY DEFINER-функций polling
GRANT CONNECT ON DATABASE postgres TO sys_replication; -- streaming replication протокол

-- Пустая publication без таблиц (requirements.md 4.3). Publication не является
-- механизмом ограничения доступа к logical messages — см. requirements.md RISK-003.
SELECT 'CREATE PUBLICATION outbox_publication' WHERE NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'outbox_publication') \gexec

-- Схема public не должна быть доступна на запись никому из прикладных ролей:
-- outbox-таблица запрещена, logical messages не требуют объектов в схеме.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM sys_writer, sys_app, sys_replication;
GRANT USAGE ON SCHEMA public TO sys_app;  -- будущие SECURITY DEFINER-функции polling
