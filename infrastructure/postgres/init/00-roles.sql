-- Роли прикладных пользователей и минимальные привилегии (requirements.md раздел 5).
-- Идемпотентно: повторный запуск не создаёт дубликаты и приводит пароли/атрибуты
-- к заданному состоянию. Пароли передаются через psql-переменные (-v), чтобы
-- не хранить их в исходном коде.
--
-- Выполняется от имени postgres на primary (через HAProxy).

-- sys_writer: только запись в outbox-таблицу (INSERT, см. 01-db-publication.sql);
-- прав репликации и объектов схемы не нужно.
SELECT format('CREATE ROLE sys_writer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT PASSWORD %L',
              :'sys_writer_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sys_writer') \gexec

ALTER ROLE sys_writer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT PASSWORD :'sys_writer_password';

-- sys_app: SQL Polling через SECURITY DEFINER-функции (создаются на этапе 4).
SELECT format('CREATE ROLE sys_app LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT PASSWORD %L',
              :'sys_app_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sys_app') \gexec

ALTER ROLE sys_app LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT PASSWORD :'sys_app_password';

-- sys_replication: streaming replication через протокол логической репликации.
SELECT format('CREATE ROLE sys_replication LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE REPLICATION NOINHERIT PASSWORD %L',
              :'sys_replication_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sys_replication') \gexec

ALTER ROLE sys_replication LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE REPLICATION NOINHERIT PASSWORD :'sys_replication_password';
