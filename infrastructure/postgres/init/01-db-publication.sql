-- Прикладная БД outbox, схема и publication (requirements.md 4.2-4.3).
-- Идемпотентно. Выполняется от имени postgres на primary (через HAProxy),
-- стартует подключённым к postgres, дальше переключается на outbox.
--
-- ОТКЛОНЕНИЕ ОТ ТЗ (см. docs/deviations.md, раздел 1): используется ФИЗИЧЕСКАЯ
-- outbox-таблица. Причина: pgoutput не передаёт logical messages ни в одной
-- версии PostgreSQL (проверено на 17.2 и 18.4), а test_decoding (первое
-- обходное решение) — не production-история; финальное решение владельца —
-- классический outbox-паттерн: Writer делает INSERT, CDC читает изменения
-- через logical slot с pgoutput (peek/advance против стриминга).
-- Доставка остаётся logical replication-ом: SELECT-polling таблицы запрещён.
--
-- Прикладная БД — отдельная `outbox`, а не postgres (решение владельца, там же):
-- реальное приложение живёт в своей БД; слот создаётся в ней же. На холодном
-- старте БД уже создаёт bootstrap-метод Patroni (bootstrap-with-appdb.sh);
-- CREATE DATABASE ниже идемпотентен и покрывает повторные прогоны bootstrap
-- на существующем кластере (CREATE DATABASE вне транзакции — psql \gexec).

SELECT 'CREATE DATABASE outbox' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'outbox') \gexec

-- postgres остаётся сервисной БД кластера: прикладным ролям там делать нечего.
REVOKE ALL ON DATABASE postgres FROM PUBLIC;

\connect outbox

-- Outbox-таблица. Строка = сообщение формата requirements.md 4.2:
-- id/topic/body/timestamp. Таблица сознательно минимальна (PK без лишних
-- индексов) — лишние индексы увеличивают объём WAL на INSERT и искажают benchmark.
-- CHECK на topic реализует требование «поле topic не должно позволять
-- произвольную маршрутизацию» (единственный прикладной topic — outbox.events).
CREATE TABLE IF NOT EXISTS public.outbox_messages (
  id         uuid PRIMARY KEY,
  topic      text NOT NULL CHECK (topic = 'outbox.events'),
  body       jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- pgoutput эмитит только изменения таблиц, входящих в publication,
-- поэтому publication НЕ пустая и содержит outbox-таблицу (отклонение от
-- requirements.md 4.3 «она не должна включать таблицы»).
SELECT 'CREATE PUBLICATION outbox_publication' WHERE NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'outbox_publication') \gexec
SELECT format('ALTER PUBLICATION outbox_publication ADD TABLE %I.%I', 'public', 'outbox_messages')
WHERE NOT EXISTS (
  SELECT FROM pg_publication_tables
  WHERE pubname = 'outbox_publication' AND schemaname = 'public' AND tablename = 'outbox_messages'
) \gexec

-- Отзыв избыточных прав PUBLIC, затем явные минимальные GRANT (requirements.md 5.1).
REVOKE ALL ON DATABASE outbox FROM PUBLIC;
GRANT CONNECT ON DATABASE outbox TO sys_writer;      -- INSERT в outbox_messages
GRANT CONNECT ON DATABASE outbox TO sys_app;         -- вызов SECURITY DEFINER-функций polling
GRANT CONNECT ON DATABASE outbox TO sys_replication; -- streaming replication протокол

-- Схема public: запись в таблицу требует USAGE; CREATE не выдаём никому.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM sys_writer, sys_app, sys_replication;
GRANT USAGE ON SCHEMA public TO sys_writer;  -- INSERT в public.outbox_messages
GRANT USAGE ON SCHEMA public TO sys_app;     -- будущие SECURITY DEFINER-функции polling

-- Минимум для Writer: INSERT + DELETE (id генерирует клиент; UPDATE не нужен).
-- Writer держит таблицу ПУСТОЙ (решение владельца): INSERT и DELETE выполняются
-- в одной транзакции — строка существует только внутри транзакции, а сообщение
-- живёт в WAL: logical decoding (pgoutput) эмитит оба изменения, CDC читает их
-- через слот, таблица не растёт (проверено: peek видит B/R/I/D/C той же транзакции).
GRANT INSERT, DELETE ON public.outbox_messages TO sys_writer;

-- Колоночный SELECT(id): PostgreSQL требует SELECT на колонки, читаемые в WHERE
-- команды DELETE (docs: "the SELECT privilege on any column whose values are read
-- in the condition"). Наш DELETE-очистка фильтрует по id — достаточно SELECT
-- только на эту колонку; SELECT всей таблицы (body и пр.) НЕ выдаётся.
GRANT SELECT (id) ON public.outbox_messages TO sys_writer;
