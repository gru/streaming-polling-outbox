-- Прикладная схема: outbox-таблица и publication (requirements.md 4.2-4.3).
-- Идемпотентно. Выполняется от имени postgres на primary (через HAProxy).
--
-- ОТКЛОНЕНИЕ ОТ ТЗ (см. docs/deviations.md, раздел 1): используется ФИЗИЧЕСКАЯ
-- outbox-таблица. Причина: pgoutput не передаёт logical messages ни в одной
-- версии PostgreSQL (проверено на 17.2 и 18.4), а test_decoding (первое
-- обходное решение) — не production-история; финальное решение владельца —
-- классический outbox-паттерн: Writer делает INSERT, CDC читает изменения
-- через logical slot с pgoutput (peek/advance против стриминга).
-- Доставка остаётся logical replication-ом: SELECT-polling таблицы запрещён.
--
-- Прикладная БД — `postgres` (см. там же, раздел 2): PG17 slotsync worker
-- синхронизирует слоты только БД из primary_conninfo.

-- Outbox-таблица. Строка = сообщение формата requirements.md 4.2:
-- id/topic/body/timestamp. Таблица сознательно минимальна (PK без лишних
-- индексов) — лишние индексы увеличивают объём WAL на INSERT и исказят benchmark.
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
REVOKE ALL ON DATABASE postgres FROM PUBLIC;
GRANT CONNECT ON DATABASE postgres TO sys_writer;      -- INSERT в outbox_messages
GRANT CONNECT ON DATABASE postgres TO sys_app;         -- вызов SECURITY DEFINER-функций polling
GRANT CONNECT ON DATABASE postgres TO sys_replication; -- streaming replication протокол

-- Схема public: запись в таблицу требует USAGE; CREATE не выдаём никому.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM sys_writer, sys_app, sys_replication;
GRANT USAGE ON SCHEMA public TO sys_writer;  -- INSERT в public.outbox_messages
GRANT USAGE ON SCHEMA public TO sys_app;     -- будущие SECURITY DEFINER-функции polling

-- Минимум для Writer: только INSERT (id генерирует клиент; UPDATE/DELETE не нужны).
GRANT INSERT ON public.outbox_messages TO sys_writer;
