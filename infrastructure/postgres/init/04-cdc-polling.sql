-- CDC Polling: SECURITY DEFINER-функции для роли sys_app (requirements.md 5.2, 7.1-7.2).
-- Идемпотентно. Выполняется от имени postgres на primary (через HAProxy),
-- работает в прикладной БД outbox (слот, publication и outbox-таблица живут в ней).
--
-- Зачем wrapper'ы: SQL-интерфейс логического декодирования (peek) и
-- pg_replication_slot_advance требуют привилегий, которых у sys_app нет
-- (NOREPLICATION, NOSUPERUSER; фактическая ошибка PG17 зафиксирована в
-- docs/deviations.md). Минимальное решение без повышения привилегий роли —
-- SECURITY DEFINER-функции владельца postgres с жёстко зашитыми параметрами
-- (requirements.md 5.2 «Внутри функций жёстко зафиксируй…»):
--   * имя слота (outbox_slot) и publication (outbox_publication) — константы;
--   * параметры декодирования (proto_version=1) — константы;
--   * размер пачки — параметр вызывающего, зажатый 1..10000 (рабочее значение
--     настраивается на стороне сервиса, requirements.md 7.1);
--   * advance не может подтвердить позицию дальше последнего реально
--     прочитанного peek (watermark cdc.peek_state) — на стороне БД это максимум
--     достижимой проверки «не подтверждай то, что не читал»: о доставке в Kafka
--     база знать не может (requirements.md 5.2 оговаривает это явно).

\connect outbox

CREATE SCHEMA IF NOT EXISTS cdc;
REVOKE ALL ON SCHEMA cdc FROM PUBLIC;
GRANT USAGE ON SCHEMA cdc TO sys_app;

-- Watermark последнего peek: ровно одна строка (CHECK id=1). Прямого доступа
-- для sys_app нет — читается/пишется только SECURITY DEFINER-функциями ниже.
-- Обновление watermark и возврат строк peek выполняются в ОДНОЙ транзакции
-- (data-modifying CTE внутри cdc.peek_outbox_changes): watermark закоммичен
-- тогда и только тогда, когда клиент реально получил строки peek — обрыв
-- соединения на середине выборки откатывает и строки, и watermark.
-- В publication outbox_publication таблица не входит: собственная запись
-- watermark в декодирование не попадает.
CREATE TABLE IF NOT EXISTS cdc.peek_state (
  id        smallint    PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  last_lsn  pg_lsn      NOT NULL,
  peeked_at timestamptz NOT NULL DEFAULT now()
);

-- Чтение изменений слота БЕЗ потребления (peek — confirmed_flush не двигается).
-- Размер пачки передаёт сервис (requirements.md 7.1, default 100), функция
-- зажимает его в 1..10000. Watermark обновляется только при росте (idle-пики
-- ничего не пишут). Декодирование выполняется ровно один раз (MATERIALIZED).
CREATE OR REPLACE FUNCTION cdc.peek_outbox_changes(p_limit integer DEFAULT 100)
RETURNS TABLE (lsn pg_lsn, xid xid, data bytea)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog
AS $peek$
WITH decoded AS MATERIALIZED (
  SELECT c.lsn, c.xid, c.data
  FROM pg_catalog.pg_logical_slot_peek_binary_changes(
         'outbox_slot',                                  -- константа: единственный слот CDC
         NULL,                                           -- без ограничения по LSN
         LEAST(GREATEST(COALESCE(p_limit, 100), 1), 10000), -- ограничение пачки (5.2)
         'proto_version', '1',                           -- константы параметров декодирования
         'publication_names', 'outbox_publication'
       ) AS c
),
watermark AS (
  INSERT INTO cdc.peek_state (id, last_lsn, peeked_at)
  SELECT 1, m.max_lsn, now()
  FROM (SELECT max(d.lsn) AS max_lsn FROM decoded d) m
  WHERE m.max_lsn IS NOT NULL
  ON CONFLICT (id) DO UPDATE
    SET last_lsn = EXCLUDED.last_lsn, peeked_at = EXCLUDED.peeked_at
    WHERE EXCLUDED.last_lsn > cdc.peek_state.last_lsn
)
SELECT d.lsn, d.xid, d.data FROM decoded d;
$peek$;

-- Продвижение слота (подтверждение позиции ПОСЛЕ успешной доставки в Kafka —
-- решение о моменте вызова принимает сервис; функция гарантирует, что target
-- не дальше последнего прочитанного peek). Сам pg_replication_slot_advance
-- дополнительно не даёт двигаться назад и дальше текущего конца WAL.
-- В PG17 функция возвращает (slot_name, end_lsn); столбец end_xid появился
-- только в PG18 — потому RETURNS TABLE без него.
DROP FUNCTION IF EXISTS cdc.advance_outbox_slot(pg_lsn);
CREATE OR REPLACE FUNCTION cdc.advance_outbox_slot(p_target pg_lsn)
RETURNS TABLE (slot_name name, end_lsn pg_lsn)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $advance$
DECLARE
  v_watermark pg_lsn;
BEGIN
  IF p_target IS NULL THEN
    RAISE EXCEPTION 'advance отклонён: целевая позиция NULL'
      USING HINT = 'Подтверждать можно только позицию, полученную из cdc.peek_outbox_changes';
  END IF;

  SELECT last_lsn INTO v_watermark FROM cdc.peek_state WHERE id = 1;

  IF v_watermark IS NULL THEN
    RAISE EXCEPTION 'advance (%) отклонён: peek ещё ничего не возвращал (watermark пуст)', p_target;
  END IF;

  IF p_target > v_watermark THEN
    RAISE EXCEPTION 'advance (%) отклонён: дальше последней прочитанной peek позиции (%)', p_target, v_watermark
      USING HINT = 'Подтверждение вслепую приведёт к потере сообщений; БД разрешает подтверждать только реально прочитанное';
  END IF;

  RETURN QUERY
  SELECT a.slot_name, a.end_lsn
  FROM pg_catalog.pg_replication_slot_advance('outbox_slot', p_target) AS a;
END;
$advance$;

-- Диагностика слота + текущий WAL LSN: метрики WAL lag (requirements.md 13.2)
-- и обнаружение инвалидации (wal_status='lost' / слот отсутствует).
CREATE OR REPLACE FUNCTION cdc.outbox_slot_status()
RETURNS jsonb
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = pg_catalog
AS $status$
SELECT COALESCE(
         (SELECT jsonb_build_object(
                   'exists', true,
                   'active', s.active,
                   'wal_status', s.wal_status,
                   'confirmed_flush_lsn', s.confirmed_flush_lsn,
                   'restart_lsn', s.restart_lsn,
                   'safe_wal_size', s.safe_wal_size,
                   'plugin', s.plugin,
                   'database', s.database,
                   'synced', s.synced)
            FROM pg_catalog.pg_replication_slots s
            WHERE s.slot_name = 'outbox_slot'),
         jsonb_build_object('exists', false))
     || jsonb_build_object(
          'in_recovery', pg_catalog.pg_is_in_recovery(),
          'current_wal_lsn',
          CASE WHEN pg_catalog.pg_is_in_recovery() THEN NULL
               ELSE pg_catalog.pg_current_wal_lsn() END);
$status$;

-- EXECUTE только у sys_app и только на эти три функции (requirements.md 5.2:
-- «явные права EXECUTE»; PUBLIC отзывается).
REVOKE ALL ON FUNCTION cdc.peek_outbox_changes(integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION cdc.advance_outbox_slot(pg_lsn) FROM PUBLIC;
REVOKE ALL ON FUNCTION cdc.outbox_slot_status() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION cdc.peek_outbox_changes(integer) TO sys_app;
GRANT EXECUTE ON FUNCTION cdc.advance_outbox_slot(pg_lsn) TO sys_app;
GRANT EXECUTE ON FUNCTION cdc.outbox_slot_status() TO sys_app;

COMMENT ON SCHEMA cdc IS 'SECURITY DEFINER-функции CDC Polling (requirements.md 5.2): слот/publication/параметры декодирования и лимит пачки зафиксированы внутри';
COMMENT ON FUNCTION cdc.peek_outbox_changes(integer) IS 'Чтение изменений слота outbox_slot без потребления (peek); обновляет watermark cdc.peek_state в той же транзакции; лимит пачки 1..10000';
COMMENT ON FUNCTION cdc.advance_outbox_slot(pg_lsn) IS 'Продвижение outbox_slot не дальше последнего прочитанного peek (watermark cdc.peek_state); вызывать только после доставки сообщений в Kafka';
COMMENT ON FUNCTION cdc.outbox_slot_status() IS 'Состояние слота outbox_slot и текущий WAL LSN (метрики WAL lag, диагностика инвалидации)';
