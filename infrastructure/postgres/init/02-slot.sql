-- Logical replication slot с failover=true (requirements.md раздел 4.4).
-- Создаётся явно после готовности кластера, идемпотентно.
--
-- ОТКЛОНЕНИЕ ОТ ТЗ (см. docs/deviations.md): плагин слота — test_decoding,
-- а не pgoutput. Причина: pgoutput не передаёт logical messages ни в одной
-- версии PostgreSQL (проверено эмпирически на 17.2 и 18.4, подтверждено
-- release notes PG18), т.е. связка «pgoutput + pg_logical_emit_message»
-- неработоспособна. test_decoding входит в официальный образ postgres,
-- сохраняет архитектуру ТЗ: transactional messages без outbox-таблицы,
-- слот + peek/advance против стриминга.
-- failover=true включает нативную синхронизацию слота на standby силами
-- PostgreSQL 17 (sync_replication_slots + hot_standby_feedback включены
-- в patroni.yml). Patroni permanent slots не используются (requirements.md).

SELECT format('SELECT pg_create_logical_replication_slot(slot_name => %L, plugin => %L, failover => true)', 'outbox_slot', 'test_decoding')
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
