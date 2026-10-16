-- Реконсилиация failover-слота на СТЕНДБАЯХ (выполняется на каждой ноде напрямую,
-- не через HAProxy). После рестарта стенда на реплике может остаться локальный
-- остаток слота (synced=false), из-за которого PG17 slotsync worker отказывается
-- работать: "same name slot already exists on the standby". Удаляем такой остаток —
-- воркер пересоздаст его как синхронизированную копию с primary.
-- На primary (pg_is_in_recovery()=false) ничего не делаем: там авторитетный слот.

SELECT format('SELECT pg_drop_replication_slot(%L)', slot_name)
FROM pg_replication_slots
WHERE slot_name = 'outbox_slot'
  AND NOT synced
  AND NOT active
  AND pg_is_in_recovery() \gexec
