-- Реконсилиация failover-слота на СТЕНДБАЯХ (выполняется на каждой ноде напрямую,
-- не через HAProxy). После рестарта стенда с СОХРАНЁННЫМИ томами на реплике
-- остаётся локальный остаток слота (в PG17 флаг synced не переживает рестарт),
-- из-за которого slotsync worker отказывается работать: "same name slot
-- already exists on the standby". Удаляем такой остаток — воркер пересоздаст
-- его как синхронизированную копию с primary.
--
-- При старте с чистого листа (wipe: down -v, свежие тома — как в тестовых
-- циклах A/B) проблемы нет: остатков не существует, этот скрипт — no-op.
-- На primary (pg_is_in_recovery()=false) ничего не делаем: там авторитетный слот.
-- pg_replication_slots — общий каталог кластера: скрипт работает из любой БД
-- (запускается подключённым к postgres).

SELECT format('SELECT pg_drop_replication_slot(%L)', slot_name)
FROM pg_replication_slots
WHERE slot_name = 'outbox_slot'
  AND NOT synced
  AND NOT active
  AND pg_is_in_recovery() \gexec
