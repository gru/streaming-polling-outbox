-- Реконсилиация failover-слота на СТЕНДБАЯХ (выполняется на каждой ноде напрямую,
-- не через HAProxy). Источник «остатка»: после switchover/failover старый primary
-- возвращается в кластер как standby со СВОИМ оригинальным слотом (synced=false —
-- на primary этот флаг не выставляется). На текущем primary есть failover-слот с
-- тем же именем, и slotsync worker не может создать синхронизированную копию:
-- конфликт имён завершает воркер целиком — "exiting from slot synchronization
-- because same name slot ... already exists on the standby" (исходники PG 17.2).
-- Synced-копии при этом переживают рестарты штатно: synced/failover — поля
-- сохраняемого на диск состояния слота (ReplicationSlotPersistentData), воркер
-- обновляет такие копии на месте (проверено рестартом pg2 на стенде 17.2: копия
-- уцелела с synced=true, воркер перезапустился без ошибок). Полный рестарт стенда
-- порождал остатки именно через гонки промоушенов Patroni — вернувшийся старый
-- primary, тот же случай. Удаляем остаток — воркер пересоздаст копию.
--
-- Отдельный случай: копия не может стать persistent — WAL/каталоги стендбая уже
-- недоступны ("remote slot precedes local slot" / "could lead to data loss").
-- Он этим скриптом НЕ лечится: по документации нужно продвижение слота на
-- primary (docs/deviations.md раздел 3; работающий CDC делает это сам), локальный
-- слот не трогаем — фильтр NOT synced его не затрагивает.
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
