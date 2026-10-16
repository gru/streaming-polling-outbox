# Отклонения от требований (requirements.md)

Все отклонения обнаружены практической проверкой и задокументированы согласно
правилу requirements.md раздел 2: «Выбери технически корректную реализацию,
сохранив исходную цель исследования. Документируй все отклонения».

## 1. Плагин слота — test_decoding вместо pgoutput (критическое)

**Пункт ТЗ:** «Используй только `pgoutput` в обоих CDC-режимах» (раздел 2),
структура раздела 7.1 предполагает разбор logical messages из бинарного pgoutput.

**Факт:** pgoutput **не передаёт logical messages ни в одной версии PostgreSQL**.

- Проверено эмпирически на PostgreSQL 17.2 и 18.4: транзакция, содержащая только
  `pg_logical_emit_message(true, 'outbox', ...)`, не видна ни через
  `pg_logical_slot_peek_binary_changes(..., 'proto_version','1',
  'publication_names',...)` (pgoutput), ни через стриминг — при непустой
  publication и принудительном XID тоже. Сообщения от `pg_logical_emit_message`
  pgoutput не эмитит вовсе (записи BEGIN/Relation/INSERT/COMMIT от табличных
  изменений при этом приходят).
- Подтверждено релиз-нотами: в [release notes PostgreSQL 18](https://www.postgresql.org/docs/18/release-18.html)
  поддержка logical messages в pgoutput отсутствует; патч из дискуссии
  [pgsql-hackers](https://www.postgresql.org/message-id/CALj2ACXLkU4y%2Bmmk2gHzmvSV4KTishSe%3DHhZ_yn%3DNHFBDGrwFQ%40mail.gmail.com)
  не принят. Тип 'M' в [документации протокола](https://www.postgresql.org/docs/current/protocol-logical-replication.html)
  зарезервирован для сторонних output-плагинов.
- Т.е. связка «pgoutput + pg_logical_emit_message» (ядро обоих CDC-режимов ТЗ)
  неработоспособна. См. также RISK-001.

**Решение (согласовано с владельцем проекта):** слот `outbox_slot` создаётся с
плагином **`test_decoding`** (входит в официальный образ postgres, внешних
зависимостей нет). Сохранена архитектура ТЗ: transactional messages через
`pg_logical_emit_message`, БЕЗ outbox-таблицы, режимы «слот + peek/advance»
против «стриминг». Отключено требование «только pgoutput».

**Следствия для следующих этапов:**
- CDC Polling разбирает текстовые записи `pg_logical_slot_peek_changes()`:
  `BEGIN <xid>`, `message: transactional: 1 prefix: outbox, sz: N content:...`,
  `COMMIT <xid>`.
- CDC Streaming использует generic Npgsql Logical Replication API
  (не `PgOutputReplicationSlot`).
- Пустая publication `outbox_publication` сохранена (раздел 4.3), но не
  участвует в декодировании.

## 2. Прикладная БД — `postgres` вместо отдельной `outbox`

**Пункт ТЗ:** «одна прикладная база данных» (раздел 3.1); первоначальный план —
отдельная БД `outbox`.

**Факт:** в PostgreSQL 17 slotsync worker синхронизирует failover logical-слоты
только той БД, которая указана в `primary_conninfo` (dbname). Patroni формирует
`primary_conninfo` от своей maintenance-БД (`postgres`) и не позволяет изменить
dbname, не переведя сам Patroni на другую БД (`postgresql.database`) — что
ломает первый bootstrap (БД ещё не существует).

**Решение:** единственная прикладная БД стенда — `postgres`. Publication, слот
и сообщения живут в ней; синхронизация слота на standby проверена (synced=true).
Требование «одна прикладная база данных» выполняется буквально.

## 3. Прочее (некритическое)

- **pg_hba максимально лоялен** (по решению владельца): ограничения источника не
  относятся к задаче; аутентификация scram сохранена. Минимальные привилегии
  реализуются GRANT'ами на уровне ролей/БД (раздел 5 требований).
- **wal_level и кластерные параметры** задаются через `bootstrap.dcs.postgresql.parameters`
  (dynamic config): параметры из `CMDLINE_OPTIONS` Patroni применяются ко всем
  нодам только через DCS, member-local `postgresql.parameters` для них
  игнорируются.
