# Отклонения от требований (requirements.md)

Все отклонения обнаружены практической проверкой и задокументированы согласно
правилу requirements.md раздел 2: «Выбери технически корректную реализацию,
сохранив исходную цель исследования. Документируй все отклонения».

## 1. Механика CDC: outbox-таблица + pgoutput (вместо pg_logical_emit_message)

**Пункты ТЗ:** «Не создавай outbox-таблицу» (раздел 2), «Сообщения создаются
через pg_logical_emit_message()» (4.1), «пустая publication… не должна включать
таблицы» (4.3).

**История и причины перехода (зафиксировано по требованию владельца):**

1. Исходный замысел ТЗ: transactional messages через `pg_logical_emit_message`
   + pgoutput, без физической таблицы.
2. **Практически доказано (PostgreSQL 17.2 и 18.4): pgoutput не передаёт
   logical messages ни в одной версии PostgreSQL.** Транзакция, содержащая
   только `pg_logical_emit_message`, невидима ни для
   `pg_logical_slot_peek_binary_changes(..., 'publication_names', ...)`,
   ни для стриминга — при непустой publication и принудительном XID тоже.
   Подтверждено [release notes PostgreSQL 18](https://www.postgresql.org/docs/18/release-18.html):
   поддержка logical messages в pgoutput там отсутствует; патч из дискуссии
   [pgsql-hackers](https://www.postgresql.org/message-id/CALj2ACXLkU4y%2Bmmk2gHzmvSV4KTishSe%3DHhZ_yn%3DNHFBDGrwFQ%40mail.gmail.com)
   не принят. Тип 'M' в [документации протокола](https://www.postgresql.org/docs/current/protocol-logical-replication.html)
   зарезервирован для сторонних output-плагинов. Связка из ТЗ неработоспособна.
3. Первое обходное решение (2026-10-16): слот на плагине `test_decoding` —
   сохраняло механику «сообщения без таблицы», проверено сквозным smoke.
4. **Финальное решение (владелец, 2026-10-17): переход на классический
   outbox-паттерн. Причина: test_decoding — не production-история**
   (демо-плагин, текстовый формат, нет экосистемы/поддержки), тогда как
   outbox-таблица + pgoutput — стандартный industrial-паттерн CDC.
5. **Уточнение (владелец, 2026-10-17): Writer держит outbox-таблицу пустой** —
   `INSERT` и `DELETE` тех же строк выполняются в одной транзакции. Строка
   существует только внутри транзакции, сообщение живёт в WAL: logical decoding
   эмитит оба изменения (проверено: peek видит B/R/I/D/C такой транзакции),
   CDC читает Insert-записи и игнорирует Delete-записи. Таблица не растёт,
   жизненный цикл строк решён архитектурно.

**Что нарушено из исходного ТЗ:** запрет outbox-таблицы; publication содержит
таблицу (не пустая).

**Что сохранено (цель исследования не изменилась):** доставка — по-прежнему
logical replication через слот (peek + advance против Npgsql-стриминга —
предмет сравнения benchmark'а); SELECT-polling таблицы запрещён; at-least-once
с подтверждением LSN только после Kafka delivery.

**Детали реализации:**

- `public.outbox_messages(id uuid PK, topic text CHECK (topic='outbox.events'),
  body jsonb, created_at timestamptz)` — строка соответствует формату сообщения
  requirements.md 4.2 (id/topic/body/timestamp). Таблица минимальна: PK без
  дополнительных индексов — лишние индексы увеличивают WAL на INSERT и искказят
  замеры. CHECK на topic реализует запрет произвольной маршрутизации.
- publication `outbox_publication` содержит `outbox_messages`: pgoutput эмитит
  только изменения опубликованных таблиц.
- `sys_writer`: USAGE на схему + INSERT/DELETE на таблицу + колоночный
  `SELECT (id)` — PostgreSQL требует SELECT на колонки, читаемые в WHERE
  команды DELETE; SELECT всей таблицы (body и пр.) не выдаётся.
- Слот `outbox_slot`: плагин `pgoutput`, `failover=true`.

## 2. Прикладная БД — отдельная `outbox` (не `postgres`)

**Пункт ТЗ:** «одна прикладная база данных» (раздел 3.1). Требование выполняется
буквально — прикладная БД ровно одна.

**История (оба решения владельца):**

1. Первоначально прикладной БД был `postgres`: PG17 slotsync worker синхронизирует
   failover-слоты только БД, указанной в `dbname` параметра `primary_conninfo`
   на стендбае, а Patroni пишет туда свою maintenance-БД (по умолчанию
   `postgres`). «В лоб» вариант `postgresql.database: outbox` ломал первый
   bootstrap: эта же опция направляет собственные SQL-подключения Patroni
   (создание ролей, слоты, CHECKPOINT) в outbox, которой после initdb ещё нет.
2. **Решение владельца (2026-10-17): прикладная БД — отдельная `outbox`, не
   `postgres`.** Мотивация: реальное приложение живёт в собственной БД (и БД с
   слотами может быть несколько); стенд обязан проверять реальную конфигурацию,
   а не дефолтную maintenance-БД. Патчи Patroni владельцем запрещены.

**Реализация на стоковом Patroni 4.1.5 (без патчей, только официальные фичи):**

- `postgresql.database: outbox` (patroni.yml) — официальная опция Patroni: задаёт dbname
  в `primary_conninfo` реплик (под неё работает PG17 slotsync) и БД собственных
  подключений Patroni;
- кастомный bootstrap-метод `bootstrap-with-appdb`
  (infrastructure/patroni/bootstrap-with-appdb.sh): initdb (UTF8/C.UTF-8/
  data-checksums — прежние флаги) → временный инстанс только на unix-сокете →
  `CREATE DATABASE outbox` → чистый shutdown. БД существует в PGDATA до первого
  SQL Patroni — тупик bootstrap снят. Реплики получают БД через pg_basebackup.

**Проверено на стенде:** `primary_conninfo` обоих стендбаев содержит
`dbname=outbox`; слот `outbox_slot` в БД `outbox` — `synced=true`,
`wal_status=reserved` на обоих стендбаях; двойной switchover (pg3→pg2→pg1):
слот пережил promotion с сохранением restart_lsn, peek работает на новом
leader, стендбаи пересинхронизируют слот; check-infra 42/42, smoke-writer 26/26.

**Выявленное ограничение PG17 (важно для этапов 4–5):** непотребляемый слот
(CDC ещё не читает и не подтверждает LSN) со временем перестаёт
синхронизироваться: воркер пишет `could not synchronize replication slot ...
remote slot precedes local slot` (catalog_xmin стендбая обогнал слот), и при
promotion такой слот НЕ сохраняется — «the slot will not be persisted to avoid
data loss» ([docs §47.2.3](https://www.postgresql.org/docs/17/logicaldecoding-explanation.html)).
Это документированное поведение, не баг. Лекарство из документации — продвигать
слот (consume/`pg_logical_slot_get_binary_changes`); в реальном benchmark CDC
делает это непрерывно после Kafka delivery, поэтому условие не возникает. До
появления CDC: перед failover-экспериментами продвигать слот вручную; после
demotion старый leader держит несинхронизированный остаток слота — убирается
`scripts/init/apply-bootstrap.sh` (03-slot-reconcile.sql).

## 3. Прочее (некритическое)

- **pg_hba максимально лоялен** (по решению владельца): ограничения источника не
  относятся к задаче; аутентификация scram сохранена. Минимальные привилегии
  реализуются GRANT'ами на уровне ролей/БД (раздел 5 требований).
- **wal_level и кластерные параметры** задаются через `bootstrap.dcs.postgresql.parameters`
  (dynamic config): параметры из `CMDLINE_OPTIONS` Patroni применяются ко всем
  нодам только через DCS, member-local `postgresql.parameters` для них
  игнорируются.
