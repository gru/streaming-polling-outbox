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

**Выявленное ограничение PG17** (непотребляемый слот и failover-синхронизация,
важно для этапов 4–5) — вынесено в отдельный раздел 3 ниже.

## 3. Непотребляемый logical-слот не переживает failover (документированное поведение PG17)

Обнаружено при первом контролируемом switchover-тесте после миграции прикладной
БД на `outbox` (2026-10-17): слот, который никто не читает, при promotion
стендбая **исчезает на новом leader**. Это не баг миграции и не баг PostgreSQL —
ровно это состояние описано в документации
[PostgreSQL 17, §47.2.3 «Replication Slot Synchronization»](https://www.postgresql.org/docs/17/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS-SYNCHRONIZATION).

### Механика (по документации)

Синхронизированная копия слота сохраняется на стендбае (и переживёт promotion)
только при условии сохранности нужных каталогов и WAL — дословно:

> «the standby can persist the synchronized slot only if the following
> condition is met: The logical replication slot on the primary must retain
> WALs and system catalog rows that are still available on the standby.»

> «If the required WALs or catalog rows have already been purged from the
> standby, the slot will not be persisted to avoid data loss.»

Если слот никто не потребляет, его `restart_lsn`/`confirmed_flush_lsn` и
catalog_xmin на primary стоят на месте, а catalog_xmin стендбая уходит вперёд
(горизонт каталожных строк стендбая движется с кластером). Воркер отказывается
синхронизировать такую копию — пример из документации (наш случай 1-в-1,
формулировка примера чуть отличается от фактической в 17.2):

> `LOG: could not synchronize replication slot "failover_slot"`
> `DETAIL: Synchronization could lead to data loss, because the remote slot
> needs WAL at LSN 0/3003F28 and catalog xmin 754, but the standby has
> LSN 0/3003F28 and catalog xmin 756.`

Фактическое сообщение на стенде (PG 17.2, до switchover):

```
LOG:  could not synchronize replication slot "outbox_slot" because remote slot precedes local slot
DETAIL:  The remote slot has LSN 0/40318A0 and catalog xmin 741, but the local slot has LSN 0/40318A0 and catalog xmin 773.
```

LSN совпадают, catalog_xmin стендбая (773) обогнал слот (741) — копия не
«sync-ready», и при promotion она отбрасывается. Дословно про условие
выживания:

> «The ability to resume logical replication after failover depends upon the
> pg_replication_slots.synced value for the synchronized slots on the standby
> at the time of failover. Only persistent slots that have attained synced
> state as true on the standby before failover can be used for logical
> replication after failover.»

### Лекарство — из той же документации

> «If the logical replication slot is actively used by a consumer, no manual
> intervention is needed; the slot will advance automatically, and
> synchronization will resume in the next cycle. However, if no consumer is
> configured, it is advisable to manually advance the slot on the primary
> using pg_logical_slot_get_changes or pg_logical_slot_get_binary_changes.»

### Эмпирика на стенде (2026-10-17)

1. Первый switchover (pg3→pg2) при непотребляемом слоте: слот исчез на новом
   leader, копия на втором стендбае тоже пропала — «not persisted to avoid
   data loss».
2. `pg_logical_slot_get_binary_changes()` на primary (аналог advance CDC):
   restart_lsn/confirmed_flush_lsn/catalog_xmin продвинулись до текущего
   горизонта; через ~40 с воркер перестал писать ошибки синхронизации.
3. Второй switchover (pg2→pg1): слот **пережил promotion** — на новом leader
   `outbox_slot` с `failover=true`, `wal_status=reserved`, `restart_lsn` равен
   точке consume (данные не потеряны); оба стендбая пересинхронизировали копию
   (`synced=true`).
4. Прогон Writer (500 сообщений) через нового leader: 500 written / 0 failed /
   0 ambiguous, peek растёт, таблица пуста.

### Выводы для стенда

- В реальном benchmark условие не возникает: CDC потребляет слот и подтверждает
  LSN после Kafka delivery непрерывно (requirements.md 8-9).
- До появления CDC (этапы 4-5) перед failover/switchover-экспериментами
  продвигать слот вручную на primary:
  `SELECT count(*) FROM pg_logical_slot_get_binary_changes('outbox_slot', NULL, NULL, 'proto_version','1','publication_names','outbox_publication')`
  (бд `outbox`; peek — НЕ продвигает, get — продвигает).
- После demotion старый leader держит несинхронизированный остаток слота —
  убирается `./scripts/init/apply-bootstrap.sh` (03-slot-reconcile.sql).
- `synced=true` у слота на новом leader после promotion — норма (это
  промоутнутая синхронизированная копия), не признак рассинхрона.
- Связанное требование документации (выполнено в стенде): на стендбае
  обязательны `hot_standby_feedback` и `primary_slot_name`
  ([§47.2.3](https://www.postgresql.org/docs/17/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS-SYNCHRONIZATION),
  [§19.6.3 primary_conninfo](https://www.postgresql.org/docs/17/runtime-config-replication.html#GUC-PRIMARY-CONNINFO):
  «For replication slot synchronization it is also necessary to specify a
  valid dbname in the primary_conninfo string»).

### Дополнение (2026-10-17): synced-копии переживают рестарт; остаток после demotion — конфликт имён

Уточнение механики по исходникам PostgreSQL 17.2 (`slotsync.c`, `slot.h`) и
повторной проверке на стенде. Корректирует прежнюю атрибуцию «после рестарта
стенда флаг synced не переживает рестарт» — она была ошибочной (исправлена в
комментарии `03-slot-reconcile.sql` и AGENTS.md, коммит 79a7e22):

1. **Synced-копии персистентны и переживают рестарты штатно.** Поля `synced`
   и `failover` входят в `ReplicationSlotPersistentData` — сохраняемое на
   диск состояние слота («On-Disk data of a replication slot, preserved
   across restarts», `slot.h`). Воркер не пересоздаёт такие копии, а
   обновляет на месте (`update_local_synced_slot` /
   `update_and_persist_local_synced_slot`, `slotsync.c`). Проверено
   контролируемым рестартом pg2 на стенде: копия уцелела без изменений
   (synced=true, те же restart_lsn/confirmed_flush_lsn), воркер
   перезапустился без ошибок, репликация вернулась.
2. **Остаток после demotion — конфликт имён, а не потеря флага.** Старый
   primary возвращается standby со своим оригинальным слотом
   (synced=false: на primary флаг не выставляется — synced=true там бывает
   только «leftover from a promoted standby», что и видно на pg1). Воркер
   синхронизирует только failover-слоты, и одноимённый локальный слот с
   synced=false завершает его целиком: `exiting from slot synchronization
   because same name slot "outbox_slot" already exists on the standby`
   (`slotsync.c`) — нода не получает синхронизированных копий, пока остаток
   не удалён. Лечение — `03-slot-reconcile.sql` (drop только `NOT synced` и
   только на standby). Полный рестарт стенда порождал остатки именно через
   гонки промоушенов Patroni (вернувшийся старый primary), а не напрямую.
3. **Два «плохих» состояния не путать.** (а) synced=false-остаток
   оригинального слота вернувшейся ноды → удалить, воркер пересоздаст копию;
   (б) копия, которую нельзя безопасно привести к состоянию primary
   («remote slot precedes local slot», недоступные WAL/каталоги — основная
   часть этого раздела) → НЕ удалять: небезопасную копию воркер держит
   temporary и не делает persistent, при promotion она отбрасывается
   (отсюда процитированное выше «Only persistent slots that have attained
   synced state as true … can be used»); лечение — продвижение слота на
   primary, что работающий CDC делает непрерывно.

Актуально для HA-тестов (этап 8 по плану §20): после каждого switchover демоутнутая нода
требует reconcile до возвращения в HA-готовное состояние; в тестовых циклах
с wipe скрипт — no-op.

## 4. Привилегии PG17 на SQL-функции слота: почему SECURITY DEFINER обязателен

requirements.md 5.2: «Исследуй реальные требования PostgreSQL 17 к выполнению
функций работы с logical slots… Если функция требует более высоких привилегий,
создай безопасный минимальный privileged wrapper и задокументируй причину».

**Результат проверки на стенде (PostgreSQL 17.2).** Прямой вызов
`pg_logical_slot_peek_binary_changes()` от роли `sys_app` (NOSUPERUSER,
NOREPLICATION) завершается ошибкой:

```
ERROR:  permission denied to use replication slots
DETAIL:  Only roles with the REPLICATION attribute may use replication slots.
```

То есть SQL-интерфейс логического декодирования требует атрибут REPLICATION
(или SUPERUSER), которого у `sys_app` по требованиям 5.1 быть не должно —
повышать привилегии роли нельзя. Минимальное решение — SECURITY DEFINER-функции
владельца `postgres` (`infrastructure/postgres/init/04-cdc-polling.sql`):

- `cdc.peek_outbox_changes(p_limit)` — peek без потребления; слот
  `outbox_slot`, publication `outbox_publication`, `proto_version=1` —
  константы в теле; лимит зажат 1..10000 («ограничение размера пачки» 5.2);
  параллельно обновляет watermark `cdc.peek_state` в той же транзакции
  (watermark закоммичен ⟺ клиент получил строки);
- `cdc.advance_outbox_slot(p_target)` — `pg_replication_slot_advance` до
  позиции, не дальше watermark («запрет произвольного продвижения LSN» /
  «проверка корректности подтверждаемой позиции» — максимум достижимого на
  стороне БД: о доставке в Kafka база знать не может, что requirements.md 5.2
  прямо оговаривает);
- `cdc.outbox_slot_status()` — состояние слота + текущий WAL LSN для метрик
  WAL lag и диагностики инвалидации.

EXECUTE — только у `sys_app` (PUBLIC отозван), `search_path = pg_catalog`,
все вызовы schema-qualified. Положительные проверки под минимальной ролью
(requirements.md 5.3) — в `scripts/check-infra.sh` и
`scripts/test/smoke-cdc-polling.sh`.

**Попутно зафиксировано о версиях интерфейса слотов** (важно для портируемости;
перепроверено 2026-10-16 по официальным докам PG17/18/19/devel и на стенде
17.2): `pg_replication_slot_advance` во всех версиях возвращает `(slot_name,
end_lsn)` — выходной колонки `end_xid` не существует ни в одной официальной
версии (более ранняя редакция этого абзаца приписывала её PG18 — ошибка
памяти). Колонки `persisted` в `pg_replication_slots` тоже нет ни в одной
версии (PG18 добавил `two_phase_at`, PG19 — `slotsync_skip_reason`):
«persistent slots» в [PG17 §47.2.3](https://www.postgresql.org/docs/17/logicaldecoding-explanation.html)
— это прилагательное «не temporary» (антоним колонки `temporary`), а не колонка
представления. В PG17 в это представление добавлены `failover` и `synced` —
обе есть на стенде.

## 5. Прочее (некритическое)

- **pg_hba максимально лоялен** (по решению владельца): ограничения источника не
  относятся к задаче; аутентификация scram сохранена. Минимальные привилегии
  реализуются GRANT'ами на уровне ролей/БД (раздел 5 требований).
- **wal_level и кластерные параметры** задаются через `bootstrap.dcs.postgresql.parameters`
  (dynamic config): параметры из `CMDLINE_OPTIONS` Patroni применяются ко всем
  нодам только через DCS, member-local `postgresql.parameters` для них
  игнорируются.
- **LSN для advance = строчный LSN Commit-записи peek** (это `end_lsn`
  commit-сообщения — позиция сразу после commit-записи в WAL): сверено на
  стенде — `pg_replication_slot_advance` устанавливает `confirmed_flush_lsn`
  ровно в переданное значение, повторный peek после advance пуст (ни дублей,
  ни пропусков). Продвижение до commit_lsn (без «end») также безопасно, но
  тогда confirmed_flush отстаёт от фактически обработанной транзакции.
- **Env-префикс конфигурации .NET-сервиса CDC — `CDCPOLLING__`** (без
  подчёркивания между CDC и POLLING): провайдер EnvironmentVariables
  превращает в разделитель только двойные подчёркивания; `CDC_POLLING__X`
  дал бы секцию `CDC_POLLING`, которая не биндится к `CdcPolling` — значения
  молча терялись бы (обнаружено на пароле БД). У Writer (`WRITER__`)
  подчёркиваний в имени нет, поэтому паттерн совпадает.

## 6. Этап 5 (CDC Streaming): решения и факты Npgsql 8.0.9

Идея из плана этапа 4 была «общий бинарный парсер CdcCommon для обоих CDC».
Практика Npgsql 8.0.9 это скорректировала; все решения ниже проверены на стенде
(прогон 500×256 → 500/500 уникальных id в Kafka, confirmed_flush == gauge,
рестарт с продолжением, waiting_kafka → recovery).

**(а) Типизированный pgoutput API вместо общего бинарного парсера.** В публичном
API Npgsql 8.0.9 сырого `XLogDataMessage` для pgoutput-слотов нет (base
`ReplicationConnection` даёт только internal-обёртки; публичный путь — расширение
`StartReplication(PgOutputReplicationSlot, PgOutputReplicationOptions, ct)` →
`IAsyncEnumerable<PgOutputReplicationMessage>`). requirements.md §8 при этом
прямо требует `PgOutputReplicationSlot`. Итог: стриминг декодирует типизированными
сообщениями Npgsql (Begin/Relation/Insert/Commit), `CdcCommon.PgOutputParser`
остаётся у Polling; CdcCommon переиспользуется для `Lsn`/`Envelope`/`OutboxEvent`/
`Backoff`/`Percentiles`/`ParseTimestamp` (timestamp парсится тем же кодом, что у
Polling).

**(б) Значения колонок — только `Get<string>()`.** pgoutput proto v1 передаёт
колонки текстом, и Npgsql размечает replication-значения как `DataTypeName='text'`
(вне зависимости от реального типа колонки) — `Get<Guid>()` кидает
`InvalidCastException: Reading as 'System.Guid' is not supported for fields
having DataTypeName 'text'` (поймано на живом стенде). Поэтому id/topic/body/
created_at читаются строками и парсятся на нашей стороне. Сообщения Npgsql
переиспользуются между итерациями — значения Insert обязаны читаться до
следующего MoveNextAsync.

**(в) Подтверждаемый LSN = `TransactionEndLsn` последнего acked Commit.** Polling
двигает слот advance'ом до строчного LSN Commit-записи; стриминг подтверждает
replication feedback'ом (`SetReplicationStatus` + немедленный `SendStatusUpdate`)
`TransactionEndLsn` — позицию сразу после commit-записи. Обе точки корректны для
at-least-once (сервер возобновляет от `confirmed_flush`); разница — та же, что
между commit_lsn и end_lsn у peek (deviations.md §5). Подтверждать `WalEnd`
сообщений НЕЛЬЗЯ — это текущий конец WAL на сервере, там могут быть ещё
недоставленные транзакции. Отправка статуса: `SetReplicationStatus` — только
Interlocked-присвоение; реальная отправка — `SendStatusUpdate()` (39 байт),
таймером раз в `WalReceiverStatusInterval` (10 c) или по запросу сервера.

**(г) Батчинг в push-стриме: bounded-канал + drain-or-batch.** У peek-цикла
естественная граница батча — выборка; у стрима нет способа «спросить, есть ли
ещё» без блокировки. Поэтому receiver-цикл (стрим → декодер → канал завершённых
транзакций, ёмкость 2048) отделён от publisher-цикла (WaitToReadAsync → набор до
`Kafka.BatchSize` или до опустошения канала → публикация чанками с параллельным
dispatch → acks → подтверждение). Заполненный канал останавливает receiver в
`WriteAsync` → стрим не читается → TCP backpressure → walsender блокируется →
WAL копится без подтверждения (§10, TEST-HA-003). Побочный эффект (документируем):
при длительной недоступности Kafka сервер перестаёт слать keepalive'ы и Npgsql
может разорвать соединение по `WalReceiverTimeout` (60 c) — это безопасно:
редоставка после освобождения канала и reconnect'а.

**(д) Почему у Streaming нет watermark-таблицы (`cdc.peek_state`).** Рестарт
Polling и так восстанавливается от `confirmed_flush` — peek_state в восстановлении
не участвует; её роль — серверная валидация отдельной SQL-точки входа advance
(«target ≤ максимальному выданному peek LSN», §5.2). Слот такой границы не даёт:
peek не оставляет в нём следа, а всякий опасный target тоже > confirmed_flush.
В Streaming отдельной advance-точки нет вообще — подтверждение это эхо LSN из
потока того же replication-соединения (`TransactionEndLsn` обработанного
`CommitMessage`), и серверную проверку «read-before-confirm» в протокол не
встроить без патча PostgreSQL (запрещён). Гарантия структурная: подтверждаемое
значение берётся только из Commit'а, чьи события доставлены в Kafka (unit-тесты
+ smoke); плюс у `sys_replication` нет EXECUTE на peek/advance-функциях —
SQL-путём слот не перепрыгнуть.

**(е) Привилегии sys_replication (фактические).** Для стрима хватило: `LOGIN` +
`REPLICATION` (атрибут роли из 00-roles.sql) + `CONNECT ON DATABASE outbox`
(01-db-publication.sql). Для статуса слота — `GRANT USAGE ON SCHEMA cdc` + `GRANT
EXECUTE ON FUNCTION cdc.outbox_slot_status()` (05-cdc-streaming.sql; SECURITY
DEFINER, read-only, форма едина с Polling). Прямой доступ к `pg_replication_slots`
не используется; peek/advance стримингу не грантуются (§8: «Не используй polling
SQL-функций в этом режиме»).

## 7. Этап 6 (observability, §13.3/13.4): решения и платформенные ограничения

**(а) kafka-exporter (danielqsj) вместо JMX-exporter.** §13.3 требует только
доступность брокеров, offsets топиков и consumer lag; danielqsj/kafka-exporter
v1.8.0 даёт ровно это по обычному PLAINTEXT-протоколу без настройки самих
брокеров. JMX-агент потребовал бы менять команду JVM брокеров и тянуть jar,
не нужный требованиям; CPU/RAM брокеров покрывает контейнерный сэмплер.
Грабли версии: env `KAFKA_BROKERS` бинарником не читается (дефолт `kafka:9092`)
— брокеры задаются повторяемым флагом `--kafka.server=`.

**(б) Patroni `/metrics` не скрапим.** Patroni требует basic-auth на `/metrics`,
а `prometheus.yml` bind-mounted из репозитория и не темплейтится env'ом —
вписывать пароль в конфиг нельзя (§23 «отсутствие hardcoded secrets»), а
городить entrypoint-обёртку с sed ради неребованных §13.3 метрик не стали.
Состояние кластера HA-скрипты (этап 8) берут напрямую из Patroni REST API
(порты 8008-8010 опубликованы, `PATRONI_API_PASSWORD` в env скриптов);
DCS-здоровье уже покрывает job `etcd`.

**(в) Кастомные запросы postgres_exporter + guard `NOT pg_is_in_recovery()`.**
Встроенных коллекторов v0.15.0 не хватает (нет WAL retention per slot), а
`stat_bgwriter` несовместим с PG17 (колонки переехали в `pg_stat_checkpointer`)
и `database` требует CONNECT на все БД — оба отключены, объём pg_wal и слоты
дают кастомные запросы `postgres-exporter-queries.yaml`. Primary-only запросы
фильтруются `NOT pg_is_in_recovery()`: HAProxy ведёт к primary, но в окне
failover соединение может попасть на реплику, где `pg_current_wal_lsn()` падает
ошибкой и ронял бы весь scrape. Роль `sys_monitor`: `pg_monitor` (встроенная,
не SUPERUSER) + CONNECT на `outbox`; peek/advance/REPLICATION не грантуются —
мониторинг не может потреблять или двигать слот.

**(г) cAdvisor на Docker Desktop не видит контейнеры — per-container CPU/RAM
сэмплером docker stats.** У Docker Desktop включён containerd image store
(`docker info`: driver=overlayfs, io.containerd.snapshotter.v1; каталога
`/var/lib/docker/image/...` не существует). cAdvisor (проверено v0.49.1 и
v0.51.0) падает на каждом контейнере «failed to identify the read-write layer
ID» и экспортирует только VM-root (`id="/"` — общий CPU/RAM стенда, оставлен
как baseline для A/B). Отключение containerd-snapshotter — host-настройка
Docker Desktop, затрагивающая другие проекты владельца (airo-host) — не
делаем. Per-container CPU/RAM для §16 собирает `scripts/reports/
sample-docker-stats.sh` (docker stats → CSV) и точечный срез в result.json
коллектора. Дополнительно: docker.sock монтируется в cadvisor отдельной
rw-монтировкой (connect(2) к unix-сокету требует write на inode — через
общий `/var/run:ro` docker-factory не регистрируется).

**(д) Артефакты результатов в `results/raw/` (gitignored).** §13.4 требует
сохранять JSON+CSV после каждого теста, но не размещения их в VCS: сырые
временные ряды повторяемых benchmark-циклов — крупные артефакты; каталог
`results/raw/` уже был в .gitignore. Итоговые цифры этап 10 переносит в
`docs/comparison.md`.

**(е) Грабля PG16+, пойманная на sys_monitor (для будущих ролей).** Опция
`inherit` членства записывается в `pg_auth_members` в момент `GRANT` по
атрибуту роли на тот момент; последующий `ALTER ROLE ... INHERIT` её не
обновляет. Роль, созданная NOINHERIT (паттерн прикладных ролей 00-roles.sql),
получает членство `inherit=false` — привилегии `pg_monitor` не действуют при
логине: `permission denied for function pg_ls_waldir`, колонки
`pg_stat_replication` (state/sync_state/replay_lsn) маскируются в NULL.
Лечение — `GRANT pg_monitor TO sys_monitor WITH INHERIT TRUE` (06-monitoring.sql);
check-infra проверяет membership регрессионно.

**(ж) Счётчики итогов — из фактических данных, не из Prometheus.** §13.4:
«Prometheus scrape не гарантирует сохранения всех кратковременных событий».
Коллектор (`scripts/reports/collect-results.sh`) берёт Writer `/api/generation/
results|results/ids`, CDC `/api/cdc/state`, слот из БД и Kafka consume по
`body.runId` (total/unique/duplicates/missing через set-diff с writtenIds);
Prometheus (`query_range` по окну прогона, шаг 5 c) — только для CSV временных
рядов. Версии: PG/patroni/kafka/dotnet-runtimes из контейнеров + образы
(`docker compose images`); dotnet в runtime-образе — `--list-runtimes`
(`--version` требует SDK, которого нет).

## 8. Этап 7 (KafkaValidator, §15): решения и осознанные компромиссы

**(а) Группа `kafka-validator` не коммитит оффсеты — consumer lag для неё
не возникает.** Консюмер KafkaFlow настроен `WithAutoOffsetReset(Earliest)` +
`WithoutStoringOffsets()` (в 4.2.0 это `NullOffsetCommitter`/`NullOffsetManager`
— проверено по исходнику тега 4.2.0): каждый старт процесса перематывает topic
с начала и полностью пересчитывает счётчики всех прогонов. Это осознанный
дизайн ради корректности валидации: закоммиченные оффсеты после рестарта
исключили бы уже прочитанную историю из поля зрения валидатора — и любой
прогон, состоявшийся до рестарта, ложно отчитался бы как «полная потеря».
Побочный эффект: `kafka_consumergroup_lag` существует только для групп с
закоммиченными оффсетами, поэтому для этой группы серии нет. §13.3 формулирует
consumer lag как «если применимо» — считаем неприменимым и фиксируем здесь
(строка в таблице §13.3 README обновлена). Доступность Kafka закрывают
`kafka_brokers` и offsets-метрики kafka-exporter; «догоняет» ли валидатор
прогон, видно poll'ом `GET /api/validation/runs/{runId}` (так ждёт harness).

**(б) Форма integration-тестов: живой bash-харнесс + контрактные xunit-тесты,
а не standalone .NET-раннер на стенде.** §18 планирует каталог `tests/Integration/`;
он создан, но как контрактные тесты формата (Writer.MessageFactory →
CdcCommon.Envelope.Serialize → Envelope.TryParse → VerdictComputer; JSON-контракт
RunIdsDto ↔ валидатора) — весь путь сообщения без Kafka. Живую же интеграцию
(запись/чтение/доставка/количество/дубликаты, §20 этап 7) проверяет
`scripts/test/integration-delivery.sh` в конвенции smoke-скриптов проекта:
локальный dotnet — v10 (тесты ходят в контейнере sdk:8.0 вне docker-сети
стенда), а stand-проверки в проекте всегда были скриптовыми (этапы 3–6).
§18 разрешает менять структуру при сохранении разделения ответственностей.

**(в) Полное состояние валидатора — в памяти процесса, без вытеснения runId.**
Стенд исследовательский, benchmark-циклы (этап 9) разделены полным wipe, объём
topic за цикл — порядка десятков тысяч записей; полная перемотка на старте
занимает доли секунды (8787 записей/32 прогона — ~0.1 с). Сброс состояния —
рестарт контейнера (= перемотка), что проверяется тестом T3 харнесса.

**(г) Семантика вердикта: неопределённый коммит Writer ≠ потеря CDC.** §15:
«Учитывай, что при потере соединения во время commit Writer может не знать,
была ли транзакция зафиксирована. Отделяй неопределённые результаты записи от
доказанных потерь CDC». Реализация: `missingWrittenIdsCount` считает потерей
только written-ид (доказанный commit), ambiguous-коммиты разложены в
`ambiguousDeliveredCount`/`ambiguousNotDeliveredCount` и вердикт не фейлят;
дубликаты (records − uniqueIds) допустимы (at-least-once, §10) и тоже не фейлят.
Фейлят `missingWrittenIdsCount > 0` и `unexpectedExtraIdsCount > 0` — последнее
включает доставленный failed-id (доказанный незафиксированный commit не мог
попасть в WAL → запись в Kafka с таким id — аномалия).

## 9. Этап 8 (fault injection, §14 TEST-HA-001..008 + §12.2 WAL Pressure): решения и платформенные грабли

**(а) ГЛАВНОЕ ОТКРЫТИЕ: после смены топологии Patroni логический декодинг на
новом primary блокируется устаревшим `synchronized_standby_slots` (обе армы).**
Механика PG17 failover-слотов: logical decoding ожидает подтверждения flush
от ВСЕХ физических standby, перечисленных в `synchronized_standby_slots`,
прежде чем отдать данные консюмеру. На новом primary после failover/switchover
этот список унаследован от старой топологии и содержит слот умершей ноды —
декодинг (и `pg_logical_slot_peek_binary_changes` у Polling, и стриминг у
Npgsql) молча висит: walsender в состоянии `catchup`, в логах повторяется
`WARNING: replication slot "pgN" ... does not have active_pid / Logical
replication is waiting on the standby`. Наблюдалось на живом стенде: после
kill primary доставка «возобновилась» (state=running) но не двигалась, пока
список не обновили. Лечение — `ALTER SYSTEM SET synchronized_standby_slots`
по фактическим активным физическим слотам + `pg_reload_conf()` (то же делает
`02-slot.sql` при `apply-bootstrap.sh`; харнесс этапа 8 делает это сразу после
смены лидера — `refresh_sync_slots()` в `scripts/test/ha-common.sh`).
Следствие для отчёта: downtime streaming/polling при failover =
выборы лидера + переподключение + обновление `synchronized_standby_slots`
(без последнего компонента «восстановление» не завершается никогда).

**(б) `docker compose kill` НЕ перезапускает контейнер.** Docker трактует
API-kill как ручную остановку — рестарт-политика `unless-stopped` не
применяется, контейнер остаётся `Exited(137)` неограниченно долго. Поэтому
харнесс после выборов нового лидера явно делает `docker compose start` убитой
ноды (моделирует автоперезапуск узла; crash-семантика PostgreSQL сохраняется —
SIGKILL без graceful shutdown, при старте выполняется crash recovery).

**(в) Recovery time: внешние и внутренние метрики (и почему «застой-замер»
извне невозможен).** Первичная версия харнесса мерила recovery как «от инъекции
до первого роста messagesDelivered после застоя ≥3с». На практике у
CdcStreaming publisher продолжает разгружать буфер канала (ёмкость канала
больше секундной партии), пока приёмник переподключается — наблюдаемого
«застоя» доставки может не быть вовсе; а после завершения прогона delivery
естественно останавливается и неотличим от «застоя из-за отказа». Поэтому
официальные метрики этапа 8: (1) время смены лидера Patroni (поллинг REST
`/cluster`), (2) собственная метрика движка `lastRecoveryMs`
(waiting_database/waiting_kafka → running), обе — в `ha-report.json`. Для
HA-003 (старт Kafka после полного outage) внешний замер корректен (за время
outage доставка нулевая) и используется.

**(г) HA-005: точки рестарта «до delivery»/«внутри batch»/«после delivery до
ack».** R1 («до delivery») детерминирован: Kafka остановлена, сообщения
прочитаны, но не опубликованы — рестарт именно в этой точке. Под-случаи
R2/R3 недетерминируемы извне (окно «deliver→ack» — миллисекунды): рестарт
выполняется на +15с/+20с rate-limited прогона и попадает в произвольную точку
конвейера, что покрывает все три под-случая статистически; корректность
доказывается вердиктом (missing=0), дубликаты считаются и репортятся.

**(д) HA-006: «частичный сбой батча» смоделирован crash'ем одного брокера
(`docker compose kill kafka3`) в момент публикации.** Точно частичный сбой
(часть партий батча ack'нута, часть нет) недетерминируем извне: при
`acks=all`, idempotence и внутренних ретраиах продюсера KafkaFlow ошибки
могут вообще не выйти на уровень сервиса (что фиксируется как наблюдение,
не фейл). Существенное проверяемое свойство — недоставленное переотправляется,
LSN не двигается мимо недоставленного (missing=0), дубликаты допустимы.

**(е) HA-008: два честных исхода.** При стоящей Kafka слот не потребляется и
перестаёт синхронизироваться (deviations 3), поэтому PG-failover в этот момент
может закончится либо (А) выживанием synced-копии слота → доставка
возобновляется без потерь, либо (Б) потерей слота → CDC обязан уйти в
`error_slot`, не заявляя доставку; вердикт fail с missing=N фиксируется как
задокументированный случай «восстановление без потерь невозможно» (§14 прямо
требует такие случаи фиксировать), а не как сбой сервиса. Харнесс автоматически
определяет ветку и восстанавливает стенд операторскими действиями.

**(ж) Восстановление слота после инвалидации/потери — явное операторское
действие, не автоматика сервиса.** Автоматическое пересоздание слота
с пропуском сообщений запрещено (§12.3); сервисы его и не делают
(`error_slot` терминален, лечится только рестартом после пересоздания слота
оператором). Харнесс этапа 8 (reconcile в ha-common.sh — HA-008 ветка потери
и самолечение в preflight) выполняет: drop инвалидированного слота →
`apply-bootstrap.sh` (пересоздание с ТЕКУЩЕГО LSN — потеря зафиксирована
в отчётах явной валидацией) → рестарт CDC → canary-прогон с вердиктом.

**(и) Дубликаты после failover-тестов — ожидаемое окно at-least-once.** После
переподключения обе армы перечитывают с последнего ПЕРСИСТЕНТНОГО
`confirmed_flush_lsn` слота (in-memory подтверждение движка на старой ноде
умерло вместе с соединением), поэтому вердикты failover-тестов содержат
duplicateRecords 50–100 на прогон 1200 (батчи размером 50, не подтверждённые
в слот до разрыва). Вердикт pass это допускает (§10); в безотказных
integration-прогонах этапа 7 дубликатов 0.

**(к) ОТКРЫТИЕ: graceful switchover Patroni ВИСНЕТ при живом консюмере
логического слота (критично для Streaming, не возникает у Polling).**
Механика: при управляемом switchover Patroni делает graceful demote —
`pg_ctl stop -m fast`. Fast shutdown прибивает обычные бекенды, но постмастер
ждёт завершения walsender'ов; логический walsender в START_REPLICATION жив,
пока консюмер держит replication-соединение. CdcStreaming держит его постоянно
(это его штатный режим) → shutdown checkpoint не выполняется → demote не
завершается → старый лидер держит lock в состоянии stopping и отвергает
записи («the database system is shutting down»). Наблюдались 5+ минут
блокировки (снята рестартом cdc-streaming — соединение разорвалось, demote
завершился за секунды, кандидат промоутнулся). CdcPolling не держит
постоянных соединений (peek — короткие SQL-сессии) — дедлок для него
структурно невозможен. Следствия: (1) харнесс HA-002 после 60с без смены
лидера рестартует streaming-CDC как операторское лечение (фиксируется в
отчёте); (2) для сравнительного отчёта — управляемое переключение при
стриминговом CDC требует разрыва replication-соединения (рестарт/стоп CDC),
у поллинга переключение «само» завершается; (3) crash-failover (HA-001)
этой проблемы не касается — SIGKILL не проходит graceful shutdown.

**(л) Failed-батчи Writer во время застрявшего demote — не потеря.** Пока
старый лидер в stopping, соединения отвергаются; часть батчей Writer
классифицирует как failed (не зафиксированы → в WAL не попадали → в Kafka их
быть не может). Вердикт это корректно отражает: expected.failed исключён из
known-набора, missing считается только по written (§15). В прогоне HA-002 с
блокировкой: written=450, failed=100, unique=450, missing=0 — pass.

**(н) ДЕФЕКТ СТЕКА (воспроизведён и изолирован на живом стенде, этап 8):
нативный SIGSEGV (exit 139) KafkaFlow 4.2.0 — и продюсера (оба CDC), и
консьюмера (kafka-validator) — при ПОЛНОЙ потере Kafka-брокеров.** Условия
воспроизведения: продюсер — хотя бы один успешный Produce +
`docker compose stop kafka1-3`; консюмер — недоступность брокеров для группы,
уже подключавшейся (валидатор уходил в рестарт-цикл до возврата брокеров,
RestartCount 80+). Процесс умирает без единого управляемого исключения
(нативный крах, CLR crash-report молчит), рестарт-политика возвращает
контейнер (~5-10с цикл), после возврата брокеров сервис штатно
восстанавливается: CDC дочитывает слот и доставляет всё (at-least-once
подтверждён вердиктами HA-003/005/008 первого прогона: pass при краш-лупах),
валидатор перематывает topic и восстанавливает счётчики (наблюдено:
/ready=200 через ~40с после старта брокеров). Исключено в экспериментах:
версия Confluent.Kafka (транзитивная 2.8.0 и явный пин 2.16.0 — крашится
одинаково; пин откачён, стенд на 2.8.0 как в этапах 3-7), EnableIdempotence
(отключение не помогает; возвращён), arm (polling и streaming — одинаково),
архитектура (linux/arm64 нативно, без эмуляции), OOM (нет). Остаётся связка
KafkaFlow 4.2.0 (последняя стабильная) с обвязкой Confluent.Kafka в пути
потери брокеров. Последствия для тестов этапа 8 учтены в харнессах: при
полном отключении Kafka состояние waiting_kafka живого процесса недостижимо —
гарантии §12.1/§14 проверяются фактами со стороны PostgreSQL (замороженный
confirmed_flush_lsn, рост удерживаемого WAL) и валидатором после
восстановления; рестарты контейнеров фиксируются как наблюдение
(`cdcRestarts` в ha-report.json). Для эксплуатационных рекомендаций в отчёте:
полный outage Kafka переводит ВСЕ KafkaFlow-сервисы стенда в рестарт-циклы —
наблюдаемое поведение стека, не различающее arm.

**(о) ГРАБЛИ этапа 8: haproxy резолвит имена pg-нод один раз при старте —
перезапуски pg-контейнеров меняют их IP в docker-сети.** Docker освобождает
IP контейнера при остановке; kill/start циклы fault-тестов приводят к новым
адресам нод, а haproxy (без секции `resolvers`) продолжает проверять
здоровье и маршрутизировать на старые адреса: все бекенды молча уходят в
DOWN по Layer4 timeout (логируется только переход), соединения к primary
через 5432 обрываются — Writer/CDC/psql-хелперсы получают «server closed the
connection unexpectedly» при живом и здоровом кластере. Наблюдалось после
серии HA-001/HA-002 (switchover + kill/start). Лечение — динамический
ре-резолв через встроенный Docker DNS (127.0.0.11): в
infrastructure/haproxy/haproxy.cfg добавлены секция `resolvers docker` и
`default-server ... resolvers docker resolve-prefer ipv4 init-addr
last,libc,none`; применяется `docker compose restart haproxy` (bind-mount:
up -d не видит изменения содержимого файла). ГРАБЛИ конфига: haproxy не
поддерживает backslash-перенос строк — `default-server` обязан быть одной
строкой, иначе парсер трактует продолжение как новый keyword. Это стойкая
инфраструктурная правка (не патч Patroni): конфигурация балансировщика —
часть стенда.

**(п) WAL Pressure Test (§12.2) исключён решением владельца (этап 8).** После
шести итераций харнесса детерминированная инвалидизация слота ограничением
`max_slot_wal_keep_size` на этом стенде признана недостижимой разумными
средствами, ценность — низкой; скрипт удалён, стенд возвращён к `=-1` полным
wipe. Для сравнительного отчёта остались два добытых факта (воспроизводимы
вручную): (1) `SHOW max_slot_wal_keep_size` меняется sighup-ом, но механизм
инвалидации слота использует значение, зафиксированное ПРИ СТАРТЕ postgres —
лимит реально действует только после перезапуска primary (Patroni
`POST /restart`); «призрачный» эффективный лимит в `safe_wal_size` равен
значению на момент последнего старта. (2) Инвалидация наступает не от
превышения лимита удержания как такового, а при РЕАЛЬНОМ рецикле WAL-сегментов
на checkpoint — то есть требует, чтобы суммарный объём записи отодвинул конец
WAL за окно `max_wal_size` (на стенде с `max_wal_size=1GB` — сотни MB
нагрузки), что и объясняет оговорку §12.2 «не гарантирует инвалидизацию».
Поведение CDC при свершившейся инвалидации проверено на живом стенде вручную
(слот доведён до `wal_status=lost` при остановленной Kafka): следующая
инкарнация сервиса корректно уходит в терминальный `error_slot`, `/health=200`
при `/ready=503`, доставка потерянных нигде не заявлялась; операторское
восстановление (drop слота + apply-bootstrap + рестарт CDC + canary) отработано
reconcile-харнессом.

## 10. Этап 9 (benchmark, §11): методология и ограничения измерений

**(а) Одиночный прогон на сценарий, сравнительный (не абсолютный) характер
чисел.** Оба цикла выполняются на одном хосте (Docker Desktop, macOS arm64)
без пиннинга CPU и изоляции от соседних процессов; числа предназначены для
сравнения Polling vs Streaming в ИДЕНТИЧНЫХ условиях (одни сценарии, один
порядок, один warmup, одинаковое инструментирование), а не как абсолютные
паспортные показатели производительности. Повторы/дисперсия не снимались —
это осознанное решение в рамках этапа; для отчёта (§16) важны порядки и
соотношения, а не сотые доли процента.

**(б) e2e-процентили CDC — rolling-окно 4096 последних сообщений, а не
пер-сценарные гистограммы.** Движки считают P50/P95/P99 по ограниченной
очереди сэмплов; для сценариев < 4096 сообщений хвост предыдущего сценария
влияет на значения. Точный пер-сценарный СРЕДНИЙ e2e считается отдельно —
дельтами счётчиков гистограммы Prometheus (`_sum`/`_count` за окно сценария);
если сценарий короче scrape-интервала Prometheus (5с), обе точки могут попасть
на один сэмпл — средний e2e недоступен (случилось с s3-burst у стриминга; у
polling сэмплы легли удачно) — это та же оговорка §13.4 про scrape.
Для s5 (8000) окно полностью покрывается сценарием — его процентили честно
пер-сценарные.

**(в) Per-container CPU/RAM — docker stats, а не cAdvisor.** cAdvisor на
Docker Desktop (containerd image store) не отдаёт per-container серии
(см. §7) — ресурсные метрики собирает `scripts/reports/sample-docker-stats.sh`
(срез 5с). Следствие: burst-сценарии (s3/s5 ~2с) короче интервала сэмплера —
их per-сценарные окна расширяются паддингом до ≥12с (фиксируется в
`resourceWindowMs`; `cpuAvg` за паддинг занижен, `cpuMax` остаётся честным),
а основным инструментом сравнения ресурсов служит цикловая агрегация за весь
блок сценариев (`resources` в `benchmark-report.json` — одинаковая работа
обеих арм).

**(г) `docker compose down` БЕЗ `--profile` не останавливает профильные
сервисы.** Обе CDC-армы живут в профилях polling/streaming: `wipe.sh` с
просто `down -v` удалял инфраструктуру, но контейнер запущенной армы выживал,
держал сеть проекта («Resource is still in use» — был виден в каждом wipe) и
после нового `up` переподключался к НОВОМУ кластеру, занимая logical slot
(проявилось на этапе 9: выживший cdc-streaming держал слот нового стенда,
cdc-polling получал «replication slot is active for PID»). `wipe.sh` исправлен
на `down -v` с ОБЕИМИ `--profile`. Отдельно: быстрые подряд идущие
pgbouncer-подключения в первые минуты свежего стенда ловят кэшированную
ошибку «server login has been failing» — самоизлечивается после готовности
primary (server_login_retry), не является отказом доставки.

**(д) Скорость доставки CDC измеряется внешним сэмплером `/api/cdc/state`
(1с), end-of-run сверка — валидатором.** Точность avg/peak — 1с (пик —
максимум доставки в скользящую секунду); `drainMs` = Writer finished → все
id подтверждены валидатором (разрешение поллинга валидатора 1с). Для
steady-сценариев, где CDC идёт в темпе записи, `deliveryTotalMs` ≈ времени
записи — пропускная способность конвейера видна на burst/throughput
сценариях (запись быстрее доставки не бывает: CDC подтверждает слот только
после Kafka-ack, writer-темп ограничен суммарной нагрузкой на PG).

**(е) s5 (max throughput): объём 400000 подобран под медленную арму; гипотеза
«валидатор — узкое место» была ошибочной и опровергнута.** Первый вариант
s5 (1M без rate-limit) уложил Writer'а за ~16с (67k msg/s), но прогон
упёрся в 15-минутный бюджет ожидания подтверждения: изначально виновником
был объявлен валидатор («~700 подтв/с» — по темпу 649k/900с). Повтор на
400k опроверг это: в цикле B валидатор подтвердил все 400000 за 37с
(≥10.8k подтв/с) — те самые «~720/с» были ФАКТИЧЕСКОЙ устойчивой скоростью
дрейна большого backlog армой polling (цикл A: 400k за 425с ≈ 930 msg/s,
пик сэмплера 5.3k). Итоговые факты для отчёта: sustained backlog-drain
polling ≈ 0.9k msg/s, streaming ≈ 11k msg/s (~12x) при равных steady-режимах
(50/200 msg/s обе армы идут в темпе записи, e2e mean 267-279мс у polling
против 17-46мс у streaming); при 400k-задержке streaming удерживал до
2.2ГБ памяти (буфер канала + очередь producer'а) против 225МБ у polling.
Побочный артефакт прогона с живым потреблением: снапшот вердикта в момент,
когда консюмер валидатора ещё читает, снимает счётчики неатомарно —
возможны бессмысленные отрицательные «дубликаты» (run 4: −200 при
unique>total); при спокойном подтверждении (после догрузки) не возникает.

**(ж) Длительности сценариев (требование владельца).** Постоянная нагрузка
s1/s2/s4 — 10/5/5 минут (30000@50/с, 60000@200/с, 15000@50/с×4КБ); короткий
прогон этапа 9-v1 (16-41с steady, 2с burst) признан владельцем
недостаточным для установившегося режима — результаты v1 сохранены в
истории git (коммит 4a8e637), эталонные — v2 (s5=400000). Короткий по
смыслу остаётся только s3 (реакция на всплеск). Бюджеты харнесса подняты
соответственно: ожидание Writer 8→20 минут, ожидание подтверждения
валидатора 15→30 минут, таймауты validate/ids-fetch 30→120с/8→90с.

**(з) Чувствительность polling к размеру peek-пачки (однофакторный
эксперимент владельца, batch 100→1000).** Гипотеза «~930 msg/s устойчивого
дрейна объясняются размером пачки» подтверждена экспериментом
(`CDC_POLLING_POLL_BATCH_SIZE=1000`, только s3/s5, чистый стенд, артефакты
`results/raw/20261018-091008-benchmark-polling/`): дрейн 400k-бэклога
ускорился с 425с до 114с — **930 → ~3400 msg/s overall (3.7x, пик
7648)**, s3-всплеск 2147→1128мс. Нюансы: (1) `upto_nchanges` считает СЫРЫЕ
changes, а сообщение = INSERT+DELETE → фактическая пачка ~460 сообщений,
не 1000 (для ~1000 сообщений нужен лимит 2000); (2) рост сублинейен: peek
подорожал 129→151мс, публикация 9→39мс — фиксированная цена цикла
(peek SQL + Kafka RT + advance RT) амортизируется, но не исчезает;
структурный потолок последовательного peek→publish→ack→advance остаётся
(стриминг на тех же данных ~10.9k msg/s без per-батчевого SQL-цикла).
Дефолт конфигурации стенда оставлен 100 (базлайн v2 закоммичен);
эксперимент — доказательство механизма, не смена режима сравнения.
