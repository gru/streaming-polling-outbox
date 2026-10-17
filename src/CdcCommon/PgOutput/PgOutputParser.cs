using System.Globalization;
using System.Text;

namespace CdcCommon.PgOutput;

/// <summary>
/// Разбор бинарных записей pgoutput (proto_version=1) из peek-выборки.
///
/// Формат (PostgreSQL, Logical Replication Message Format): первый байт — тип
/// записи; целые — big-endian; значения колонок — текстовое представление типа
/// ('t' + int32 длина + байты; 'n' = NULL; 'u' = неизменённый TOAST).
/// Пользовательское событие = Insert-запись на public.outbox_messages; Delete
/// игнорируется (Writer делает INSERT+DELETE в одной транзакции, таблица пустая);
/// B/R/C — служебные (R кэшируется в пределах батча: каждая peek-сессия
/// декодирования объявляет relations заново).
///
/// События публикуются только для транзакций, чей Commit присутствует в батче:
/// peek возвращает выборку, выровненную по границам транзакций (upto_nchanges
/// проверяется после декодирования очередной транзакции), но парсер не
/// полагается на это и не отдаёт «хвосты» без подтверждения.
/// </summary>
public static class PgOutputParser
{
    public const string OutboxSchema = "public";
    public const string OutboxTable = "outbox_messages";

    /// <summary>Единственный разрешённый topic (CHECK в таблице; здесь — вторая линия защиты от произвольной маршрутизации).</summary>
    public const string AllowedTopic = "outbox.events";

    private const string ColumnId = "id";
    private const string ColumnTopic = "topic";
    private const string ColumnBody = "body";
    private const string ColumnCreatedAt = "created_at";

    public static PgOutputBatch Parse(IReadOnlyList<RawSlotRecord> records)
    {
        var relations = new Dictionary<int, PgOutputRelation>();
        var pendingByXid = new Dictionary<uint, List<PendingEvent>>();
        var events = new List<OutboxEvent>();
        var counts = new SortedDictionary<char, long>();
        Lsn? lastCommitLsn = null;
        Lsn? lastReceivedLsn = null;

        foreach (var rec in records)
        {
            if (lastReceivedLsn is null || rec.Lsn > lastReceivedLsn)
                lastReceivedLsn = rec.Lsn;

            if (rec.Data.Length == 0)
            {
                counts['?'] = counts.GetValueOrDefault('?') + 1;
                continue;
            }

            var kind = (char)rec.Data[0];
            counts[kind] = counts.GetValueOrDefault(kind) + 1;
            var reader = new PgOutputReader(rec.Data.AsSpan(1));

            switch (kind)
            {
                case 'B': // Begin: final_lsn, commit_ts, xid — структуру проверяем, содержимое не нужно
                    reader.ReadInt64();
                    reader.ReadInt64();
                    reader.ReadInt32();
                    break;

                case 'R':
                {
                    var relation = ParseRelation(ref reader);
                    relations[relation.RelationId] = relation;
                    break;
                }

                case 'I':
                    ParseInsert(ref reader, rec, relations, pendingByXid);
                    break;

                case 'D': // Delete: relation_id, 'K'/'O' (ключ/старый кортеж), TupleData —
                          // Writer удаляет те же строки; событие НЕ порождается
                    reader.ReadInt32();
                    reader.ReadByte();
                    reader.SkipTupleData();
                    break;

                case 'C': // Commit: commit_lsn, end_lsn, commit_ts; события транзакции получают строчный LSN commit-записи
                {
                    reader.ReadInt64();
                    reader.ReadInt64();
                    reader.ReadInt64();
                    lastCommitLsn = rec.Lsn;
                    if (pendingByXid.Remove(rec.Xid, out var committed))
                    {
                        foreach (var p in committed)
                            events.Add(new OutboxEvent(p.Id, p.Topic, p.BodyJson, p.CreatedAtUtc, rec.Lsn, rec.Xid));
                    }
                    break;
                }

                // Update/Type/Origin/Truncate/Message не возникают в outbox_publication
                // (у Writer только INSERT/DELETE); неизвестное содержимое пропускаем
                // с подсчётом — запись самодостаточна, это не ошибка.
                case 'U':
                case 'Y':
                case 'O':
                case 'T':
                case 'M':
                    break;

                default:
                    break;
            }
        }

        var incomplete = 0;
        foreach (var list in pendingByXid.Values)
            incomplete += list.Count;

        return new PgOutputBatch
        {
            Events = events,
            LastCommitLsn = lastCommitLsn,
            LastReceivedLsn = lastReceivedLsn,
            RecordCounts = counts,
            IncompleteTransactionEventCount = incomplete,
        };
    }

    private static PgOutputRelation ParseRelation(ref PgOutputReader reader)
    {
        var relationId = reader.ReadInt32();
        var schema = reader.ReadCString();
        var name = reader.ReadCString();
        var replicaIdentity = reader.ReadByte(); // 'd'/'i'/'n'/'f' — для парсинга не нужен, но присутствует в записи
        var columns = new List<PgOutputColumn>();
        var ncols = reader.ReadInt16();
        for (var i = 0; i < ncols; i++)
        {
            var flags = reader.ReadByte();
            var columnName = reader.ReadCString();
            var typeOid = unchecked((uint)reader.ReadInt32());
            var typeModifier = reader.ReadInt32();
            columns.Add(new PgOutputColumn(flags, columnName, typeOid, typeModifier));
        }
        return new PgOutputRelation(relationId, schema, name, columns);
    }

    private static void ParseInsert(ref PgOutputReader reader, RawSlotRecord rec,
        Dictionary<int, PgOutputRelation> relations, Dictionary<uint, List<PendingEvent>> pendingByXid)
    {
        var relationId = reader.ReadInt32();
        var tupleKind = reader.ReadByte();
        if (tupleKind != (byte)'N')
            throw new PgOutputParseException(
                $"Insert с неожидаемым видом кортежа '{(char)tupleKind}' (ожидался 'N' — новая строка), xid={rec.Xid}");

        if (!relations.TryGetValue(relationId, out var relation))
            throw new PgOutputParseException(
                $"Insert для relation_id={relationId} без предшествующей Relation-записи в батче (xid={rec.Xid})");

        var ncols = reader.ReadInt16();
        var values = new string?[ncols];
        for (var i = 0; i < ncols; i++)
        {
            var valueKind = reader.ReadByte();
            switch (valueKind)
            {
                case (byte)'t':
                    var len = reader.ReadInt32();
                    if (len < 0)
                        throw new PgOutputParseException($"отрицательная длина значения колонки ({len})");
                    values[i] = Encoding.UTF8.GetString(reader.ReadBytes(len));
                    break;
                case (byte)'n':
                case (byte)'u':
                    values[i] = null;
                    break;
                default:
                    throw new PgOutputParseException($"неизвестный вид значения колонки '{(char)valueKind}'");
            }
        }

        if (!relation.Is(OutboxSchema, OutboxTable))
            return; // публикация содержит только outbox_messages; чужие записи просто не считаем событиями

        string? ByName(string columnName)
        {
            for (var i = 0; i < relation.Columns.Count && i < values.Length; i++)
                if (string.Equals(relation.Columns[i].Name, columnName, StringComparison.Ordinal))
                    return values[i];
            return null;
        }

        var idText = Required(ByName(ColumnId), ColumnId, rec);
        var topic = Required(ByName(ColumnTopic), ColumnTopic, rec);
        var bodyJson = Required(ByName(ColumnBody), ColumnBody, rec);
        var createdAtText = Required(ByName(ColumnCreatedAt), ColumnCreatedAt, rec);

        if (!Guid.TryParse(idText, out var id))
            throw new PgOutputParseException($"колонка id не является UUID: '{idText}' (xid={rec.Xid})");
        if (!string.Equals(topic, AllowedTopic, StringComparison.Ordinal))
            throw new PgOutputParseException(
                $"запрещённый topic '{topic}' в outbox-строке (допустим только '{AllowedTopic}'; xid={rec.Xid})");
        var createdAt = ParseTimestamp(createdAtText, rec);

        var pending = new PendingEvent(id, topic, bodyJson, createdAt);
        if (!pendingByXid.TryGetValue(rec.Xid, out var list))
            pendingByXid[rec.Xid] = list = new List<PendingEvent>();
        list.Add(pending);
    }

    private static string Required(string? value, string column, RawSlotRecord rec)
    {
        if (value is null)
            throw new PgOutputParseException(
                $"колонка {column} пуста (NULL или unchanged-TOAST) в Insert outbox-строки (xid={rec.Xid}); все колонки NOT NULL");
        return value;
    }

    /// <summary>
    /// Текстовый формат timestamptz PG (DateStyle=ISO): "2026-10-16 12:34:56.789+00".
    /// Целочасовое смещение PG пишет как +HH — .NET ожидает +HH:MM, дописываем
    /// минуты; формат +HH:MM/+HHMM тоже поддержан. Строки без времени (без ':')
    /// не трогаем — это не timestamptz-вывод PG.
    /// </summary>
    public static DateTimeOffset ParseTimestamp(string text, RawSlotRecord? rec = null)
    {
        var s = text.Trim();
        if (s.Contains(':'))
        {
            if (s.Length >= 3 && s[^3] is '+' or '-')
                s += ":00";                        // "+03" → "+03:00"
            else if (s.Length >= 5 && s[^5] is '+' or '-' && char.IsAsciiDigit(s[^3]))
                s = s.Insert(s.Length - 2, ":");   // "+0300" → "+03:00"
        }
        if (DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dto))
            return dto.ToUniversalTime();
        throw new PgOutputParseException(
            $"не удалось разобрать timestamptz '{text}'{(rec is null ? "" : $" (xid={rec.Xid})")}");
    }

    private sealed record PendingEvent(Guid Id, string Topic, string BodyJson, DateTimeOffset CreatedAtUtc);
}
