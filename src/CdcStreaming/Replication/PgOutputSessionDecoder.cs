using CdcCommon;
using CdcCommon.PgOutput;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;

namespace CdcStreaming.Replication;

/// <summary>Завершённая транзакция: события + LSN, который можно подтвердить после их доставки.</summary>
public sealed record FlushItem(IReadOnlyList<OutboxEvent> Events, Lsn TransactionEndLsn, uint Xid);

/// <summary>Результат обработки одного сообщения стрима: тип записи + возможно завершённая транзакция.</summary>
public sealed record DecodedMessage(char Kind, FlushItem? Completed);

/// <summary>
/// Декодер типизированных pgoutput-сообщений Npgsql (requirements.md 8). Экземпляр
/// живёт ровно одну replication-сессию: Relation-кэш и незакрытые транзакции
/// переживают границы «flush-наборов», но НЕ переподключение — новая сессия
/// начинается с нового декодера (сервер повторно пришлёт R, а транзакции,
/// чей Commit не подтверждён, — целиком).
///
/// Отличия от бинарного парсера Polling (CdcCommon.PgOutputParser): парсер
/// получает сырые байты peek-выборки и сам читает форматы, здесь Npgsql уже
/// декодировал протокол в типизированные сообщения — мы только группируем
/// транзакции (xid приходит с Begin; в proto v1 промежуточные сообщения xid
/// не несут) и собираем события из значений NewRow.
///
/// Сообщения Npgsql переиспользуются: значения Insert нужно прочитать до
/// следующего MoveNextAsync — декодер делает это сразу в ProcessAsync.
/// </summary>
public sealed class PgOutputSessionDecoder
{
    public const string OutboxSchema = "public";
    public const string OutboxTable = "outbox_messages";

    private readonly Dictionary<uint, List<PendingEvent>> _pendingByXid = new();
    private uint _currentXid;
    private OutboxColumnMap? _columnMap;

    /// <summary>События транзакций, чей Commit ещё не пришёл (диагностика; при переподключении отбрасываются).</summary>
    public int IncompleteEventCount
    {
        get
        {
            var count = 0;
            foreach (var list in _pendingByXid.Values)
                count += list.Count;
            return count;
        }
    }

    public async Task<DecodedMessage> ProcessAsync(PgOutputReplicationMessage message, CancellationToken ct)
    {
        switch (message)
        {
            case BeginMessage begin:
                // В proto v1 xid несёт только Begin (TransactionalMessage.TransactionXid
                // заполняется лишь в streaming-режиме v2+)
                _currentXid = begin.TransactionXid;
                return new DecodedMessage('B', null);

            case RelationMessage relation:
                OnRelation(relation);
                return new DecodedMessage('R', null);

            case InsertMessage insert:
                await OnInsertAsync(insert, ct).ConfigureAwait(false);
                return new DecodedMessage('I', null);

            case CommitMessage commit:
                return new DecodedMessage('C', CompleteTransaction(commit));

            // В outbox_publication не возникают (Writer делает только INSERT+DELETE);
            // структурно возможные записи пропускаем с подсчётом вида — как в Polling
            case UpdateMessage:
                return new DecodedMessage('U', null);
            case DeleteMessage:
                return new DecodedMessage('D', null);
            case TruncateMessage:
                return new DecodedMessage('T', null);
            case TypeMessage:
                return new DecodedMessage('Y', null);
            case OriginMessage:
                return new DecodedMessage('O', null);
            case LogicalDecodingMessage:
                return new DecodedMessage('M', null);
            default:
                return new DecodedMessage('?', null);
        }
    }

    private void OnRelation(RelationMessage relation)
    {
        // Перезапись при повторной посылке корректна (после DDL); relation-кэш
        // живёт сессию — в стриме R приходит один раз на отношение
        if (!IsOutboxRelation(relation))
            return; // публикация содержит только outbox_messages; чужие relations не отслеживаем
        var names = new string[relation.Columns.Count];
        for (var i = 0; i < relation.Columns.Count; i++)
            names[i] = relation.Columns[i].ColumnName;
        _columnMap = OutboxColumnMap.Create(names);
    }

    private async Task OnInsertAsync(InsertMessage insert, CancellationToken ct)
    {
        if (!IsOutboxRelation(insert.Relation))
            return; // событие не порождается; NewRow не читаем — Npgsql потребит его сам

        var map = _columnMap ?? throw new PgOutputParseException(
            $"Insert для public.outbox_messages без предшествующего Relation в сессии (xid={_currentXid})");

        // Все значения читаем как string: pgoutput proto v1 передаёт колонки текстом,
        // и Npgsql размечает их DataTypeName='text' — Get<Guid>/Get<DateTime> кидают
        // InvalidCastException. Разбор строк — тот же, что у Polling (Guid.TryParse,
        // PgOutputParser.ParseTimestamp)
        string? idText = null;
        string? topic = null;
        string? body = null;
        string? createdAtText = null;
        var missing = new List<OutboxColumn>();

        var position = 0;
        await foreach (var value in insert.NewRow.WithCancellation(ct).ConfigureAwait(false))
        {
            if (map.TryGetColumn(position, out var column))
            {
                // Null/UnchangedToastedValue → значение отсутствует (для outbox-строки фатально)
                if (value.Kind is TupleDataKind.Null or TupleDataKind.UnchangedToastedValue)
                    missing.Add(column);
                else
                {
                    var text = await value.Get<string>(ct).ConfigureAwait(false);
                    switch (column)
                    {
                        case OutboxColumn.Id: idText = text; break;
                        case OutboxColumn.Topic: topic = text; break;
                        case OutboxColumn.Body: body = text; break;
                        case OutboxColumn.CreatedAt: createdAtText = text; break;
                    }
                }
            }
            position++;
        }

        if (missing.Count > 0)
            throw new PgOutputParseException(
                $"Insert outbox-строки с NULL/unchanged-TOAST колонками: [{string.Join(", ", missing)}] (xid={_currentXid}); все колонки NOT NULL");

        var created = OutboxRowFactory.Create(idText, topic, body, createdAtText, _currentXid, Lsn.Zero);
        if (!_pendingByXid.TryGetValue(_currentXid, out var list))
            _pendingByXid[_currentXid] = list = new List<PendingEvent>();
        list.Add(new PendingEvent(created.Id, created.Topic, created.BodyJson, created.CreatedAtUtc));
    }

    private FlushItem CompleteTransaction(CommitMessage commit)
    {
        var transactionEndLsn = new Lsn((ulong)commit.TransactionEndLsn);
        var commitLsn = new Lsn((ulong)commit.CommitLsn);
        _pendingByXid.Remove(_currentXid, out var pending);

        var events = new List<OutboxEvent>(pending?.Count ?? 0);
        if (pending is not null)
            foreach (var p in pending)
                events.Add(new OutboxEvent(p.Id, p.Topic, p.BodyJson, p.CreatedAtUtc, commitLsn, _currentXid));

        // Пустые транзакции (BEGIN..COMMIT без событий) тоже возвращаем: их нечего
        // публиковать, но TransactionEndLsn нужно протолкнуть в подтверждение,
        // иначе слот не двигается на «пустых» коммитах
        return new FlushItem(events, transactionEndLsn, _currentXid);
    }

    private static bool IsOutboxRelation(RelationMessage relation) =>
        string.Equals(relation.Namespace, OutboxSchema, StringComparison.Ordinal) &&
        string.Equals(relation.RelationName, OutboxTable, StringComparison.Ordinal);

    private sealed record PendingEvent(Guid Id, string Topic, string BodyJson, DateTimeOffset CreatedAtUtc);
}
