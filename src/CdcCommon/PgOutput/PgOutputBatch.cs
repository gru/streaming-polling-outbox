namespace CdcCommon.PgOutput;

/// <summary>
/// Результат разбора одной peek-выборки: события (Insert public.outbox_messages
/// в закоммиченных транзакциях), водяной знак для advance и счётчики записей.
/// События транзакций без Commit в этой выборке НЕ публикуются (защита от
/// теоретически неполных батчей — peek режет выборку по границам транзакций,
/// но парсер не полагается на это).
/// </summary>
public sealed class PgOutputBatch
{
    public required IReadOnlyList<OutboxEvent> Events { get; init; }

    /// <summary>Строчный LSN последней Commit-записи в батче — до неё можно подтвердить слот.</summary>
    public required Lsn? LastCommitLsn { get; init; }

    /// <summary>Максимальный LSN среди всех строк батча (метрика «текущая полученная LSN»).</summary>
    public required Lsn? LastReceivedLsn { get; init; }

    /// <summary>Счётчики протокольных записей по типу ('B','R','I','U','D','C','T','M','Y','O','?').</summary>
    public required IReadOnlyDictionary<char, long> RecordCounts { get; init; }

    /// <summary>События транзакций, чей Commit не попал в батч (норма — 0).</summary>
    public required int IncompleteTransactionEventCount { get; init; }
}
