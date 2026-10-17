namespace CdcCommon;

/// <summary>
/// Одно пользовательское сообщение outbox, извлечённое из Insert-записи pgoutput
/// (Delete-записи события не порождают — Writer делает INSERT+DELETE в одной
/// транзакции, CDC игнорирует Delete). CommitLsn — позиция Commit-записи
/// транзакции (строчный LSN из peek): до неё подтверждается слот после доставки.
/// </summary>
public sealed record OutboxEvent(
    Guid Id,
    string Topic,
    string BodyJson,
    DateTimeOffset CreatedAtUtc,
    Lsn CommitLsn,
    uint Xid);
