namespace CdcCommon.PgOutput;

/// <summary>
/// Одна строка результата peek: позиция в WAL, xid транзакции и бинарная
/// запись протокола pgoutput (первый байт data — тип записи: B/R/I/U/D/C/T/M/Y/O).
/// lsn — это write_location контекста декодирования на момент эмитта записи
/// (для Commit-записи — позиция commit-записи транзакции в WAL).
/// </summary>
public sealed record RawSlotRecord(Lsn Lsn, uint Xid, byte[] Data);
