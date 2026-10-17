namespace KafkaValidator.Validation;

/// <summary>
/// Публичная сводка прогона без полного набора id (GET /api/validation/runs).
/// Дубликаты = records − uniqueIds (at-least-once, requirements.md 10).
/// </summary>
public sealed record RunSummary(
    Guid RunId,
    long Records,
    int UniqueIds,
    long DuplicateRecords,
    DateTimeOffset FirstSeenUtc,
    DateTimeOffset LastSeenUtc);

/// <summary>
/// Полный снимок прогона с множеством id — для сверки с ожидаемым набором
/// (VerdictComputer). Отдаётся только наружу из-под lock реестра.
/// </summary>
public sealed record RunDetail(
    Guid RunId,
    long Records,
    IReadOnlySet<Guid> UniqueIds,
    DateTimeOffset FirstSeenUtc,
    DateTimeOffset LastSeenUtc);

/// <summary>
/// Накопленные счётчики одного прогона (runId) по фактическим записям Kafka.
/// Потоковая безопасность обеспечивается внешним lock RunRegistry
/// (воркер консюмера один, но реестр читают и HTTP-эндпоинты).
/// </summary>
internal sealed class RunTallies
{
    private readonly HashSet<Guid> _ids = new();

    public RunTallies(Guid runId) => RunId = runId;

    public Guid RunId { get; }

    public long Records { get; private set; }

    public DateTimeOffset FirstSeenUtc { get; private set; }

    public DateTimeOffset LastSeenUtc { get; private set; }

    public void Record(Guid messageId, DateTimeOffset nowUtc)
    {
        Records++;
        _ids.Add(messageId);
        if (Records == 1)
            FirstSeenUtc = nowUtc;
        LastSeenUtc = nowUtc;
    }

    public RunSummary ToSummary() => new(RunId, Records, _ids.Count, Records - _ids.Count, FirstSeenUtc, LastSeenUtc);

    public RunDetail ToDetail() => new(RunId, Records, _ids, FirstSeenUtc, LastSeenUtc);
}
