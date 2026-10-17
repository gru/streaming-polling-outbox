using CdcCommon;

namespace KafkaValidator.Validation;

/// <summary>
/// Реестр прогонов: живой индекс содержимого topic по body.runId
/// (requirements.md 15 — счётчики из фактических данных, не из Prometheus).
/// Хранится в памяти процесса без вытеснения: стенд исследовательский,
/// benchmark-циклы разделены полным wipe; «ресет» = рестарт контейнера,
/// который перематывает topic с начала и восстанавливает счётчики полностью.
/// </summary>
public sealed class RunRegistry
{
    private readonly object _sync = new();
    private readonly Dictionary<Guid, RunTallies> _runs = new();

    public long RecordsTotal { get; private set; }

    /// <summary>Валидные записи без body.runId (ручные INSERT и т.п.) — в прогоны не попадают.</summary>
    public long SkippedNoRunId { get; private set; }

    /// <summary>Записи с структурно битым конвертом (Envelope.TryParse = false).</summary>
    public long ParseErrors { get; private set; }

    public DateTimeOffset? LastRecordUtc { get; private set; }

    public void Record(ConsumedMessage message, Guid? runId, DateTimeOffset nowUtc)
    {
        lock (_sync)
        {
            RecordsTotal++;
            LastRecordUtc = nowUtc;
            if (runId is null)
            {
                SkippedNoRunId++;
                return;
            }
            if (_runs.TryGetValue(runId.Value, out var tallies) is false)
            {
                tallies = new RunTallies(runId.Value);
                _runs[runId.Value] = tallies;
            }
            tallies.Record(message.Id, nowUtc);
        }
    }

    public void RecordParseError()
    {
        lock (_sync)
        {
            RecordsTotal++;
            ParseErrors++;
        }
    }

    public RunSummary? GetRun(Guid runId)
    {
        lock (_sync)
            return _runs.TryGetValue(runId, out var t) ? t.ToSummary() : null;
    }

    public RunDetail? GetRunDetail(Guid runId)
    {
        lock (_sync)
            return _runs.TryGetValue(runId, out var t) ? t.ToDetail() : null;
    }

    /// <summary>Список прогонов в порядке появления в topic.</summary>
    public IReadOnlyList<RunSummary> ListRuns()
    {
        lock (_sync)
            return _runs.Values.OrderBy(t => t.FirstSeenUtc).ThenBy(t => t.RunId).Select(t => t.ToSummary()).ToList();
    }

    public int RunsSeen
    {
        get
        {
            lock (_sync)
                return _runs.Count;
        }
    }
}
