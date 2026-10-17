using System.Text.Json.Serialization;

namespace Writer.Generation;

/// <summary>Живой снимок состояния генерации (GET /api/generation/state).</summary>
public sealed class RunStateSnapshot
{
    public string State { get; set; } = GenerationEngine.StateIdle;
    public Guid? RunId { get; set; }
    public DateTimeOffset? StartedAt { get; set; }
    /// <summary>Прошло с момента старта текущего/последнего прогона.</summary>
    public double? ElapsedSeconds { get; set; }
    public LoadProfile? Profile { get; set; }
    public long Dispatched { get; set; }
    public long Written { get; set; }
    public long BytesWritten { get; set; }
    public long Ambiguous { get; set; }
    public long Failed { get; set; }
    public double ActualRatePerSecond { get; set; }
    public int InFlight { get; set; }
    public string? LastError { get; set; }
    public string? LastStopReason { get; set; }
}

/// <summary>Итоги завершённого прогона (GET /api/generation/results).</summary>
public sealed class RunResults
{
    public Guid RunId { get; set; }
    public string ConnectionMode { get; set; } = "";
    public LoadProfile? Profile { get; set; }

    /// <summary>completed | failed | stopped</summary>
    public string State { get; set; } = "";
    /// <summary>TargetMessages | TargetPayloadBytes | TargetDuration | UserStop | HostShutdown | FatalError</summary>
    public string StopReason { get; set; } = "";

    public DateTimeOffset StartedAt { get; set; }
    public DateTimeOffset FinishedAt { get; set; }
    public double DurationSeconds { get; set; }

    public long DispatchedMessages { get; set; }
    /// <summary>Зафиксированные commit'ом сообщения (включая доказанные ретраем, см. AlreadyCommittedDuplicates).</summary>
    public long WrittenMessages { get; set; }
    /// <summary>Неоднозначные commit'ы, разрешённые повтором (unique violation по PK).</summary>
    public long AlreadyCommittedDuplicates { get; set; }
    /// <summary>Сообщения с незавершённым исходом commit (остановка во время retry после сбоя соединения).</summary>
    public long AmbiguousUnresolved { get; set; }
    public long FailedMessages { get; set; }

    public long BytesWritten { get; set; }
    public int Batches { get; set; }
    public int Commits { get; set; }
    public int Reconnects { get; set; }

    public double CommitMeanMs { get; set; }
    public double CommitP50Ms { get; set; }
    public double CommitP95Ms { get; set; }
    public double CommitP99Ms { get; set; }
    public double BatchMeanMs { get; set; }
    public double BatchP50Ms { get; set; }
    public double BatchP95Ms { get; set; }
    public double BatchP99Ms { get; set; }

    public double ActualRatePerSecond { get; set; }
    public double ActualBytesPerSecond { get; set; }

    /// <summary>Первые 20 ошибок прогона (полный счёт — в метриках).</summary>
    public List<string> Errors { get; set; } = new();

    [JsonIgnore] public List<Guid> WrittenIds { get; } = new();
    [JsonIgnore] public List<Guid> AmbiguousIds { get; } = new();
    [JsonIgnore] public List<Guid> FailedIds { get; } = new();
}

/// <summary>Ожидаемый набор id прогона для KafkaValidator (requirements.md раздел 15).</summary>
public sealed record RunIdsDto(
    Guid RunId,
    int WrittenCount,
    int AmbiguousCount,
    int FailedCount,
    List<Guid> WrittenIds,
    List<Guid> AmbiguousIds,
    List<Guid> FailedIds);
