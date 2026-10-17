using System.Text.Json.Serialization;

namespace KafkaValidator.Validation;

/// <summary>
/// Ожидаемый набор id прогона: источник — Writer (GET /api/generation/results/ids)
/// или явные массивы в запросе (сохранённый на время теста набор, §15).
/// </summary>
public sealed record ExpectedSet(
    Guid RunId,
    IReadOnlySet<Guid> WrittenIds,
    IReadOnlySet<Guid> AmbiguousIds,
    IReadOnlySet<Guid> FailedIds,
    string Source)
{
    public const string SourceWriter = "writer";
    public const string SourceExplicit = "explicit";
}

/// <summary>Тело POST /api/validation/validate (camelCase из JSON).</summary>
public sealed class ValidateRequest
{
    /// <summary>Идентификатор прогона (body.runId записей Writer).</summary>
    public Guid? RunId { get; set; }

    /// <summary>
    /// true — ожидаемый набор взять у Writer (/api/generation/results/ids);
    /// действует только для ПОСЛЕДНЕГО завершённого прогона (при несовпадении
    /// runId — 409). Для старых прогонов передавайте сохранённые массивы явно.
    /// </summary>
    public bool FetchFromWriter { get; set; }

    public List<Guid>? WrittenIds { get; set; }

    public List<Guid>? AmbiguousIds { get; set; }

    public List<Guid>? FailedIds { get; set; }
}

/// <summary>Ответ Writer /api/generation/results/ids (RunIdsDto, camelCase).</summary>
public sealed record WriterRunIds(
    Guid RunId,
    int WrittenCount,
    int AmbiguousCount,
    int FailedCount,
    List<Guid> WrittenIds,
    List<Guid> AmbiguousIds,
    List<Guid> FailedIds);
