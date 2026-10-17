using KafkaValidator.Metrics;

namespace KafkaValidator.Validation;

/// <summary>
/// Оркестрация валидации: сбор ожидаемого набора (Writer или явно), снятие
/// фактических счётчиков с реестра, вычисление вердикта, обогащение снапшотом
/// CDC. Все счётчики — из фактических данных тестового прогона
/// (requirements.md 13.4/15), Prometheus не используется как источник.
/// </summary>
public sealed class ValidationService
{
    private readonly RunRegistry _registry;
    private readonly WriterClient _writer;
    private readonly CdcClient _cdc;
    private readonly KafkaValidatorMetrics _metrics;
    private readonly ILogger<ValidationService> _logger;

    public ValidationService(RunRegistry registry, WriterClient writer, CdcClient cdc,
        KafkaValidatorMetrics metrics, ILogger<ValidationService> logger)
    {
        _registry = registry;
        _writer = writer;
        _cdc = cdc;
        _metrics = metrics;
        _logger = logger;
    }

    /// <summary>
    /// Резолв ожидаемого набора: fetchFromWriter → последний прогон Writer
    /// (несовпадение runId — ошибка: набор чужого прогона), иначе — явные
    /// массивы из запроса (сохранённый на время теста набор, §15).
    /// </summary>
    public async Task<ExpectedSet?> ResolveExpectedAsync(ValidateRequest request, CancellationToken ct)
    {
        if (request.FetchFromWriter)
        {
            WriterRunIds? ids;
            try
            {
                ids = await _writer.FetchLastRunIdsAsync(ct);
            }
            catch (HttpRequestException ex)
            {
                throw new WriterUnavailableException($"Writer недоступен: {ex.Message}");
            }
            if (ids is null)
                throw new WriterUnavailableException("у Writer нет завершённого прогона (404 на /api/generation/results/ids)");
            if (ids.RunId != request.RunId)
                throw new WriterRunMismatchException(
                    $"последний прогон Writer {ids.RunId} ≠ запрошенный {request.RunId}; " +
                    "для старых прогонов передайте сохранённый набор id явно (writtenIds)");
            return ToExpectedSet(ids);
        }

        if (request.WrittenIds is null)
            return null;
        return new ExpectedSet(
            request.RunId!.Value,
            request.WrittenIds.ToHashSet(),
            (request.AmbiguousIds ?? []).ToHashSet(),
            (request.FailedIds ?? []).ToHashSet(),
            ExpectedSet.SourceExplicit);
    }

    public async Task<ValidationVerdict> ValidateAsync(ValidateRequest request, CancellationToken ct)
    {
        var expected = await ResolveExpectedAsync(request, ct)
            ?? throw new InvalidOperationException("ожидаемый набор id не задан (writtenIds или fetchFromWriter)");

        var verdict = VerdictComputer.Compute(request.RunId!.Value, _registry.GetRunDetail(request.RunId.Value), expected, DateTimeOffset.UtcNow);

        try
        {
            verdict.Cdc = await _cdc.FetchAsync(ct);
        }
        catch (Exception ex)
        {
            // снапшот CDC — обогащение, а не условие вердикта
            _logger.LogWarning(ex, "снапшот CDC недоступен для вердикта прогона {RunId}", request.RunId);
        }

        _metrics.AddValidation(verdict.Verdict == "pass");
        _logger.LogInformation(
            "валидация {RunId}: {Verdict}; kafka records={Records} unique={Unique} dupes={Dupes} missing={Missing} unexpected={Unexpected}",
            verdict.RunId, verdict.Verdict, verdict.Kafka.Records, verdict.Kafka.UniqueIds,
            verdict.Kafka.DuplicateRecords, verdict.MissingWrittenIdsCount, verdict.UnexpectedExtraIdsCount);
        return verdict;
    }

    private static ExpectedSet ToExpectedSet(WriterRunIds ids) => new(
        ids.RunId,
        (ids.WrittenIds ?? []).ToHashSet(),
        (ids.AmbiguousIds ?? []).ToHashSet(),
        (ids.FailedIds ?? []).ToHashSet(),
        ExpectedSet.SourceWriter);
}

/// <summary>Writer недоступен или не может отдать набор (нет завершённого прогона) — 502.</summary>
public sealed class WriterUnavailableException(string message) : Exception(message);

/// <summary>Последний прогон Writer — другой runId — 409 (набор чужого прогона нельзя молча подставлять).</summary>
public sealed class WriterRunMismatchException(string message) : Exception(message);
