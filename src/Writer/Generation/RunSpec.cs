using Writer.Configuration;

namespace Writer.Generation;

/// <summary>
/// Запрос запуска прогона (HTTP DTO для POST /api/generation/start).
/// null-поля означают «взять значение по умолчанию из конфигурации».
/// </summary>
public sealed class RunSpec
{
    public Guid? RunId { get; set; }
    public int? TotalMessages { get; set; }
    public long? TotalPayloadBytes { get; set; }
    public int? DurationSeconds { get; set; }
    public bool? RunUntilStopped { get; set; }
    public int? MessageSizeBytes { get; set; }
    public int? RatePerSecond { get; set; }
    public int? Parallelism { get; set; }
    public int? BatchSize { get; set; }
    public string? ConnectionMode { get; set; }
}

/// <summary>
/// Разрешённый профиль прогона: все поля определены и провалидированы.
/// Прогон завершается по ПЕРВОЙ достигнутой цели из заданных
/// (totalMessages / totalPayloadBytes / durationSeconds); если ничего не задано,
/// требуется RunUntilStopped — генерация до явного stop.
/// </summary>
public sealed record LoadProfile
{
    public required Guid RunId { get; init; }
    public required string ConnectionMode { get; init; }
    public required int MessageSizeBytes { get; init; }
    public required int? RatePerSecond { get; init; }
    public required int Parallelism { get; init; }
    public required int BatchSize { get; init; }
    public int? TotalMessages { get; init; }
    public long? TotalPayloadBytes { get; init; }
    public int? DurationSeconds { get; init; }
    public bool RunUntilStopped { get; init; }

    /// <summary>
    /// Сливает запрос с дефолтами конфигурации и валидирует результат.
    /// Возвращает null и заполняет errors при некорректной комбинации.
    /// </summary>
    public static LoadProfile? Resolve(WriterOptions options, RunSpec? request, List<string> errors)
    {
        var d = options.Defaults;
        var r = request;
        var profile = new LoadProfile
        {
            RunId = r?.RunId ?? Guid.NewGuid(),
            ConnectionMode = (r?.ConnectionMode ?? options.Connection.Mode ?? "").Trim().ToLowerInvariant(),
            MessageSizeBytes = r?.MessageSizeBytes ?? d.MessageSizeBytes,
            RatePerSecond = r?.RatePerSecond ?? d.RatePerSecond,
            Parallelism = r?.Parallelism ?? d.Parallelism,
            BatchSize = r?.BatchSize ?? d.BatchSize,
            TotalMessages = r?.TotalMessages ?? d.TotalMessages,
            TotalPayloadBytes = r?.TotalPayloadBytes ?? d.TotalPayloadBytes,
            DurationSeconds = r?.DurationSeconds ?? d.DurationSeconds,
            RunUntilStopped = r?.RunUntilStopped ?? d.RunUntilStopped,
        };

        if (profile.ConnectionMode is not ("pgbouncer" or "direct"))
            errors.Add($"connectionMode должен быть 'pgbouncer' или 'direct', получено '{profile.ConnectionMode}'");
        if (profile.MessageSizeBytes is < 1 or > 10_000_000)
            errors.Add($"messageSizeBytes должен быть в [1..10000000], получено {profile.MessageSizeBytes}");
        if (profile.Parallelism is < 1 or > 64)
            errors.Add($"parallelism должен быть в [1..64], получено {profile.Parallelism}");
        if (profile.BatchSize is < 1 or > OutboxWriter.MaxBatchSize)
            errors.Add($"batchSize должен быть в [1..{OutboxWriter.MaxBatchSize}] (4 параметра на строку, лимит протокола 65535), получено {profile.BatchSize}");
        if (profile.RatePerSecond is < 1)
            errors.Add($"ratePerSecond должен быть >= 1 или отсутствовать (без лимита), получено {profile.RatePerSecond}");
        if (profile.DurationSeconds is < 1)
            errors.Add($"durationSeconds должен быть >= 1, получено {profile.DurationSeconds}");
        if (profile.TotalMessages is < 1)
            errors.Add($"totalMessages должен быть >= 1, получено {profile.TotalMessages}");
        if (profile.TotalPayloadBytes is < 1)
            errors.Add($"totalPayloadBytes должен быть >= 1, получено {profile.TotalPayloadBytes}");

        if (!profile.RunUntilStopped &&
            profile.TotalMessages is null &&
            profile.TotalPayloadBytes is null &&
            profile.DurationSeconds is null)
            errors.Add("задайте хотя бы одну цель завершения (totalMessages / totalPayloadBytes / durationSeconds) или runUntilStopped=true");

        return errors.Count == 0 ? profile : null;
    }
}
