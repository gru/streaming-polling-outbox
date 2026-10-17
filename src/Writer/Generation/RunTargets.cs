namespace Writer.Generation;

/// <summary>
/// Чистая логика целей прогона (unit-тестируемая): условия прекращения диспетчеризации
/// и размер следующего батча. Отделена от engine, чтобы правила завершения
/// (первая достигнутая цель) можно было проверить без БД.
/// </summary>
public static class RunTargets
{
    /// <summary>
    /// true — новых батчей создавать не нужно: цель диспетчеризации достигнута,
    /// пришёл stop, либо истёк durationSeconds. Объём учитывается по ДИСПЕТЧЕРИЗОВАННЫМ
    /// байтам (оценка), чтобы не превышать цель больше чем на один батч.
    /// </summary>
    public static bool StopDispatch(LoadProfile p, long dispatchedMessages, long dispatchedBytes,
        DateTimeOffset nowUtc, DateTimeOffset startedAtUtc)
    {
        if (p.TotalMessages is { } total && dispatchedMessages >= total) return true;
        if (p.TotalPayloadBytes is { } bytes && dispatchedBytes >= bytes) return true;
        if (p.DurationSeconds is { } secs && nowUtc >= startedAtUtc.AddSeconds(secs)) return true;
        return false;
    }

    /// <summary>Размер следующего батча: урезается по остатку цели count/volume.</summary>
    public static int NextBatchSize(LoadProfile p, long dispatchedMessages, long dispatchedBytes, int avgBodyBytes)
    {
        var size = p.BatchSize;
        if (p.TotalMessages is { } total)
            size = Math.Min(size, (int)Math.Min(total - dispatchedMessages, int.MaxValue));
        if (p.TotalPayloadBytes is { } bytes && avgBodyBytes > 0)
        {
            var remaining = (long)Math.Ceiling((double)(bytes - dispatchedBytes) / avgBodyBytes);
            size = Math.Min(size, (int)Math.Min(remaining, int.MaxValue));
        }
        return (int)Math.Max(size, 0);
    }

    /// <summary>Какая цель прогона достигнута (для stopReason итога).</summary>
    public static string? ReachedTarget(LoadProfile p, long writtenMessages, long writtenBytes, DateTimeOffset nowUtc, DateTimeOffset startedAtUtc) =>
        p.TotalMessages is { } total && writtenMessages >= total ? "TargetMessages"
        : p.TotalPayloadBytes is { } bytes && writtenBytes >= bytes ? "TargetPayloadBytes"
        : p.DurationSeconds is { } secs && nowUtc >= startedAtUtc.AddSeconds(secs) ? "TargetDuration"
        : null;
}
