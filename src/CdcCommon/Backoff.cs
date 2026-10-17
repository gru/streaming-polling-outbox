namespace CdcCommon;

/// <summary>
/// Экспоненциальный backoff с jitter ±30% (тот же паттерн, что у Writer):
/// задержка растёт от base к cap, дрожание исключает синхронные штормы ретраев.
/// </summary>
public static class Backoff
{
    public static TimeSpan Delay(int attempt, double baseMs = 250.0, double capMs = 5000.0, Random? random = null)
    {
        if (attempt < 1) attempt = 1;
        var ms = Math.Min(baseMs * (1L << Math.Min(attempt - 1, 20)), capMs);
        var r = random ?? Random.Shared;
        var jittered = ms * (0.7 + 0.6 * r.NextDouble()); // ±30%
        return TimeSpan.FromMilliseconds(Math.Clamp(jittered, 50.0, capMs));
    }
}
