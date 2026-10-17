namespace Writer.Generation;

/// <summary>Персентили методом nearest-rank (ceil(p/100*n)), как в гистограммных оценках Prometheus.</summary>
public static class Percentiles
{
    public static double Rank(double[] sortedAsc, double p)
    {
        if (sortedAsc.Length == 0) return 0;
        var idx = (int)Math.Ceiling(p / 100.0 * sortedAsc.Length) - 1;
        return sortedAsc[Math.Clamp(idx, 0, sortedAsc.Length - 1)];
    }

    public static (double P50, double P95, double P99) P50P95P99(IReadOnlyList<double> samples)
    {
        if (samples.Count == 0) return (0, 0, 0);
        var sorted = new double[samples.Count];
        for (var i = 0; i < samples.Count; i++) sorted[i] = samples[i];
        Array.Sort(sorted);
        return (Rank(sorted, 50), Rank(sorted, 95), Rank(sorted, 99));
    }
}
