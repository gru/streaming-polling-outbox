using Writer.Generation;

using Xunit;

namespace Writer.UnitTests;

public class PercentilesTests
{
    [Fact]
    public void P50P95P99_EmptyInput_ReturnsZeros()
    {
        var (p50, p95, p99) = Percentiles.P50P95P99(Array.Empty<double>());
        Assert.Equal(0, p50);
        Assert.Equal(0, p95);
        Assert.Equal(0, p99);
    }

    [Fact]
    public void P50P95P99_NearestRankOnSorted()
    {
        // 1..100, nearest-rank: ceil(p/100*n)
        var samples = Enumerable.Range(1, 100).Select(i => (double)i).ToArray();
        var (p50, p95, p99) = Percentiles.P50P95P99(samples);
        Assert.Equal(50, p50);
        Assert.Equal(95, p95);
        Assert.Equal(99, p99);
    }

    [Fact]
    public void P50P95P99_SingleSample()
    {
        var (p50, p95, p99) = Percentiles.P50P95P99(new[] { 42.5 });
        Assert.Equal(42.5, p50);
        Assert.Equal(42.5, p95);
        Assert.Equal(42.5, p99);
    }

    [Fact]
    public void P50P95P99_UnsortedInput_IsSortedInternally()
    {
        var samples = new[] { 9.0, 1.0, 5.0, 3.0, 7.0 };
        var (p50, _, p99) = Percentiles.P50P95P99(samples);
        Assert.Equal(5, p50);
        Assert.Equal(9, p99);
    }
}
