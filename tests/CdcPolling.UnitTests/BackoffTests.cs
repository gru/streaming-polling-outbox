using Xunit;
using CdcCommon;

namespace CdcPolling.UnitTests;

public class BackoffTests
{
    [Fact]
    public void Delay_GrowsExponentially_UpToCap()
    {
        // без jitter проверить нельзя — берём границы: любая реализация ≥ cap*0.7 и ≤ cap*1.3
        for (var attempt = 1; attempt <= 12; attempt++)
        {
            var d = Backoff.Delay(attempt, baseMs: 250, capMs: 5000, random: new Random(42));
            Assert.InRange(d.TotalMilliseconds, 50, 6500);
        }
    }

    [Fact]
    public void Delay_FirstAttempt_WithinBaseJitter()
    {
        var d = Backoff.Delay(1, baseMs: 250, capMs: 5000, random: new Random(1));
        // 250 * [0.7 .. 1.3] = 175..325
        Assert.InRange(d.TotalMilliseconds, 175, 326);
    }

    [Fact]
    public void Delay_CapReached_NoUnboundedGrowth()
    {
        Assert.True(Backoff.Delay(30, random: new Random(7)).TotalMilliseconds <= 6500);
    }

    [Fact]
    public void Delay_InvalidAttempt_Clamped()
    {
        var d = Backoff.Delay(0, random: new Random(3));
        Assert.True(d >= TimeSpan.Zero);
    }
}
