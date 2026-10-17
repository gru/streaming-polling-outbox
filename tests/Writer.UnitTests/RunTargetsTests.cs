using Writer.Generation;

using Xunit;

namespace Writer.UnitTests;

public class RunTargetsTests
{
    private static LoadProfile Profile(
        int? totalMessages = null, long? totalBytes = null, int? durationSec = null,
        bool runUntilStopped = false, int batchSize = 100) => new()
    {
        RunId = Guid.NewGuid(),
        ConnectionMode = "pgbouncer",
        MessageSizeBytes = 1000,
        RatePerSecond = null,
        Parallelism = 4,
        BatchSize = batchSize,
        TotalMessages = totalMessages,
        TotalPayloadBytes = totalBytes,
        DurationSeconds = durationSec,
        RunUntilStopped = runUntilStopped,
    };

    private static readonly DateTimeOffset Start = new(2026, 10, 16, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void StopDispatch_CountTarget()
    {
        var p = Profile(totalMessages: 500);
        Assert.False(RunTargets.StopDispatch(p, 499, 0, Start.AddSeconds(1), Start));
        Assert.True(RunTargets.StopDispatch(p, 500, 0, Start.AddSeconds(1), Start));
    }

    [Fact]
    public void StopDispatch_VolumeTarget()
    {
        var p = Profile(totalBytes: 10_000_000);
        Assert.False(RunTargets.StopDispatch(p, 0, 9_999_999, Start.AddSeconds(1), Start));
        Assert.True(RunTargets.StopDispatch(p, 0, 10_000_000, Start.AddSeconds(1), Start));
    }

    [Fact]
    public void StopDispatch_DurationTarget()
    {
        var p = Profile(durationSec: 60);
        Assert.False(RunTargets.StopDispatch(p, 1000, 0, Start.AddSeconds(59), Start));
        Assert.True(RunTargets.StopDispatch(p, 1000, 0, Start.AddSeconds(60), Start));
    }

    [Fact]
    public void StopDispatch_RunUntilStopped_NeverStopsByItself()
    {
        var p = Profile(runUntilStopped: true);
        Assert.False(RunTargets.StopDispatch(p, 10_000_000, long.MaxValue / 2, Start.AddHours(1), Start));
    }

    [Fact]
    public void NextBatchSize_ClampedByRemainingMessages()
    {
        var p = Profile(totalMessages: 120, batchSize: 100);
        Assert.Equal(100, RunTargets.NextBatchSize(p, 0, 0, 100));
        Assert.Equal(20, RunTargets.NextBatchSize(p, 100, 0, 100));
        Assert.Equal(0, RunTargets.NextBatchSize(p, 120, 0, 100));
    }

    [Fact]
    public void NextBatchSize_ClampedByRemainingBytes()
    {
        // 1 сообщение ≈ 100 байт body; осталось 250 байт → 3 сообщения, но batch=2
        var p = Profile(totalBytes: 1250, batchSize: 2);
        Assert.Equal(2, RunTargets.NextBatchSize(p, 0, 1000, 100));
        // осталось ровно 0 — ничего не диспетчеризуем
        Assert.Equal(0, RunTargets.NextBatchSize(p, 0, 1250, 100));
    }

    [Fact]
    public void ReachedTarget_ReportsWhichTarget()
    {
        var start = Start;
        Assert.Equal("TargetMessages", RunTargets.ReachedTarget(Profile(totalMessages: 5), 5, 0, start.AddSeconds(1), start));
        Assert.Equal("TargetPayloadBytes", RunTargets.ReachedTarget(Profile(totalBytes: 100), 0, 100, start.AddSeconds(1), start));
        Assert.Equal("TargetDuration", RunTargets.ReachedTarget(Profile(durationSec: 10), 0, 0, start.AddSeconds(11), start));
        Assert.Null(RunTargets.ReachedTarget(Profile(runUntilStopped: true), 100, 100, start.AddHours(1), start));
    }
}
