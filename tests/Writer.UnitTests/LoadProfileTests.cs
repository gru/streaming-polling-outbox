using Writer.Configuration;
using Writer.Generation;

using Xunit;

namespace Writer.UnitTests;

public class LoadProfileTests
{
    private static WriterOptions Options() => new()
    {
        Connection = new() { Mode = "pgbouncer" },
        Defaults = new()
        {
            MessageSizeBytes = 1024,
            Parallelism = 4,
            BatchSize = 100,
        },
    };

    [Fact]
    public void Resolve_WithoutRequest_UsesDefaultsAndRequiresTarget()
    {
        var errors = new List<string>();
        var p = LoadProfile.Resolve(Options(), null, errors);

        Assert.Null(p); // ни цели, ни runUntilStopped
        Assert.Contains(errors, e => e.Contains("цель завершения"));
    }

    [Fact]
    public void Resolve_DefaultsPlusRunUntilStopped_IsValid()
    {
        var errors = new List<string>();
        var p = LoadProfile.Resolve(Options(), new RunSpec { RunUntilStopped = true }, errors);

        Assert.NotNull(p);
        Assert.Equal("pgbouncer", p!.ConnectionMode);
        Assert.Equal(1024, p.MessageSizeBytes);
        Assert.Equal(4, p.Parallelism);
        Assert.Equal(100, p.BatchSize);
        Assert.Null(p.RatePerSecond);
        Assert.True(p.RunUntilStopped);
        Assert.Empty(errors);
    }

    [Fact]
    public void Resolve_RequestOverridesDefaults()
    {
        var errors = new List<string>();
        var p = LoadProfile.Resolve(Options(), new RunSpec
        {
            TotalMessages = 1234,
            MessageSizeBytes = 256,
            RatePerSecond = 500,
            Parallelism = 8,
            BatchSize = 64,
            ConnectionMode = "DIRECT",
            RunId = Guid.NewGuid(),
        }, errors);

        Assert.NotNull(p);
        Assert.Equal(1234, p!.TotalMessages);
        Assert.Equal(256, p.MessageSizeBytes);
        Assert.Equal(500, p.RatePerSecond);
        Assert.Equal(8, p.Parallelism);
        Assert.Equal(64, p.BatchSize);
        Assert.Equal("direct", p.ConnectionMode); // нормализация регистра
    }

    [Theory]
    [InlineData("vertical", "connectionMode")]
    [InlineData("", "connectionMode")]
    public void Resolve_InvalidMode_Fails(string mode, string fragment)
    {
        var errors = new List<string>();
        var p = LoadProfile.Resolve(Options(), new RunSpec { TotalMessages = 10, ConnectionMode = mode }, errors);
        Assert.Null(p);
        Assert.Contains(errors, e => e.Contains(fragment));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(OutboxWriter.MaxBatchSize + 1)]
    public void Resolve_BatchSizeOutOfRange_Fails(int batchSize)
    {
        var errors = new List<string>();
        var p = LoadProfile.Resolve(Options(), new RunSpec { TotalMessages = 10, BatchSize = batchSize }, errors);
        Assert.Null(p);
        Assert.Contains(errors, e => e.Contains("batchSize"));
    }

    [Fact]
    public void Resolve_MaxBatchSize_IsValid()
    {
        var errors = new List<string>();
        var p = LoadProfile.Resolve(Options(), new RunSpec { TotalMessages = 10_000, BatchSize = OutboxWriter.MaxBatchSize }, errors);
        Assert.NotNull(p);
    }

    [Fact]
    public void Resolve_OutOfRangeNumericTargets_Fail()
    {
        var errors = new List<string>();
        Assert.Null(LoadProfile.Resolve(Options(), new RunSpec { TotalMessages = 0 }, errors));
        Assert.Null(LoadProfile.Resolve(Options(), new RunSpec { TotalPayloadBytes = 0 }, errors));
        Assert.Null(LoadProfile.Resolve(Options(), new RunSpec { DurationSeconds = 0 }, errors));
        Assert.Null(LoadProfile.Resolve(Options(), new RunSpec { TotalMessages = 5, Parallelism = 0 }, errors));
        Assert.Null(LoadProfile.Resolve(Options(), new RunSpec { TotalMessages = 5, Parallelism = 65 }, errors));
        Assert.Null(LoadProfile.Resolve(Options(), new RunSpec { TotalMessages = 5, RatePerSecond = 0 }, errors));
        Assert.Null(LoadProfile.Resolve(Options(), new RunSpec { TotalMessages = 5, MessageSizeBytes = 0 }, errors));
    }
}
