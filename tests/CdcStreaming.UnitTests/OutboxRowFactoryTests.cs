using CdcCommon;
using CdcCommon.PgOutput;
using CdcStreaming.Replication;
using Xunit;

namespace CdcStreaming.UnitTests;

public class OutboxRowFactoryTests
{
    private const string Id = "018f3a2b-7c4d-7e5f-8a9b-0c1d2e3f4a5b";
    // текстовый формат timestamptz, как его пишет PG (DateStyle=ISO)
    private const string CreatedAt = "2026-10-16 12:34:56.789+00";
    private static readonly Lsn Commit = new(0x16ABCDEFL);

    [Fact]
    public void Create_ValidRow_ProducesEvent()
    {
        var e = OutboxRowFactory.Create(Id, "outbox.events", """{"n":1}""", CreatedAt, 42, Commit);

        Assert.Equal(Guid.Parse(Id), e.Id);
        Assert.Equal("outbox.events", e.Topic);
        Assert.Equal("""{"n":1}""", e.BodyJson);
        Assert.Equal(new DateTimeOffset(2026, 10, 16, 12, 34, 56, 789, TimeSpan.Zero), e.CreatedAtUtc);
        Assert.Equal(42u, e.Xid);
        Assert.Equal(Commit, e.CommitLsn);
    }

    [Fact]
    public void Create_TimestampWithShortOffset_Parses()
    {
        // PG пишет целочасовое смещение как +HH — тот же разбор, что у Polling
        var e = OutboxRowFactory.Create(Id, "outbox.events", "{}", "2026-10-16 15:34:56.789+03", 1, Commit);

        Assert.Equal(new DateTimeOffset(2026, 10, 16, 12, 34, 56, 789, TimeSpan.Zero), e.CreatedAtUtc);
    }

    [Fact]
    public void Create_TimestampGarbage_Throws()
    {
        Assert.Throws<PgOutputParseException>(() =>
            OutboxRowFactory.Create(Id, "outbox.events", "{}", "не-время", 1, Commit));
    }

    [Theory]
    [InlineData(true, false, false, false)]
    [InlineData(false, true, false, false)]
    [InlineData(false, false, true, false)]
    [InlineData(false, false, false, true)]
    public void Create_MissingColumn_Throws(bool nullId, bool nullTopic, bool nullBody, bool nullCreatedAt)
    {
        Assert.Throws<PgOutputParseException>(() => OutboxRowFactory.Create(
            nullId ? null : Id,
            nullTopic ? null : "outbox.events",
            nullBody ? null : "{}",
            nullCreatedAt ? null : CreatedAt,
            42, Commit));
    }

    [Fact]
    public void Create_WrongTopic_Throws()
    {
        Assert.Throws<PgOutputParseException>(() =>
            OutboxRowFactory.Create(Id, "other.topic", "{}", CreatedAt, 42, Commit));
    }

    [Fact]
    public void Create_IdNotUuid_Throws()
    {
        Assert.Throws<PgOutputParseException>(() =>
            OutboxRowFactory.Create("не-uuid", "outbox.events", "{}", CreatedAt, 42, Commit));
    }
}
