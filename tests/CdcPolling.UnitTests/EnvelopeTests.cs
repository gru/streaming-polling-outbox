using Xunit;
using System.Text.Json;
using CdcCommon;

namespace CdcPolling.UnitTests;

public class EnvelopeTests
{
    [Fact]
    public void Serialize_ExactShape_RawBody_NoEscaping()
    {
        var e = new OutboxEvent(
            Id: Guid.Parse("018f3e6b-7c2a-7c2a-8c2a-3e6b7c2a8c2a"),
            Topic: "outbox.events",
            BodyJson: """{"eventType":"TestEvent","runId":"r-1","payload":"abc"}""",
            CreatedAtUtc: DateTimeOffset.Parse("2026-10-16T12:34:56.789Z"),
            CommitLsn: Lsn.Parse("0/410A290"),
            Xid: 741);

        var json = JsonSerializer.Deserialize<JsonElement>(Envelope.Serialize(e));

        Assert.Equal("018f3e6b-7c2a-7c2a-8c2a-3e6b7c2a8c2a", json.GetProperty("id").GetString());
        Assert.Equal("outbox.events", json.GetProperty("topic").GetString());
        Assert.Equal("TestEvent", json.GetProperty("body").GetProperty("eventType").GetString()); // body — сырой JSON, не строка
        Assert.Equal("r-1", json.GetProperty("body").GetProperty("runId").GetString());
        Assert.Equal("2026-10-16T12:34:56.789Z", json.GetProperty("timestamp").GetString());      // формат requirements.md 4.2
    }

    [Fact]
    public void Serialize_Timestamp_IsUtcTruncatedToMilliseconds()
    {
        var e = new OutboxEvent(Guid.NewGuid(), "outbox.events", "{}",
            new DateTimeOffset(2026, 10, 16, 12, 34, 56, 789, 123, TimeSpan.Zero), Lsn.Zero, 1);

        var json = JsonSerializer.Deserialize<JsonElement>(Envelope.Serialize(e));
        Assert.Equal("2026-10-16T12:34:56.789Z", json.GetProperty("timestamp").GetString());
    }

    [Fact]
    public void Serialize_BodyIsNotAString()
    {
        var e = new OutboxEvent(Guid.NewGuid(), "outbox.events", """{"k":1}""", DateTimeOffset.UtcNow, Lsn.Zero, 1);
        var json = JsonSerializer.Deserialize<JsonElement>(Envelope.Serialize(e));
        Assert.Equal(JsonValueKind.Object, json.GetProperty("body").ValueKind);
    }
}
