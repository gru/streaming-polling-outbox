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

    private static OutboxEvent SampleEvent() => new(
        Guid.Parse("018f3e6b-7c2a-7c2a-8c2a-3e6b7c2a8c2a"),
        "outbox.events",
        """{"eventType":"TestEvent","runId":"018f3e6b-1111-7111-8111-3e6b7c2a8c2a","payload":"abc"}""",
        DateTimeOffset.Parse("2026-10-16T12:34:56.789Z"),
        Lsn.Parse("0/410A290"),
        741);

    [Fact]
    public void TryParse_RoundtripOfSerialize()
    {
        var e = SampleEvent();

        var ok = Envelope.TryParse(Envelope.Serialize(e), out var parsed);

        Assert.True(ok);
        Assert.NotNull(parsed);
        Assert.Equal(e.Id, parsed!.Id);
        Assert.Equal(e.Topic, parsed.Topic);
        Assert.Equal(e.BodyJson, parsed.BodyJson);
        Assert.Equal(e.CreatedAtUtc, parsed.TimestampUtc);
        Assert.Equal(TimeSpan.Zero, parsed.TimestampUtc.Offset); // UTC
    }

    [Fact]
    public void TryParse_AcceptsTimestampWithoutMilliseconds()
    {
        // фолбэк общего парсинга: конверт из другого источника без .fff
        var json = """{"id":"018f3e6b-7c2a-7c2a-8c2a-3e6b7c2a8c2a","topic":"outbox.events","body":{"k":1},"timestamp":"2026-10-16T12:34:56Z"}""";

        var ok = Envelope.TryParse(System.Text.Encoding.UTF8.GetBytes(json), out var parsed);

        Assert.True(ok);
        Assert.Equal(DateTimeOffset.Parse("2026-10-16T12:34:56Z"), parsed!.TimestampUtc);
    }

    [Theory]
    [InlineData("""{"topic":"outbox.events","body":{},"timestamp":"2026-10-16T12:34:56.789Z"}""")]            // нет id
    [InlineData("""{"id":"not-a-guid","topic":"outbox.events","body":{},"timestamp":"2026-10-16T12:34:56.789Z"}""")] // битый id
    [InlineData("""{"id":"018f3e6b-7c2a-7c2a-8c2a-3e6b7c2a8c2a","body":{},"timestamp":"2026-10-16T12:34:56.789Z"}""")] // нет topic
    [InlineData("""{"id":"018f3e6b-7c2a-7c2a-8c2a-3e6b7c2a8c2a","topic":"outbox.events","timestamp":"2026-10-16T12:34:56.789Z"}""")] // нет body
    [InlineData("""{"id":"018f3e6b-7c2a-7c2a-8c2a-3e6b7c2a8c2a","topic":"outbox.events","body":{},"timestamp":"не дата"}""")] // битый timestamp
    [InlineData("[1,2,3]")]       // не объект
    [InlineData("")]              // пусто
    [InlineData("not json")]      // мусор
    public void TryParse_StructurallyInvalid_ReturnsFalse(string json)
    {
        Assert.False(Envelope.TryParse(System.Text.Encoding.UTF8.GetBytes(json), out var parsed));
        Assert.Null(parsed);
    }
}
