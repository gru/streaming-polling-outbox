using System.Text.Json;
using Writer.Generation;

using Xunit;

namespace Writer.UnitTests;

public class MessageFactoryTests
{
    [Fact]
    public void Create_PayloadSizeIsExactAndBytesCounted()
    {
        var runId = Guid.NewGuid();
        var msg = MessageFactory.Create(runId, 512);

        using var doc = JsonDocument.Parse(msg.BodyJson);
        var root = doc.RootElement;
        Assert.Equal("TestEvent", root.GetProperty("eventType").GetString());
        Assert.Equal(runId.ToString(), root.GetProperty("runId").GetString());
        Assert.Equal(512, root.GetProperty("payload").GetString()!.Length);
        Assert.Equal(MessageFactory.Topic, msg.Topic); // 'outbox.events' — единственный разрешённый topic
        Assert.Equal(System.Text.Encoding.UTF8.GetByteCount(msg.BodyJson), msg.BodyBytes);
    }

    [Fact]
    public void Create_ZeroPayload_IsValidJson()
    {
        var msg = MessageFactory.Create(Guid.NewGuid(), 0);
        using var doc = JsonDocument.Parse(msg.BodyJson);
        Assert.Equal(string.Empty, doc.RootElement.GetProperty("payload").GetString());
        Assert.True(msg.BodyBytes > 0); // остаётся конверт eventType/runId
    }

    [Fact]
    public void EstimateBodyBytes_MatchesCreated()
    {
        var runId = Guid.NewGuid();
        var estimate = MessageFactory.EstimateBodyBytes(runId, 300);
        var created = MessageFactory.Create(runId, 300).BodyBytes;
        Assert.Equal(created, estimate); // длина payload фиксирована → оценка точна
    }

    [Fact]
    public void CreateBatch_SumsBytesAndCreatesCount()
    {
        var (messages, total) = MessageFactory.CreateBatch(Guid.NewGuid(), 50, 128);
        Assert.Equal(50, messages.Length);
        Assert.Equal(messages.Sum(m => (long)m.BodyBytes), total);
        Assert.Equal(50, messages.Select(m => m.Id).Distinct().Count());
    }

    [Fact]
    public void CreateUuidV7_HasVersion7AndRfc4122Variant()
    {
        var guid = MessageFactory.CreateUuidV7();
        var s = guid.ToString();
        Assert.Equal('7', s[14]);                     // version в канонической строке
        Assert.Contains(s[19], "89ab".ToCharArray()); // variant 10xx
        var bytes = guid.ToByteArray();
        Assert.Equal(0x70, bytes[7] & 0xF0);
        Assert.Equal(0x80, bytes[8] & 0xC0);
    }

    [Fact]
    public void CreateUuidV7_TimestampMonotonic()
    {
        // старшие 48 бит канонической строки — unix ms: не убывают
        var tsOf = (string s) => (Convert.ToUInt64(s[..8], 16) << 16) | Convert.ToUInt64(s[9..13], 16);
        var a = MessageFactory.CreateUuidV7().ToString();
        var b = MessageFactory.CreateUuidV7().ToString();
        Assert.True(tsOf(b) >= tsOf(a));
    }
}
