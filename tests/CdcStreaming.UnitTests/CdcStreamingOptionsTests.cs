using CdcStreaming.Configuration;
using Xunit;

namespace CdcStreaming.UnitTests;

public class CdcStreamingOptionsTests
{
    [Fact]
    public void Defaults_MatchStand()
    {
        var o = new CdcStreamingOptions();

        Assert.Equal("haproxy", o.Connection.Endpoint.Host);
        Assert.Equal(5432, o.Connection.Endpoint.Port);
        Assert.Equal("outbox", o.Connection.Database.Name);
        Assert.Equal("sys_replication", o.Connection.Database.Username);
        Assert.Equal("outbox_slot", o.Streaming.SlotName);
        Assert.Equal("outbox_publication", o.Streaming.Publication);
        Assert.Equal(1UL, o.Streaming.ProtocolVersion);
        Assert.Equal(2048, o.Streaming.ChannelCapacity);
        Assert.Equal(100, o.Kafka.BatchSize);
        Assert.Equal(5, o.Kafka.LingerMs);
        Assert.Equal(15_000, o.Kafka.MessageTimeoutMs);
        Assert.Equal("outbox.events", o.Kafka.Topic);
        Assert.Equal(250, o.Retry.BaseDelayMs);
        Assert.Equal(5000, o.Retry.MaxDelayMs);
    }

    [Theory]
    [InlineData(0, 16)]
    [InlineData(-5, 16)]
    [InlineData(15, 16)]
    [InlineData(16, 16)]
    [InlineData(2048, 2048)]
    [InlineData(1_000_000, 1_000_000)]
    [InlineData(2_000_000, 1_000_000)]
    public void ClampChannelCapacity_ClampsTo_16_1000000(int value, int expected)
    {
        var o = new CdcStreamingOptions { Streaming = new StreamingOptions { ChannelCapacity = value } };

        Assert.Equal(expected, o.Streaming.ClampChannelCapacity());
    }

    [Fact]
    public void BrokerList_SplitsAndTrims()
    {
        var o = new CdcStreamingOptions { Kafka = new KafkaOptions { Brokers = " kafka1:9092 , kafka2:9092 ,, kafka3:9092 " } };

        Assert.Equal(new[] { "kafka1:9092", "kafka2:9092", "kafka3:9092" }, o.Kafka.BrokerList());
    }
}
