using Xunit;
using KafkaValidator.Configuration;

namespace KafkaValidator.UnitTests;

public class KafkaValidatorOptionsTests
{
    [Fact]
    public void Defaults_MatchStand()
    {
        var o = new KafkaValidatorOptions();

        Assert.Equal("kafka1:9092,kafka2:9092,kafka3:9092", o.Kafka.Brokers);
        Assert.Equal(["kafka1:9092", "kafka2:9092", "kafka3:9092"], o.Kafka.BrokerList());
        Assert.Equal("outbox.events", o.Kafka.Topic);
        Assert.Equal("kafka-validator", o.Kafka.GroupId);
        Assert.Equal(1000, o.Kafka.BufferSize);
        Assert.Equal(1, o.Kafka.WorkersCount);
        Assert.Equal("http://writer:8080", o.Writer.BaseUrl);
        Assert.Equal("http://cdc-polling:8080", o.CdcPolling.BaseUrl);
        Assert.Equal("http://cdc-streaming:8080", o.CdcStreaming.BaseUrl);
    }

    [Theory]
    [InlineData(0, 100)]
    [InlineData(99, 100)]
    [InlineData(100, 100)]
    [InlineData(5000, 5000)]
    [InlineData(1_000_000, 100_000)]
    public void ClampBufferSize_Bounds(int value, int expected)
    {
        Assert.Equal(expected, new KafkaOptions { BufferSize = value }.ClampBufferSize());
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(1, 1)]
    [InlineData(4, 4)]
    [InlineData(99, 16)]
    public void ClampWorkersCount_Bounds(int value, int expected)
    {
        Assert.Equal(expected, new KafkaOptions { WorkersCount = value }.ClampWorkersCount());
    }
}
