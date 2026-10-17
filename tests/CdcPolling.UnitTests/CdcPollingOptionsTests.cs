using Xunit;
using CdcPolling.Configuration;

namespace CdcPolling.UnitTests;

public class CdcPollingOptionsTests
{
    [Fact]
    public void ClampBatchSize_Default100()
    {
        Assert.Equal(100, new CdcPollingOptions().Poll.ClampBatchSize());
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(-5, 1)]
    [InlineData(1, 1)]
    [InlineData(100, 100)]
    [InlineData(5000, 5000)]
    [InlineData(10000, 10000)]
    [InlineData(999999, 10000)]
    public void ClampBatchSize_ClampsTo_1_10000(int value, int expected)
    {
        Assert.Equal(expected, new CdcPollingOptions { Poll = new PollOptions { BatchSize = value } }.Poll.ClampBatchSize());
    }

    [Fact]
    public void BrokerList_SplitsAndTrims()
    {
        var kafka = new KafkaOptions { Brokers = " kafka1:9092 , kafka2:9092,kafka3:9092 " };
        Assert.Equal(new[] { "kafka1:9092", "kafka2:9092", "kafka3:9092" }, kafka.BrokerList());
    }
}
