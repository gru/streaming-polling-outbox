using Xunit;
using CdcCommon;

namespace CdcPolling.UnitTests;

public class LsnTests
{
    [Fact]
    public void Parse_Format_Roundtrip()
    {
        foreach (var s in new[] { "0/0", "0/410A290", "1/FFFFFFFF", "FF/1" })
        {
            var lsn = Lsn.Parse(s);
            Assert.Equal(s, lsn.ToString());
        }
    }

    [Fact]
    public void Parse_LowercaseHex_Ok()
    {
        Assert.Equal(Lsn.Parse("0/410A290"), Lsn.Parse("0/410a290"));
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("410A290")]
    [InlineData("/410A290")]
    [InlineData("0/")]
    [InlineData("0/123456789")]   // младшая часть > 8 hex-цифр
    [InlineData("XY/Z")]
    public void TryParse_Invalid_ReturnsNull(string bad)
    {
        Assert.Null(Lsn.TryParse(bad));
    }

    [Fact]
    public void Comparison_Works()
    {
        Assert.True(Lsn.Parse("0/100") > Lsn.Parse("0/FF"));
        Assert.True(Lsn.Parse("1/0") > Lsn.Parse("0/FFFFFFFF"));
        Assert.True(Lsn.Parse("0/10") < Lsn.Parse("0/11"));
        Assert.Equal(Lsn.Parse("0/10"), Lsn.Parse("0/10"));
    }
}
