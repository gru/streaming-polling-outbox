using CdcCommon;
using CdcStreaming.Replication;
using NpgsqlTypes;
using Xunit;

namespace CdcStreaming.UnitTests;

public class LsnConvertTests
{
    [Fact]
    public void Roundtrip_PreservesValue()
    {
        var lsn = new Lsn(0x16ABCDEF12345678UL);

        var npgsql = LsnConvert.ToNpgsql(lsn);
        var back = LsnConvert.FromNpgsql(npgsql);

        Assert.Equal(lsn, back);
        Assert.Equal((ulong)npgsql, lsn.Value);
    }

    [Fact]
    public void FromNpgsql_FormatsAsPgLsn()
    {
        // формат совпадает с выводом pg_lsn (используется в снапшоте/метриках)
        var lsn = LsnConvert.FromNpgsql(new NpgsqlLogSequenceNumber(0x000000160000ABCD));

        Assert.Equal("16/ABCD", lsn.ToString());
    }

    [Fact]
    public void Zero_Roundtrips()
    {
        Assert.Equal(Lsn.Zero, LsnConvert.FromNpgsql(LsnConvert.ToNpgsql(Lsn.Zero)));
    }
}
