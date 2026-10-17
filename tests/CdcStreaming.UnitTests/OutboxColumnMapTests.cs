using CdcCommon.PgOutput;
using CdcStreaming.Replication;
using Xunit;

namespace CdcStreaming.UnitTests;

public class OutboxColumnMapTests
{
    // pgoutput присылает значения позиционно в порядке колонок Relation-сообщения —
    // карта должна находить обязательные колонки при любом порядке

    [Fact]
    public void Create_CanonicalOrder_MapsIndexes()
    {
        var map = OutboxColumnMap.Create(new[] { "id", "topic", "body", "created_at" });

        Assert.Equal(0, map.IdIndex);
        Assert.Equal(1, map.TopicIndex);
        Assert.Equal(2, map.BodyIndex);
        Assert.Equal(3, map.CreatedAtIndex);
        Assert.True(map.TryGetColumn(0, out var c0));
        Assert.Equal(OutboxColumn.Id, c0);
        Assert.True(map.TryGetColumn(3, out var c3));
        Assert.Equal(OutboxColumn.CreatedAt, c3);
    }

    [Fact]
    public void Create_DifferentOrder_MapsByName()
    {
        var map = OutboxColumnMap.Create(new[] { "created_at", "body", "id", "topic" });

        Assert.Equal(2, map.IdIndex);
        Assert.Equal(3, map.TopicIndex);
        Assert.Equal(1, map.BodyIndex);
        Assert.Equal(0, map.CreatedAtIndex);
        Assert.True(map.TryGetColumn(2, out var c));
        Assert.Equal(OutboxColumn.Id, c);
    }

    [Theory]
    [InlineData("topic", "body", "created_at")]          // нет id
    [InlineData("id", "body", "created_at")]              // нет topic
    [InlineData("id", "topic", "created_at")]             // нет body
    [InlineData("id", "topic", "body")]                   // нет created_at
    public void Create_MissingRequiredColumn_Throws(params string[] names)
    {
        Assert.Throws<PgOutputParseException>(() => OutboxColumnMap.Create(names));
    }

    [Fact]
    public void TryGetColumn_PositionOutsideMap_ReturnsFalse()
    {
        var map = OutboxColumnMap.Create(new[] { "id", "topic", "body", "created_at", "note" });

        // лишняя колонка relation'а (5-я) не входит в карту — её значение пропускается
        Assert.False(map.TryGetColumn(4, out _));
        Assert.True(map.TryGetColumn(4 - 1, out _));
    }
}
