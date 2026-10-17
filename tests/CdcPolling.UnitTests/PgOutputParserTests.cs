using Xunit;
using CdcCommon;
using CdcCommon.PgOutput;

namespace CdcPolling.UnitTests;

public class PgOutputParserTests
{
    [Fact]
    public void WriterTransaction_TwoRows_TwoEvents_DeleteIgnored()
    {
        var records = PgOutputTestBuilder.WriterTransaction(beginLsn: 0x4000000, xid: 741, rows: 2);

        var batch = PgOutputParser.Parse(records);

        Assert.Equal(2, batch.Events.Count);
        Assert.All(batch.Events, e =>
        {
            Assert.Equal(Guid.Parse(PgOutputTestBuilder.DefaultId), e.Id);
            Assert.Equal(PgOutputTestBuilder.DefaultTopic, e.Topic);
            Assert.Equal(PgOutputTestBuilder.DefaultBody, e.BodyJson);
            Assert.Equal(DateTimeOffset.Parse("2026-10-16T12:34:56.789Z"), e.CreatedAtUtc);
            Assert.Equal(741u, e.Xid);
        });

        // Delete-записи проигнорированы (не события), но посчитаны
        Assert.Equal(2, batch.RecordCounts['D']);
        Assert.Equal(2, batch.RecordCounts['I']);
        Assert.Equal(1, batch.RecordCounts['B']);
        Assert.Equal(1, batch.RecordCounts['R']);
        Assert.Equal(1, batch.RecordCounts['C']);
        Assert.Equal(0, batch.IncompleteTransactionEventCount);

        // LastCommitLsn = строчный LSN Commit-записи — до неё подтверждается слот
        var commitRecord = records[^1];
        Assert.Equal(commitRecord.Lsn, batch.LastCommitLsn);
        Assert.Equal(commitRecord.Lsn, batch.LastReceivedLsn);
    }

    [Fact]
    public void Columns_MappedByName_RegardlessOfOrder()
    {
        // Relation объявляет колонки в другом порядке: body, created_at, id, topic
        var shuffled = new[]
        {
            ("body", 3802u), ("created_at", 1184u), ("id", 2950u), ("topic", 25u),
        };
        var records = new List<RawSlotRecord>
        {
            PgOutputTestBuilder.Rec(0x10, 10, PgOutputTestBuilder.Begin(1, 1, 10)),
            PgOutputTestBuilder.Rec(0x11, 10, PgOutputTestBuilder.Relation(PgOutputTestBuilder.OutboxRelationId, "public", "outbox_messages", shuffled)),
            PgOutputTestBuilder.Rec(0x12, 10, PgOutputTestBuilder.Insert(PgOutputTestBuilder.OutboxRelationId,
                PgOutputTestBuilder.DefaultBody, PgOutputTestBuilder.DefaultCreatedAt,
                PgOutputTestBuilder.DefaultId, PgOutputTestBuilder.DefaultTopic)),
            PgOutputTestBuilder.Rec(0x13, 10, PgOutputTestBuilder.Commit(0x13, 0x14, 1)),
        };

        var batch = PgOutputParser.Parse(records);

        var e = Assert.Single(batch.Events);
        Assert.Equal(Guid.Parse(PgOutputTestBuilder.DefaultId), e.Id);
        Assert.Equal(PgOutputTestBuilder.DefaultTopic, e.Topic);
        Assert.Equal(PgOutputTestBuilder.DefaultBody, e.BodyJson);
    }

    [Fact]
    public void TwoTransactions_LastCommitLsn_IsLastCommit()
    {
        var first = PgOutputTestBuilder.WriterTransaction(0x100, 1, rows: 1);
        var second = PgOutputTestBuilder.WriterTransaction(0x200, 2, rows: 1);
        var batch = PgOutputParser.Parse(first.Concat(second).ToList());

        Assert.Equal(2, batch.Events.Count);
        Assert.Equal(second[^1].Lsn, batch.LastCommitLsn);
        Assert.Contains(batch.Events, e => e.Xid == 1);
        Assert.Contains(batch.Events, e => e.Xid == 2);
    }

    [Fact]
    public void TransactionWithoutCommit_EventsNotPublished()
    {
        var records = new List<RawSlotRecord>
        {
            PgOutputTestBuilder.Rec(0x10, 10, PgOutputTestBuilder.Begin(1, 1, 10)),
            PgOutputTestBuilder.Rec(0x11, 10, PgOutputTestBuilder.Relation(PgOutputTestBuilder.OutboxRelationId, "public", "outbox_messages", PgOutputTestBuilder.OutboxColumns())),
            PgOutputTestBuilder.Rec(0x12, 10, PgOutputTestBuilder.Insert(PgOutputTestBuilder.OutboxRelationId,
                PgOutputTestBuilder.DefaultId, PgOutputTestBuilder.DefaultTopic, PgOutputTestBuilder.DefaultBody, PgOutputTestBuilder.DefaultCreatedAt)),
        };

        var batch = PgOutputParser.Parse(records);

        Assert.Empty(batch.Events);
        Assert.Equal(1, batch.IncompleteTransactionEventCount);
        Assert.Null(batch.LastCommitLsn); // подтверждать нечего
    }

    [Fact]
    public void InsertOnOtherRelation_Skipped()
    {
        var records = new List<RawSlotRecord>
        {
            PgOutputTestBuilder.Rec(0x10, 10, PgOutputTestBuilder.Begin(1, 1, 10)),
            PgOutputTestBuilder.Rec(0x11, 10, PgOutputTestBuilder.Relation(PgOutputTestBuilder.OtherRelationId, "public", "other_table", ("x", 25u))),
            PgOutputTestBuilder.Rec(0x12, 10, PgOutputTestBuilder.Insert(PgOutputTestBuilder.OtherRelationId, "value")),
            PgOutputTestBuilder.Rec(0x13, 10, PgOutputTestBuilder.Commit(0x13, 0x14, 1)),
        };

        var batch = PgOutputParser.Parse(records);

        Assert.Empty(batch.Events);                      // чужой relation — не событие
        Assert.Equal(0, batch.IncompleteTransactionEventCount);
        Assert.NotNull(batch.LastCommitLsn);             // но commit двигает watermark
    }

    [Fact]
    public void InsertWithoutRelation_Throws()
    {
        var records = new List<RawSlotRecord>
        {
            PgOutputTestBuilder.Rec(0x12, 10, PgOutputTestBuilder.Insert(PgOutputTestBuilder.OutboxRelationId,
                PgOutputTestBuilder.DefaultId, PgOutputTestBuilder.DefaultTopic, PgOutputTestBuilder.DefaultBody, PgOutputTestBuilder.DefaultCreatedAt)),
        };

        var ex = Assert.Throws<PgOutputParseException>(() => PgOutputParser.Parse(records));
        Assert.Contains("Relation", ex.Message);
    }

    [Theory]
    [InlineData(2)]  // NULL body
    [InlineData(3)]  // unchanged-TOAST body
    public void Insert_RequiredColumnMissing_Throws(int nullIndex)
    {
        string?[] values =
        [
            PgOutputTestBuilder.DefaultId, PgOutputTestBuilder.DefaultTopic,
            PgOutputTestBuilder.DefaultBody, PgOutputTestBuilder.DefaultCreatedAt,
        ];
        values[nullIndex] = nullIndex == 3 ? "«u»" : null; // body(2) — NULL, created_at(3) — 'u'

        var records = new List<RawSlotRecord>
        {
            PgOutputTestBuilder.Rec(0x10, 10, PgOutputTestBuilder.Begin(1, 1, 10)),
            PgOutputTestBuilder.Rec(0x11, 10, PgOutputTestBuilder.Relation(PgOutputTestBuilder.OutboxRelationId, "public", "outbox_messages", PgOutputTestBuilder.OutboxColumns())),
            PgOutputTestBuilder.Rec(0x12, 10, PgOutputTestBuilder.Insert(PgOutputTestBuilder.OutboxRelationId, values)),
        };

        Assert.Throws<PgOutputParseException>(() => PgOutputParser.Parse(records));
    }

    [Fact]
    public void Insert_WrongTopic_Throws()
    {
        var records = PgOutputTestBuilder.WriterTransaction(0x100, 5, rows: 1, topic: "other.topic");

        Assert.Throws<PgOutputParseException>(() => PgOutputParser.Parse(records));
    }

    [Fact]
    public void TruncatedRecord_ThrowsWithOffset()
    {
        var records = new List<RawSlotRecord>
        {
            PgOutputTestBuilder.Rec(0x10, 10, PgOutputTestBuilder.Begin(1, 1, 10)),
            PgOutputTestBuilder.Rec(0x11, 10, PgOutputTestBuilder.Relation(PgOutputTestBuilder.OutboxRelationId, "public", "outbox_messages", PgOutputTestBuilder.OutboxColumns())),
            PgOutputTestBuilder.Rec(0x12, 10, PgOutputTestBuilder.TruncatedInsert(PgOutputTestBuilder.OutboxRelationId)),
        };

        var ex = Assert.Throws<PgOutputParseException>(() => PgOutputParser.Parse(records));
        Assert.Contains("обрывается", ex.Message);
    }

    [Fact]
    public void UnknownRecordKind_CountedNotThrown()
    {
        var records = new List<RawSlotRecord>
        {
            PgOutputTestBuilder.Rec(0x10, 10, PgOutputTestBuilder.Unknown((byte)'Z')),
        };

        var batch = PgOutputParser.Parse(records);

        Assert.Empty(batch.Events);
        Assert.Equal(1, batch.RecordCounts['Z']);
    }

    [Fact]
    public void EmptyInput_EmptyBatch()
    {
        var batch = PgOutputParser.Parse([]);
        Assert.Empty(batch.Events);
        Assert.Null(batch.LastCommitLsn);
        Assert.Null(batch.LastReceivedLsn);
    }

    [Fact]
    public void EmptyTransaction_OnlyBeginCommit_AdvancesWatermark()
    {
        var records = new List<RawSlotRecord>
        {
            PgOutputTestBuilder.Rec(0x10, 10, PgOutputTestBuilder.Begin(1, 1, 10)),
            PgOutputTestBuilder.Rec(0x11, 10, PgOutputTestBuilder.Commit(0x11, 0x12, 1)),
        };

        var batch = PgOutputParser.Parse(records);

        Assert.Empty(batch.Events);
        Assert.NotNull(batch.LastCommitLsn); // пустая транзакция подтверждается
    }

    [Theory]
    [InlineData("2026-10-16 12:34:56.789+00")]     // ISO, UTC — формат стенда
    [InlineData("2026-10-16 15:34:56.789+03")]     // целочасовое смещение без минут
    [InlineData("2026-10-16 07:04:56.789-05:30")]  // смещение с минутами
    [InlineData("2026-10-16 12:34:56.789")]        // без смещения (AssumeUniversal)
    public void ParseTimestamp_Variants(string text)
    {
        var dto = PgOutputParser.ParseTimestamp(text);
        Assert.Equal(DateTimeOffset.Parse("2026-10-16T12:34:56.789Z"), dto);
    }

    [Fact]
    public void ParseTimestamp_Garbage_Throws()
    {
        Assert.Throws<PgOutputParseException>(() => PgOutputParser.ParseTimestamp("not-a-timestamp"));
    }
}
