using Xunit;
using KafkaValidator.Validation;

namespace KafkaValidator.UnitTests;

public class VerdictComputerTests
{
    private static readonly DateTimeOffset Now = DateTimeOffset.Parse("2026-10-16T12:00:00Z");

    private static Guid G(char c) => Guid.Parse($"00000000-0000-7000-8000-00000000000{c}");

    private static RunDetail Detail(params Guid[] ids) => new(Guid.Empty, ids.Length, ids.ToHashSet(), Now, Now);

    private static ExpectedSet Expected(
        Guid[] written, Guid[] ambiguous = null!, Guid[] failed = null!) => new(
        Guid.Empty,
        written.ToHashSet(),
        (ambiguous ?? []).ToHashSet(),
        (failed ?? []).ToHashSet(),
        ExpectedSet.SourceWriter);

    [Fact]
    public void Compute_FullDelivery_Pass()
    {
        var v = VerdictComputer.Compute(Guid.Empty, Detail(G('1'), G('2'), G('3')),
            Expected([G('1'), G('2'), G('3')]), Now);

        Assert.Equal("pass", v.Verdict);
        Assert.Equal(3, v.Kafka.Records);
        Assert.Equal(3, v.Kafka.UniqueIds);
        Assert.Equal(0, v.Kafka.DuplicateRecords);
        Assert.Equal(0, v.MissingWrittenIdsCount);
        Assert.Equal(0, v.UnexpectedExtraIdsCount);
        Assert.Equal(3, v.Expected.WrittenCount);
        Assert.Equal(ExpectedSet.SourceWriter, v.Expected.Source);
    }

    [Fact]
    public void Compute_DuplicatesAllowedByAtLeastOnce_StillPass()
    {
        // 5 записей, 3 уникальных: ретраи CDC после сбоя между delivery и ack (§10)
        var v = VerdictComputer.Compute(Guid.Empty,
            new RunDetail(Guid.Empty, 5, new HashSet<Guid> { G('1'), G('2'), G('3') }, Now, Now),
            Expected([G('1'), G('2'), G('3')]), Now);

        Assert.Equal("pass", v.Verdict);
        Assert.Equal(2, v.Kafka.DuplicateRecords);
    }

    [Fact]
    public void Compute_WrittenMissingInKafka_Fail_ProvenCdcLoss()
    {
        var v = VerdictComputer.Compute(Guid.Empty, Detail(G('1'), G('2')),
            Expected([G('1'), G('2'), G('3')]), Now);

        Assert.Equal("fail", v.Verdict);
        Assert.Equal(1, v.MissingWrittenIdsCount);
        Assert.Equal(G('3'), Assert.Single(v.MissingWrittenIdsSample));
    }

    [Fact]
    public void Compute_AmbiguousNotDelivered_IsNotProvenLoss_Pass()
    {
        // Writer не знает, был ли commit (обрыв соединения во время commit);
        // записи в Kafka нет — доказанной потери CDC нет (§15)
        var v = VerdictComputer.Compute(Guid.Empty, Detail(G('1')),
            Expected([G('1')], ambiguous: [G('9')]), Now);

        Assert.Equal("pass", v.Verdict);
        Assert.Equal(0, v.MissingWrittenIdsCount);
        Assert.Equal(1, v.AmbiguousNotDeliveredCount);
        Assert.Equal(0, v.AmbiguousDeliveredCount);
    }

    [Fact]
    public void Compute_AmbiguousDelivered_Pass_Counted()
    {
        // commit всё-таки случился и CDC доставил — это нормально
        var v = VerdictComputer.Compute(Guid.Empty, Detail(G('1'), G('9')),
            Expected([G('1')], ambiguous: [G('9')]), Now);

        Assert.Equal("pass", v.Verdict);
        Assert.Equal(1, v.AmbiguousDeliveredCount);
        Assert.Equal(0, v.UnexpectedExtraIdsCount);
    }

    [Fact]
    public void Compute_UnexpectedExtraId_Fail()
    {
        // id вне written/ambiguous/failed — в Kafka есть то, чего Writer не отправлял
        var v = VerdictComputer.Compute(Guid.Empty, Detail(G('1'), G('e')),
            Expected([G('1')]), Now);

        Assert.Equal("fail", v.Verdict);
        Assert.Equal(1, v.UnexpectedExtraIdsCount);
        Assert.Equal(G('e'), Assert.Single(v.UnexpectedExtraIdsSample));
    }

    [Fact]
    public void Compute_FailedIdDelivered_IsUnexpected_Fail()
    {
        // Writer доказал, что транзакция НЕ зафиксирована, а запись в Kafka есть
        var v = VerdictComputer.Compute(Guid.Empty, Detail(G('1'), G('f')),
            Expected([G('1')], failed: [G('f')]), Now);

        Assert.Equal("fail", v.Verdict);
        Assert.Equal(1, v.UnexpectedExtraIdsCount);
    }

    [Fact]
    public void Compute_RunNeverSeenInKafka_AllWrittenMissing_Fail()
    {
        var v = VerdictComputer.Compute(Guid.Empty, tallies: null,
            Expected([G('1'), G('2')]), Now);

        Assert.Equal("fail", v.Verdict);
        Assert.Equal(0, v.Kafka.Records);
        Assert.Equal(0, v.Kafka.UniqueIds);
        Assert.Equal(2, v.MissingWrittenIdsCount);
    }

    [Fact]
    public void Compute_SampleLists_CappedAt20()
    {
        var written = Enumerable.Range(0, 30).Select(i => Guid.NewGuid()).ToArray();
        var v = VerdictComputer.Compute(Guid.Empty, tallies: null, Expected(written), Now);

        Assert.Equal(30, v.MissingWrittenIdsCount);
        Assert.Equal(VerdictComputer.SampleLimit, v.MissingWrittenIdsSample.Count);
    }
}
