using System.Text.Json;
using Xunit;
using CdcCommon;
using KafkaValidator.Validation;
using Writer.Generation;

namespace IntegrationTests;

/// <summary>
/// Контракт формата «Writer → конверт Kafka → KafkaValidator» без стенда:
/// сообщение, созданное MessageFactory (прод), сериализуется Envelope'ом CDC
/// и разбирается валидатором — до самого вердикта. Живую доставку проверяет
/// scripts/test/integration-delivery.sh (этап 7, requirements.md 20).
/// </summary>
public class EnvelopeContractTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 10, 16, 12, 34, 56, 789, TimeSpan.Zero);

    /// <summary>Ровно то превращение, которое выполняет CDC после декодирования pgoutput.</summary>
    private static OutboxEvent ToOutboxEvent(OutboxMessage m) =>
        new(m.Id, m.Topic, m.BodyJson, m.CreatedAt, Lsn.Zero, 0);

    [Fact]
    public void MessageFactoryBody_EnvelopeRoundtrip_ValidatorSeesRunId()
    {
        var runId = Guid.NewGuid();
        var (messages, _) = MessageFactory.CreateBatch(runId, 25, payloadBytes: 64);

        foreach (var m in messages)
        {
            var ok = Envelope.TryParse(Envelope.Serialize(ToOutboxEvent(m)), out var parsed);

            Assert.True(ok, $"конверт id={m.Id} не разобрался");
            Assert.Equal(m.Id, parsed!.Id);
            Assert.Equal(MessageFactory.Topic, parsed.Topic);
            Assert.Equal(m.BodyJson, parsed.BodyJson);
            Assert.Equal(runId, BodyRunId.TryGet(parsed.BodyJson));
        }
    }

    [Fact]
    public void FullCycle_MessageFactoryToVerdict_PassWithoutLoss()
    {
        var runId = Guid.NewGuid();
        var (messages, _) = MessageFactory.CreateBatch(runId, 50, payloadBytes: 128);
        var registry = new RunRegistry();

        foreach (var m in messages)
        {
            var ok = Envelope.TryParse(Envelope.Serialize(ToOutboxEvent(m)), out var parsed);
            Assert.True(ok);
            registry.Record(parsed!, BodyRunId.TryGet(parsed!.BodyJson), Now);
        }

        var expected = new ExpectedSet(
            runId, messages.Select(m => m.Id).ToHashSet(), new HashSet<Guid>(), new HashSet<Guid>(),
            ExpectedSet.SourceWriter);
        var verdict = VerdictComputer.Compute(runId, registry.GetRunDetail(runId), expected, Now);

        Assert.Equal("pass", verdict.Verdict);
        Assert.Equal(50, verdict.Kafka.Records);
        Assert.Equal(50, verdict.Kafka.UniqueIds);
        Assert.Equal(0, verdict.Kafka.DuplicateRecords);
        Assert.Equal(0, verdict.MissingWrittenIdsCount);
    }

    [Fact]
    public void FullCycle_OneMessageLostInKafka_FailWithProvenLoss()
    {
        var runId = Guid.NewGuid();
        var (messages, _) = MessageFactory.CreateBatch(runId, 10, payloadBytes: 32);
        var registry = new RunRegistry();

        foreach (var m in messages.Skip(1)) // первая запись «потерялась» до Kafka
        {
            var ok = Envelope.TryParse(Envelope.Serialize(ToOutboxEvent(m)), out var parsed);
            Assert.True(ok);
            registry.Record(parsed!, runId, Now);
        }

        var expected = new ExpectedSet(
            runId, messages.Select(m => m.Id).ToHashSet(), new HashSet<Guid>(), new HashSet<Guid>(),
            ExpectedSet.SourceExplicit);
        var verdict = VerdictComputer.Compute(runId, registry.GetRunDetail(runId), expected, Now);

        Assert.Equal("fail", verdict.Verdict);
        Assert.Equal(1, verdict.MissingWrittenIdsCount);
        Assert.Equal(messages[0].Id, Assert.Single(verdict.MissingWrittenIdsSample));
    }

    [Fact]
    public void WriterRunIdsDto_JsonContract_ValidatorDeserializes()
    {
        // Writer отвечает camelCase (дефолт ASP.NET Core: JsonSerializerDefaults.Web);
        // валидатор читает case-insensitive — тест фиксирует контракт обеих сторон
        var dto = new RunIdsDto(
            RunId: Guid.NewGuid(),
            WrittenCount: 3,
            AmbiguousCount: 1,
            FailedCount: 0,
            WrittenIds: [Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid()],
            AmbiguousIds: [Guid.NewGuid()],
            FailedIds: []);

        var json = JsonSerializer.Serialize(dto, new JsonSerializerOptions(JsonSerializerDefaults.Web));
        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        var parsed = JsonSerializer.Deserialize<WriterRunIds>(json, options);

        Assert.NotNull(parsed);
        Assert.Equal(dto.RunId, parsed!.RunId);
        Assert.Equal(3, parsed.WrittenCount);
        Assert.Equal(dto.WrittenIds, parsed.WrittenIds);
        Assert.Equal(dto.AmbiguousIds, parsed.AmbiguousIds);
        Assert.Empty(parsed.FailedIds);
    }
}
