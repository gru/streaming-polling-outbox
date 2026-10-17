using CdcCommon;
using KafkaFlow;
using KafkaValidator.Metrics;
using KafkaValidator.Validation;
using Microsoft.Extensions.Options;

namespace KafkaValidator.Kafka;

/// <summary>
/// Обработка каждой записи topic: разбор конверта → body.runId → счётчики
/// реестра. Полная логика вердикта — в VerdictComputer (чистая, unit-тесты);
/// handler максимально тонкий: потребление никогда не падает (битая запись —
/// счётчик parse_errors, не сбой консюмера).
/// </summary>
public sealed class ValidationHandler : IMessageHandler<byte[]>
{
    private readonly RunRegistry _registry;
    private readonly KafkaValidatorMetrics _metrics;

    public ValidationHandler(RunRegistry registry, KafkaValidatorMetrics metrics)
    {
        _registry = registry;
        _metrics = metrics;
    }

    public Task Handle(IMessageContext context, byte[] message)
    {
        _metrics.RecordsConsumed.Add(1);

        if (Envelope.TryParse(message, out var consumed) is false || consumed is null)
        {
            _metrics.AddParseError("malformed");
            _registry.RecordParseError();
            return Task.CompletedTask;
        }

        var runId = BodyRunId.TryGet(consumed.BodyJson);
        if (runId is null)
        {
            // валидный конверт без тестового runId (ручная запись в outbox) —
            // в прогоны не попадает, но в общем счётчик потребления учтён
            _metrics.AddSkipped("no_run_id");
        }
        _registry.Record(consumed, runId, DateTimeOffset.UtcNow);
        return Task.CompletedTask;
    }
}
