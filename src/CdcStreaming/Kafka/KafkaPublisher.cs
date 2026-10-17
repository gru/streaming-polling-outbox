using CdcCommon;
using KafkaFlow;
using KafkaFlow.Producers;

namespace CdcStreaming.Kafka;

/// <summary>
/// Публикация в Kafka через KafkaFlow (requirements.md 9 — KafkaFlow как основной API).
/// ProduceAsync ожидает delivery-репорт (acks=all, идемпотентный продюсер
/// настраиваются в Program.cs) — сообщение считается доставленным только после него.
/// Key = id: повторы одного сообщения (at-least-once после сбоя между доставкой
/// и подтверждением LSN) попадают в одну партицию — консюмер дедуплицирует по id;
/// UUIDv7 даёт равномерное распределение по партициям. Value = конверт
/// {"id","topic","body","timestamp"} (requirements.md 4.2).
/// </summary>
public sealed class KafkaPublisher
{
    public const string ProducerName = "cdc-streaming";

    private readonly IProducerAccessor _accessor;
    private readonly string _topic;
    private IMessageProducer? _producer;

    public KafkaPublisher(IProducerAccessor accessor, Configuration.CdcStreamingOptions options)
    {
        _accessor = accessor;
        _topic = options.Kafka.Topic;
    }

    public Task ProduceAsync(OutboxEvent e, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var producer = _producer ??= _accessor.GetProducer(ProducerName);
        return producer.ProduceAsync(_topic, e.Id.ToString(), Envelope.Serialize(e));
    }
}
