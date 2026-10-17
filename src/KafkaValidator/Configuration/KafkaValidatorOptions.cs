namespace KafkaValidator.Configuration;

/// <summary>
/// Конфигурация KafkaValidator (секция "KafkaValidator", env-префикс
/// KAFKAVALIDATOR__ — одним словом, как CDCPOLLING__/CDCSTREAMING__: одиночные
/// подчёркивания не являются разделителями; в compose человеческие имена
/// KAFKA_VALIDATOR_* подставляются плейсхолдерами ${...}).
///
/// Настраивается без правки кода (requirements.md 23). Ключевая семантика
/// консюмера (группа + Earliest + без сохранения оффсетов) зашита в коде —
/// это не опция: полная перемотка topic при каждом старте гарантирует полноту
/// валидации после рестарта (иначе закоммиченные оффсеты «съедят» историю
/// и валидатор ложно сообщит о потерях).
/// </summary>
public sealed class KafkaValidatorOptions
{
    public const string SectionName = "KafkaValidator";

    public KafkaOptions Kafka { get; set; } = new();

    /// <summary>Откуда валидатор берёт ожидаемый набор id (fetchFromWriter).</summary>
    public EndpointOptions Writer { get; set; } = new() { BaseUrl = "http://writer:8080" };

    /// <summary>CDC Polling HTTP API (обогащение вердикта, profile polling).</summary>
    public EndpointOptions CdcPolling { get; set; } = new() { BaseUrl = "http://cdc-polling:8080" };

    /// <summary>CDC Streaming HTTP API (обогащение вердикта, profile streaming).</summary>
    public EndpointOptions CdcStreaming { get; set; } = new() { BaseUrl = "http://cdc-streaming:8080" };
}

public sealed class KafkaOptions
{
    public string Brokers { get; set; } = "kafka1:9092,kafka2:9092,kafka3:9092";

    public string Topic { get; set; } = "outbox.events";

    /// <summary>Имя consumer-группы (видно в Kafka; оффсеты НЕ коммитятся).</summary>
    public string GroupId { get; set; } = "kafka-validator";

    /// <summary>Буфер KafkaFlow на воркер (записей).</summary>
    public int BufferSize { get; set; } = 1000;

    public int ClampBufferSize() => Math.Clamp(BufferSize, 100, 100_000);

    /// <summary>
    /// Число воркеров обработки. По умолчанию 1: записи одного прогона идут
    /// последовательно, состояние реестра под lock — параллелизм не нужен.
    /// </summary>
    public int WorkersCount { get; set; } = 1;

    public int ClampWorkersCount() => Math.Clamp(WorkersCount, 1, 16);

    public string[] BrokerList() =>
        Brokers.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
}

public sealed class EndpointOptions
{
    public string BaseUrl { get; set; } = "";
}
