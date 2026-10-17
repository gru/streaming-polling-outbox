namespace CdcStreaming.Configuration;

/// <summary>
/// Конфигурация CDC Streaming (секция "CdcStreaming", env-префикс CDCSTREAMING__ —
/// одно слово, как CDCPOLLING__ у Polling: одиночные подчёркивания не являются
/// разделителями; в compose человеческие имена CDC_STREAMING_* подставляются
/// плейсхолдерами ${...}).
///
/// Настраивается без правки кода (requirements.md 8, 9):
///   Kafka.BatchSize          — прикладной batch публикации (сколько событий
///                              набирается во flush перед отправкой в Kafka);
///   Streaming.ChannelCapacity — ёмкость внутреннего канала receiver→publisher
///                              (backpressure: заполненный канал останавливает
///                              чтение стрима, WAL копится на сервере).
/// Это ТРИ разных «батча»: внутренний batching Confluent-продюсера (LingerMs),
/// прикладной batch Kafka и канал декодированных событий — не путать с
/// подтверждением LSN, которое выполняется replication feedback'ом после
/// доставки всего flush-набора.
/// </summary>
public sealed class CdcStreamingOptions
{
    public const string SectionName = "CdcStreaming";

    public ConnectionOptions Connection { get; set; } = new();
    public StreamingOptions Streaming { get; set; } = new();
    public KafkaOptions Kafka { get; set; } = new();
    public RetryOptions Retry { get; set; } = new();
}

public sealed class ConnectionOptions
{
    /// <summary>
    /// Единственный endpoint, БЕЗ переключателя режимов: replication-протокол
    /// PgBouncer не проксирует (transaction pooling с ним несовместим),
    /// поэтому только напрямую через HAProxy на текущий primary
    /// (requirements.md 3.3: «минуя PgBouncer»).
    /// </summary>
    public EndpointOptions Endpoint { get; set; } = new() { Host = "haproxy", Port = 5432 };

    public DatabaseOptions Database { get; set; } = new();
}

public sealed class EndpointOptions
{
    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 5432;
}

public sealed class DatabaseOptions
{
    /// <summary>Прикладная БД стенда (слот и publication живут в ней).</summary>
    public string Name { get; set; } = "outbox";

    /// <summary>Роль с атрибутом REPLICATION (requirements.md 5, 8) — без SUPERUSER.</summary>
    public string Username { get; set; } = "sys_replication";

    public string Password { get; set; } = "";
}

public sealed class StreamingOptions
{
    /// <summary>Существующий failover-слот pgoutput (02-slot.sql, failover => true).</summary>
    public string SlotName { get; set; } = "outbox_slot";

    /// <summary>Публикация, изменения которой читаем (proto-параметр publication_names).</summary>
    public string Publication { get; set; } = "outbox_publication";

    /// <summary>
    /// Версия протокола pgoutput (proto_version). 1 — как у peek в Polling:
    /// B/R/I/U/D/C без streaming/двухфазных сообщений (их декодер пропускает).
    /// </summary>
    public ulong ProtocolVersion { get; set; } = 1;

    /// <summary>
    /// Ёмкость bounded-канала завершённых транзакций между receiver и publisher.
    /// Заполнение канала останавливает чтение стрима (TCP backpressure) —
    /// подтверждение LSN не происходит, WAL копится (requirements.md 10).
    /// </summary>
    public int ChannelCapacity { get; set; } = 2048;

    public int ClampChannelCapacity() => Math.Clamp(ChannelCapacity, 16, 1_000_000);
}

public sealed class KafkaOptions
{
    public string Brokers { get; set; } = "kafka1:9092,kafka2:9092,kafka3:9092";
    public string Topic { get; set; } = "outbox.events";

    /// <summary>Прикладной batch: максимум событий, публикуемых одним flush-циклом.</summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>Внутренний batching Confluent-продюсера (linger.ms).</summary>
    public double LingerMs { get; set; } = 5;

    /// <summary>
    /// message.timeout.ms: сколько ждать доставки сообщения прежде чем признать
    /// публикацию неуспешной (недоступная Kafka обнаруживается за это время).
    /// </summary>
    public int MessageTimeoutMs { get; set; } = 15_000;

    public string[] BrokerList() =>
        Brokers.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
}

public sealed class RetryOptions
{
    public int BaseDelayMs { get; set; } = 250;
    public int MaxDelayMs { get; set; } = 5000;
}
