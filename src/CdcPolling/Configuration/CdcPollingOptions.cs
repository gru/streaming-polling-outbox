namespace CdcPolling.Configuration;

/// <summary>
/// Конфигурация CDC Polling (секция "CdcPolling", env-префикс CDC_POLLING__).
/// Все размеры пачек настраиваются без правки кода (requirements.md 7.1, 9):
///   Poll.BatchSize   — сколько logical changes запрашивать одним peek;
///   Kafka.BatchSize  — прикладной batch публикации;
///   Poll.IdleIntervalMs — пауза между пустыми poll'ами.
/// Это ТРИ разных «батча»: внутренний batching Confluent-продюсера (LingerMs),
/// прикладной batch Kafka и peek-пачка — не путать с подтверждением LSN,
/// которое выполняется один раз на peek-выборку после доставки всего батча.
/// </summary>
public sealed class CdcPollingOptions
{
    public const string SectionName = "CdcPolling";

    public ConnectionOptions Connection { get; set; } = new();
    public PollOptions Poll { get; set; } = new();
    public KafkaOptions Kafka { get; set; } = new();
    public RetryOptions Retry { get; set; } = new();
}

public sealed class ConnectionOptions
{
    /// <summary>pgbouncer | direct (диагностическое прямое подключение к primary).</summary>
    public string Mode { get; set; } = "pgbouncer";

    public EndpointOptions Pgbouncer { get; set; } = new() { Host = "pgbouncer", Port = 6432 };
    public EndpointOptions Direct { get; set; } = new() { Host = "haproxy", Port = 5432 };
    public DatabaseOptions Database { get; set; } = new();
}

public sealed class EndpointOptions
{
    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 5432;
}

public sealed class DatabaseOptions
{
    /// <summary>Прикладная БД стенда (слот и SECURITY DEFINER-функции живут в ней).</summary>
    public string Name { get; set; } = "outbox";
    public string Username { get; set; } = "sys_app";
    public string Password { get; set; } = "";
}

public sealed class PollOptions
{
    /// <summary>
    /// Сколько logical changes запрашивать одним peek (requirements.md 7.1:
    /// по умолчанию 100). Жёсткий потолок 10000 зафиксирован внутри
    /// SECURITY DEFINER-функции cdc.peek_outbox_changes.
    /// </summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>Пауза между пустыми poll'ами (нет busy loop).</summary>
    public int IdleIntervalMs { get; set; } = 500;

    public int ClampBatchSize() => Math.Clamp(BatchSize, 1, 10_000);
}

public sealed class KafkaOptions
{
    public string Brokers { get; set; } = "kafka1:9092,kafka2:9092,kafka3:9092";
    public string Topic { get; set; } = "outbox.events";

    /// <summary>Прикладной batch: максимум сообщений в одном цикле публикации.</summary>
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
