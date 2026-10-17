namespace Writer.Configuration;

/// <summary>
/// Конфигурация Writer (секция "Writer", env-префикс WRITER__).
/// Режим подключения по умолчанию задаётся конфигом (pgbouncer | direct) и может
/// переопределяться в каждом запросе /api/generation/start — смены режима без
/// изменения кода (requirements.md 3.3, 6.2).
/// </summary>
public sealed class WriterOptions
{
    public const string SectionName = "Writer";

    public ConnectionOptions Connection { get; set; } = new();
    public LoadProfileDefaults Defaults { get; set; } = new();
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
    /// <summary>Прикладная БД стенда (слот и outbox-таблица живут в ней).</summary>
    public string Name { get; set; } = "outbox";
    public string Username { get; set; } = "sys_writer";
    public string Password { get; set; } = "";
}

/// <summary>Значения нагрузки по умолчанию; запрос старта может переопределить любое.</summary>
public sealed class LoadProfileDefaults
{
    public int? TotalMessages { get; set; }
    public long? TotalPayloadBytes { get; set; }
    public int? DurationSeconds { get; set; }
    public bool RunUntilStopped { get; set; }
    public int MessageSizeBytes { get; set; } = 1024;
    /// <summary>null — без ограничения скорости (закрытый контур по parallelism).</summary>
    public int? RatePerSecond { get; set; }
    public int Parallelism { get; set; } = 4;
    public int BatchSize { get; set; } = 100;
}
