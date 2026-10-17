using System.Text.Json;
using CdcCommon;
using CdcStreaming.Configuration;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;

namespace CdcStreaming.Replication;

/// <summary>
/// Доступ к replication-потоку и статусу слота (requirements.md 8).
///
/// Replication-соединение открывается НАПРЯМУЮ через HAProxy (PgBouncer
/// replication-протокол не проксирует, requirements.md 3.3) от sys_replication.
/// На каждое (пере)подключение — новый LogicalReplicationConnection: ReplicationConnection
/// сам отключает Pooling/Enlist/Multiplexing, соединение одноразовое.
/// Старт без walLocation — от confirmed_flush слота: неподтверждённое будет
/// передано повторно (at-least-once).
///
/// Статус слота — обычным SQL-соединением через ту же функцию cdc.outbox_slot_status(),
/// что и у Polling (гранты — 05-cdc-streaming.sql; peek/advance стримингу не нужны).
/// </summary>
public sealed class ReplicationGateway : IAsyncDisposable
{
    private readonly NpgsqlDataSource _statusSource;
    private readonly CdcStreamingOptions _options;

    public ReplicationGateway(CdcStreamingOptions options)
    {
        _options = options;
        // Статус нужен редко (не чаще раза в секунду) и только диагностически;
        // пул шире единицы — на замену битых соединений при failover
        var csb = new NpgsqlConnectionStringBuilder
        {
            Host = options.Connection.Endpoint.Host,
            Port = options.Connection.Endpoint.Port,
            Database = options.Connection.Database.Name,
            Username = options.Connection.Database.Username,
            Password = options.Connection.Database.Password,
            MaxPoolSize = 4,
            MinPoolSize = 1,
            Timeout = 15,
            CommandTimeout = 30,
        };
        _statusSource = NpgsqlDataSource.Create(csb);
    }

    public async Task<LogicalReplicationConnection> OpenReplicationConnectionAsync(CancellationToken ct)
    {
        var conn = new LogicalReplicationConnection(BuildReplicationConnectionString());
        await conn.Open(ct);
        return conn;
    }

    /// <summary>Стрим pgoutput-сообщений по открытому соединению (обёртка над Npgsql-расширением).</summary>
    public IAsyncEnumerable<PgOutputReplicationMessage> StartReplication(
        LogicalReplicationConnection connection, CancellationToken ct) =>
        connection.StartReplication(
            new PgOutputReplicationSlot(_options.Streaming.SlotName),
            new PgOutputReplicationOptions(_options.Streaming.Publication, _options.Streaming.ProtocolVersion),
            ct);

    public async Task<SlotStatus?> SlotStatusAsync(CancellationToken ct)
    {
        await using var conn = await _statusSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("SELECT cdc.outbox_slot_status()::text", conn);
        var json = await cmd.ExecuteScalarAsync(ct) as string;
        return SlotStatus.FromJson(json);
    }

    private string BuildReplicationConnectionString()
    {
        var csb = new NpgsqlConnectionStringBuilder
        {
            Host = _options.Connection.Endpoint.Host,
            Port = _options.Connection.Endpoint.Port,
            Database = _options.Connection.Database.Name,
            Username = _options.Connection.Database.Username,
            Password = _options.Connection.Database.Password,
            ApplicationName = "cdc-streaming",
        };
        return csb.ConnectionString;
    }

    public async ValueTask DisposeAsync() => await _statusSource.DisposeAsync();
}

/// <summary>Разбор jsonb из cdc.outbox_slot_status(): состояние слота + текущий WAL LSN (форма едина с Polling).</summary>
public sealed record SlotStatus(
    bool Exists,
    bool? Active,
    string? WalStatus,
    Lsn? ConfirmedFlushLsn,
    Lsn? RestartLsn,
    long? SafeWalSize,
    string? Plugin,
    string? Database,
    bool? Synced,
    bool InRecovery,
    Lsn? CurrentWalLsn)
{
    /// <summary>Слот отсутствует или инвалидирован (нужный WAL утрачен) — терминальная ошибка CDC.</summary>
    public bool IsInvalidated => !Exists || string.Equals(WalStatus, "lost", StringComparison.OrdinalIgnoreCase);

    public long? WalLagBytes =>
        CurrentWalLsn is { } cur && ConfirmedFlushLsn is { } confirmed && cur > confirmed
            ? (long)(cur.Value - confirmed.Value)
            : CurrentWalLsn is { } cur2 && ConfirmedFlushLsn is { } conf2 ? 0 : null;

    public static SlotStatus? FromJson(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return null;
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        return new SlotStatus(
            Exists: root.GetProperty("exists").GetBoolean(),
            Active: GetBool(root, "active"),
            WalStatus: GetString(root, "wal_status"),
            ConfirmedFlushLsn: Lsn.TryParse(GetString(root, "confirmed_flush_lsn")),
            RestartLsn: Lsn.TryParse(GetString(root, "restart_lsn")),
            SafeWalSize: GetLong(root, "safe_wal_size"),
            Plugin: GetString(root, "plugin"),
            Database: GetString(root, "database"),
            Synced: GetBool(root, "synced"),
            InRecovery: root.GetProperty("in_recovery").GetBoolean(),
            CurrentWalLsn: Lsn.TryParse(GetString(root, "current_wal_lsn")));

        static string? GetString(JsonElement root, string name) =>
            root.TryGetProperty(name, out var e) && e.ValueKind == JsonValueKind.String ? e.GetString() : null;
        static bool? GetBool(JsonElement root, string name) =>
            root.TryGetProperty(name, out var e) && e.ValueKind is JsonValueKind.True or JsonValueKind.False ? e.GetBoolean() : null;
        static long? GetLong(JsonElement root, string name) =>
            root.TryGetProperty(name, out var e) && e.ValueKind == JsonValueKind.Number ? e.GetInt64() : null;
    }
}

/// <summary>Конверсия LSN: CdcCommon.Lsn (ulong) ↔ NpgsqlLogSequenceNumber.</summary>
public static class LsnConvert
{
    public static NpgsqlLogSequenceNumber ToNpgsql(Lsn lsn) => new(lsn.Value);

    public static Lsn FromNpgsql(NpgsqlLogSequenceNumber lsn) => new((ulong)lsn);
}
