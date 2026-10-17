using System.Globalization;
using System.Text.Json;
using CdcCommon;
using CdcCommon.PgOutput;
using CdcPolling.Configuration;
using Npgsql;

namespace CdcPolling.Polling;

/// <summary>
/// Доступ к слоту через SECURITY DEFINER-функции из БД outbox (requirements.md 5.2, 7.1-7.2):
/// peek (без потребления), advance (после Kafka delivery), статус слота (метрики/диагностика).
/// Каждый вызов — одиночный SQL-statement, совместимый с PgBouncer transaction pool.
/// </summary>
public sealed class SlotGateway : IAsyncDisposable
{
    private readonly NpgsqlDataSource _pgbouncerSource;
    private readonly NpgsqlDataSource _directSource;

    public SlotGateway(CdcPollingOptions options)
    {
        // Последовательный цикл peek→publish→advance: несколько соединений не нужны;
        // пул шире единицы — только на замену битых соединений при failover.
        const int pool = 8;
        _pgbouncerSource = BuildSource(options.Connection.Pgbouncer, options.Connection.Database, viaPgBouncer: true, pool);
        _directSource = BuildSource(options.Connection.Direct, options.Connection.Database, viaPgBouncer: false, pool);
    }

    public NpgsqlDataSource SourceFor(string mode) =>
        string.Equals(mode, "direct", StringComparison.OrdinalIgnoreCase) ? _directSource : _pgbouncerSource;

    /// <summary>Позиционный параметр — совместимо с PgBouncer transaction pool.</summary>
    public async Task<List<RawSlotRecord>> PeekAsync(string mode, int limit, CancellationToken ct)
    {
        var source = SourceFor(mode);
        await using var conn = await source.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("SELECT lsn::text, xid::text, data FROM cdc.peek_outbox_changes($1)", conn)
        {
            // peek может декодировать заметный объём WAL — CommandTimeout из строки подключения
        };
        cmd.Parameters.Add(new NpgsqlParameter { Value = limit });

        var records = new List<RawSlotRecord>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var lsn = Lsn.Parse(reader.GetString(0));
            var xid = uint.Parse(reader.GetString(1), CultureInfo.InvariantCulture); // xid::text — десятичное
            var data = reader.GetFieldValue<byte[]>(2);
            records.Add(new RawSlotRecord(lsn, xid, data));
        }
        return records;
    }

    /// <summary>Возвращает end_lsn — позицию, до которой слот реально подтверждён.</summary>
    public async Task<Lsn> AdvanceAsync(string mode, Lsn target, CancellationToken ct)
    {
        var source = SourceFor(mode);
        await using var conn = await source.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "SELECT end_lsn::text FROM cdc.advance_outbox_slot(CAST($1 AS pg_lsn))", conn);
        cmd.Parameters.Add(new NpgsqlParameter { Value = target.ToString() });
        var result = await cmd.ExecuteScalarAsync(ct) as string;
        return Lsn.Parse(result ?? throw new InvalidOperationException("advance не вернул end_lsn"));
    }

    public async Task<SlotStatus?> SlotStatusAsync(string mode, CancellationToken ct)
    {
        var source = SourceFor(mode);
        await using var conn = await source.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("SELECT cdc.outbox_slot_status()::text", conn);
        var json = await cmd.ExecuteScalarAsync(ct) as string;
        return SlotStatus.FromJson(json);
    }

    private static NpgsqlDataSource BuildSource(EndpointOptions ep, DatabaseOptions db, bool viaPgBouncer, int maxPool)
    {
        var csb = new NpgsqlConnectionStringBuilder
        {
            Host = ep.Host,
            Port = ep.Port,
            Database = db.Name,
            Username = db.Username,
            Password = db.Password,
            MaxPoolSize = maxPool,
            MinPoolSize = 1,
            Timeout = 15,
            // peek большой пачки при отставании может декодировать долго
            CommandTimeout = 120,
            // Рекомендация Npgsql для внешних пулов (см. Writer)
            NoResetOnClose = viaPgBouncer,
        };
        return NpgsqlDataSource.Create(csb);
    }

    public async ValueTask DisposeAsync()
    {
        await _pgbouncerSource.DisposeAsync();
        await _directSource.DisposeAsync();
    }
}

/// <summary>Разбор jsonb из cdc.outbox_slot_status(): состояние слота + текущий WAL LSN.</summary>
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
