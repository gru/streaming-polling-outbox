using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Text;
using Npgsql;
using NpgsqlTypes;
using Writer.Configuration;

namespace Writer.Generation;

public enum BatchOutcome
{
    /// <summary>Commit подтверждён в этой попытке.</summary>
    Committed,
    /// <summary>Unique violation по PK на повторной попытке — доказательство, что «неоднозначный» commit прошлой попытки прошёл.</summary>
    AlreadyCommitted,
    /// <summary>Остановка (stop/shutdown) во время ожидания ретрая — исход commit не определён (ambiguous).</summary>
    Cancelled,
    /// <summary>Нетранзиентная ошибка SQL — повтор бессмыслен.</summary>
    Fatal,
}

public sealed record BatchResult(
    BatchOutcome Outcome,
    int Attempts,
    double BatchDurationMs,
    double CommitDurationMs,
    string? FatalError = null,
    string? LastTransientError = null);

/// <summary>
/// Выполнение INSERT-батчей в public.outbox_messages от sys_writer.
/// Два источника Npgsql: через PgBouncer (transaction pool) и напрямую через HAProxy
/// на актуальный primary; активный выбирает режим прогона.
/// </summary>
public sealed class OutboxWriter : IAsyncDisposable
{
    /// <summary>4 параметра на строку (id, topic, body, created_at); протокол допускает до 65535 — берём запас.</summary>
    public const int MaxBatchSize = 5000;

    private static readonly ConcurrentDictionary<int, string> SqlCache = new();

    private readonly NpgsqlDataSource _pgbouncerSource;
    private readonly NpgsqlDataSource _directSource;

    public OutboxWriter(WriterOptions options)
    {
        // Пул фиксированно широк (parallelism ≤ 64 + запас на замену битых соединений);
        // соединения открываются лениво — неактивный режим ничего не держит.
        const int pool = 72;
        _pgbouncerSource = BuildSource(options.Connection.Pgbouncer, options.Connection.Database, viaPgBouncer: true, pool);
        _directSource = BuildSource(options.Connection.Direct, options.Connection.Database, viaPgBouncer: false, pool);
    }

    public NpgsqlDataSource SourceFor(string mode) =>
        string.Equals(mode, "direct", StringComparison.OrdinalIgnoreCase) ? _directSource : _pgbouncerSource;

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
            MinPoolSize = 2,
            Timeout = 15,
            CommandTimeout = 60,
            // PgBouncer transaction pool: серверная сессия живёт только внутри транзакции,
            // сброс состояния соединения Npgsql после возврата в пул не нужен и вреден
            // (рекомендация Npgsql для внешних пулов). AutoPrepare выключен по умолчанию —
            // именованных prepared statements через transaction pool не возникает.
            NoResetOnClose = viaPgBouncer,
        };
        return NpgsqlDataSource.Create(csb);
    }

    /// <summary>
    /// Записывает батч одной транзакцией: один multi-row INSERT → commit.
    /// Транзиентные сбои (failover/switchover, обрывы соединения) ретраятся теми же UUID
    /// с exponential backoff + jitter. Unique violation на ретрае означает, что прошлая
    /// попытка закоммитилась — это и есть разрешение неоднозначного commit (requirements.md 6.3):
    /// противоречие невозможно, дубликатов в outbox не возникает.
    /// hardCt прерывает попытки немедленно (остановка хоста); stopCt прерывает только
    /// ожидание между ретраями (мягкий stop) — начавшийся commit всегда доигрывается.
    /// </summary>
    public async Task<BatchResult> WriteBatchAsync(string mode, OutboxMessage[] batch,
        CancellationToken hardCt, CancellationToken stopCt)
    {
        var source = SourceFor(mode);
        var sql = SqlCache.GetOrAdd(batch.Length, BuildInsertSql);
        var attempts = 0;
        string? lastTransient = null;
        var totalSw = System.Diagnostics.Stopwatch.StartNew();

        while (true)
        {
            attempts++;
            try
            {
                await using var conn = await source.OpenConnectionAsync(hardCt);
                await using var tx = await conn.BeginTransactionAsync(hardCt);
                await using var cmd = new NpgsqlCommand(sql, conn, tx);
                AddParameters(cmd, batch);
                await cmd.ExecuteNonQueryAsync(hardCt);

                var commitSw = System.Diagnostics.Stopwatch.StartNew();
                await tx.CommitAsync(hardCt);
                return new BatchResult(BatchOutcome.Committed, attempts,
                    totalSw.Elapsed.TotalMilliseconds, commitSw.Elapsed.TotalMilliseconds);
            }
            catch (OperationCanceledException) when (hardCt.IsCancellationRequested)
            {
                return new BatchResult(BatchOutcome.Cancelled, attempts,
                    totalSw.Elapsed.TotalMilliseconds, 0, LastTransientError: lastTransient);
            }
            catch (PostgresException pe) when (pe.SqlState == PostgresErrorCodes.UniqueViolation && attempts > 1)
            {
                return new BatchResult(BatchOutcome.AlreadyCommitted, attempts,
                    totalSw.Elapsed.TotalMilliseconds, 0, LastTransientError: lastTransient);
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                lastTransient = ex.Message;
                using var waitCts = CancellationTokenSource.CreateLinkedTokenSource(stopCt, hardCt);
                try
                {
                    await Task.Delay(BackoffDelay(attempts), waitCts.Token);
                }
                catch (OperationCanceledException)
                {
                    return new BatchResult(BatchOutcome.Cancelled, attempts,
                        totalSw.Elapsed.TotalMilliseconds, 0, LastTransientError: lastTransient);
                }
            }
            catch (Exception ex)
            {
                var detail = ex is PostgresException pg ? $"{pg.SqlState}: {pg.MessageText}" : ex.Message;
                return new BatchResult(BatchOutcome.Fatal, attempts,
                    totalSw.Elapsed.TotalMilliseconds, 0, FatalError: detail, LastTransientError: lastTransient);
            }
        }
    }

    private static string BuildInsertSql(int rows)
    {
        var sb = new StringBuilder(96 + rows * 44)
            .Append("INSERT INTO public.outbox_messages (id, topic, body, created_at) VALUES ");
        for (var i = 0; i < rows; i++)
        {
            if (i > 0) sb.Append(',');
            var p = i * 4;
            sb.Append("($").Append(p + 1).Append(", $").Append(p + 2).Append(", $")
              .Append(p + 3).Append("::jsonb, $").Append(p + 4).Append(')');
        }
        return sb.ToString();
    }

    private static void AddParameters(NpgsqlCommand cmd, OutboxMessage[] batch)
    {
        // Позиционные параметры (без имён) — совместимо с PgBouncer transaction pool.
        foreach (var m in batch)
        {
            cmd.Parameters.Add(new NpgsqlParameter<Guid> { NpgsqlDbType = NpgsqlDbType.Uuid, TypedValue = m.Id });
            cmd.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = m.Topic });
            cmd.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Jsonb, TypedValue = m.BodyJson });
            cmd.Parameters.Add(new NpgsqlParameter<DateTimeOffset> { NpgsqlDbType = NpgsqlDbType.TimestampTz, TypedValue = m.CreatedAt });
        }
    }

    /// <summary>
    /// Транзиентные для стенда сбои: обрывы TCP/таймауты (failover, рестарт PgBouncer/HAProxy,
    /// kill walsender-независимых сессий) и классы ошибок PG 08xxx/шатдауны/перегрузка/локи.
    /// Ошибки прав/схемы/ограничений сюда НЕ входят — они фатальны.
    /// </summary>
    private static bool IsTransient(Exception ex) => ex switch
    {
        PostgresException pe => TransientSqlStates.Contains(pe.SqlState),
        NpgsqlException { InnerException: SocketException or IOException or TimeoutException } => true,
        NpgsqlException { InnerException: PostgresException pe2 } => TransientSqlStates.Contains(pe2.SqlState),
        _ => false,
    };

    private static readonly HashSet<string> TransientSqlStates = new()
    {
        "08000", "08003", "08006", "08001", "08004", "08007", // connection_exception
        "53300",                                               // too_many_connections
        "57P01", "57P02", "57P03",                             // admin/crash shutdown, cannot_connect_now (Patroni failover/switchover)
        "40001", "40P01",                                      // serialization_failure, deadlock_detected
        "55P03",                                               // lock_not_available
        "54000",                                               // out_of_memory / program limit
    };

    private static TimeSpan BackoffDelay(int attempt)
    {
        var cap = 5000.0;
        var ms = Math.Min(250.0 * (1L << Math.Min(attempt - 1, 14)), cap);
        var jittered = ms * (0.7 + 0.6 * Random.Shared.NextDouble()); // ±30%
        return TimeSpan.FromMilliseconds(Math.Clamp(jittered, 50, cap));
    }

    public async ValueTask DisposeAsync()
    {
        await _pgbouncerSource.DisposeAsync();
        await _directSource.DisposeAsync();
    }
}
