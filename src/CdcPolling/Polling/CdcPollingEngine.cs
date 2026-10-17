using System.Diagnostics;
using System.Net.Sockets;
using CdcCommon;
using CdcCommon.PgOutput;
using CdcPolling.Configuration;
using CdcPolling.Kafka;
using CdcPolling.Metrics;
using Microsoft.Extensions.Options;
using Npgsql;

namespace CdcPolling.Polling;

/// <summary>
/// Цикл CDC Polling (requirements.md 7, 10): peek → разбор pgoutput → публикация
/// батча в Kafka → подтверждение доставки → advance слота. Ключевой инвариант
/// at-least-once: LSN подтверждается ТОЛЬКО после delivery-репорта Kafka по всему
/// батчу; при недоступности Kafka подтверждения нет и WAL копится; при сбое между
/// доставкой и advance сообщения будут доставлены повторно (дубликаты допустимы).
///
/// Состояния:
///   running           — обычный цикл;
///   waiting_database  — транзиентный сбой SQL (failover/switchover/обрыв) — backoff и повтор;
///   waiting_kafka     — Kafka недоступна — повтор публикации без advance;
///   error_slot        — слот инвалидирован (wal_status=lost или удалён): ТЕРМИНАЛЬНОЕ
///                       состояние с диагностикой, слот не пересоздаётся и потери
///                       не скрываются (requirements.md 10, 12.3);
///   failed            — неустранимая ошибка данных/протокола (битый pgoutput и т.п.);
///   stopped           — graceful shutdown.
/// </summary>
public sealed class CdcPollingEngine : BackgroundService
{
    public const string StateStarting = "starting";
    public const string StateRunning = "running";
    public const string StateWaitingDatabase = "waiting_database";
    public const string StateWaitingKafka = "waiting_kafka";
    public const string StateErrorSlot = "error_slot";
    public const string StateFailed = "failed";
    public const string StateStopped = "stopped";

    private const int E2eSampleLimit = 4096;

    private readonly CdcPollingOptions _options;
    private readonly SlotGateway _gateway;
    private readonly KafkaPublisher _publisher;
    private readonly CdcPollingMetrics _metrics;
    private readonly ILogger<CdcPollingEngine> _logger;

    private readonly object _gate = new();

    private string _state = StateStarting;
    private DateTimeOffset _startedAt;
    private DateTimeOffset? _stateChangedAt;
    private DateTimeOffset? _degradedSince;

    private long _messagesRead, _messagesDelivered, _recordsDecoded, _peekBatches, _publishBatches;
    private long _readErrors, _kafkaErrors, _retries, _reconnects;
    private int _recoveries;
    private int _incompleteEvents;
    private double? _lastReadMs, _lastPublishMs, _lastRecoveryMs;
    private Lsn? _lastReceived, _confirmed;
    private long? _walLag;
    private string? _lastError, _lastKafkaError;
    private SlotStatus? _slot;
    private readonly Queue<double> _e2eSamples = new();
    private DateTimeOffset _lastStatusRefresh;

    public CdcPollingEngine(
        IOptions<CdcPollingOptions> options,
        SlotGateway gateway,
        KafkaPublisher publisher,
        CdcPollingMetrics metrics,
        ILogger<CdcPollingEngine> logger)
    {
        _options = options.Value;
        _gateway = gateway;
        _publisher = publisher;
        _metrics = metrics;
        _logger = logger;
    }

    public string State { get { lock (_gate) return _state; } }

    public CdcStateSnapshot GetSnapshot()
    {
        lock (_gate)
        {
            var (p50, p95, p99) = Percentiles.P50P95P99(_e2eSamples.ToArray());
            return new CdcStateSnapshot
            {
                State = _state,
                StartedAt = _startedAt,
                StateChangedAt = _stateChangedAt,
                Config = new ConfigEcho
                {
                    ConnectionMode = _options.Connection.Mode,
                    PollBatchSize = _options.Poll.ClampBatchSize(),
                    IdleIntervalMs = _options.Poll.IdleIntervalMs,
                    KafkaBatchSize = _options.Kafka.BatchSize,
                    KafkaTopic = _options.Kafka.Topic,
                    KafkaBrokers = _options.Kafka.Brokers,
                },
                MessagesRead = _messagesRead,
                MessagesDelivered = _messagesDelivered,
                RecordsDecoded = _recordsDecoded,
                PeekBatches = _peekBatches,
                PublishBatches = _publishBatches,
                ReadErrors = _readErrors,
                KafkaErrors = _kafkaErrors,
                Retries = _retries,
                Reconnects = _reconnects,
                IncompleteTransactionEvents = _incompleteEvents,
                LastReceivedLsn = _lastReceived?.ToString(),
                ConfirmedLsn = _confirmed?.ToString(),
                WalLagBytes = _walLag,
                LastReadMs = _lastReadMs,
                LastPublishMs = _lastPublishMs,
                E2eP50Ms = p50,
                E2eP95Ms = p95,
                E2eP99Ms = p99,
                LastRecoveryMs = _lastRecoveryMs,
                Recoveries = _recoveries,
                Slot = _slot,
                LastError = _lastError,
                LastKafkaError = _lastKafkaError,
            };
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var mode = _options.Connection.Mode;
        var pollBatch = _options.Poll.ClampBatchSize();
        var idle = TimeSpan.FromMilliseconds(Math.Max(50, _options.Poll.IdleIntervalMs));
        var kafkaBatch = Math.Max(1, _options.Kafka.BatchSize);
        var backoffBase = Math.Max(50, _options.Retry.BaseDelayMs);
        var backoffCap = Math.Max(backoffBase, _options.Retry.MaxDelayMs);

        lock (_gate) _startedAt = DateTimeOffset.UtcNow;
        _logger.LogInformation(
            "CDC Polling запускается: mode={Mode} peekBatch={PeekBatch} kafkaBatch={KafkaBatch} idle={Idle}ms topic={Topic} brokers={Brokers} retry={Base}..{Cap}ms",
            mode, pollBatch, kafkaBatch, idle.TotalMilliseconds, _options.Kafka.Topic, _options.Kafka.Brokers,
            backoffBase, backoffCap);

        var dbAttempts = 0;
        var kafkaAttempts = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            // ---------- 1. peek ----------
            List<RawSlotRecord> records;
            var readSw = Stopwatch.StartNew();
            try
            {
                records = await _gateway.PeekAsync(mode, pollBatch, stoppingToken);
                readSw.Stop();
                dbAttempts = 0;
                RecoveredIfWas(StateWaitingDatabase);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                var status = await TryStatusAsync(mode, stoppingToken);
                if (status?.IsInvalidated == true)
                {
                    await EnterErrorSlotAsync(mode, DescribeException(ex), status, stoppingToken);
                    return;
                }
                var transient = IsTransient(ex);
                lock (_gate)
                {
                    _state = StateWaitingDatabase;
                    if (_degradedSince is null) _degradedSince = DateTimeOffset.UtcNow;
                    _stateChangedAt ??= DateTimeOffset.UtcNow;
                    _readErrors++;
                    _reconnects++;
                    _lastError = DescribeException(ex);
                }
                _metrics.ObserveState(CdcPollingMetrics.StateWaitingDatabase);
                _metrics.ReadErrors.Add(1, new KeyValuePair<string, object?>("reason", "transient"));
                _metrics.Reconnects.Add(1);
                if (!transient)
                    _logger.LogError(ex, "Нетранзиентная ошибка чтения слота — повтор с backoff");
                else
                    _logger.LogWarning("Сбой чтения (транзиентный, попытка {Attempt}): {Error}", dbAttempts + 1, DescribeException(ex));
                if (!await DelayCancellable(Backoff.Delay(++dbAttempts, backoffBase, backoffCap), stoppingToken))
                    break;
                continue;
            }

            SetState(StateRunning);
            lock (_gate)
            {
                _peekBatches++;
                _lastReadMs = readSw.Elapsed.TotalMilliseconds;
                _recordsDecoded += records.Count;
            }
            _metrics.ReadBatchDurationMs.Record(readSw.Elapsed.TotalMilliseconds);
            _metrics.ReadBatchSize.Record(records.Count);

            // ---------- 2. разбор ----------
            PgOutputBatch batch;
            try
            {
                batch = PgOutputParser.Parse(records);
            }
            catch (PgOutputParseException ex)
            {
                lock (_gate)
                {
                    _state = StateFailed;
                    _lastError = $"pgoutput parse: {ex.Message}";
                    _readErrors++;
                }
                _metrics.ObserveState(CdcPollingMetrics.StateFailed);
                _metrics.ReadErrors.Add(1, new KeyValuePair<string, object?>("reason", "fatal"));
                _logger.LogCritical(ex,
                    "Не удалось разобрать бинарную запись pgoutput — терминальная ошибка (данные не теряются: слот не подтверждён). Требуется вмешательство.");
                await MonitorTerminalAsync(mode, stoppingToken);
                return;
            }

            _metrics.AddRecords(batch.RecordCounts);
            if (batch.LastReceivedLsn is { } received)
            {
                lock (_gate) _lastReceived = received;
                _metrics.ObserveLastReceived(received);
            }
            lock (_gate)
            {
                _messagesRead += batch.Events.Count;
                _incompleteEvents = batch.IncompleteTransactionEventCount;
            }
            _metrics.MessagesRead.Add(batch.Events.Count);

            // ---------- 3. публикация (после ack ВСЕГО батча — advance) ----------
            if (batch.Events.Count > 0)
            {
                var delivered = false;
                var publishSw = Stopwatch.StartNew();
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        foreach (var chunk in batch.Events.Chunk(kafkaBatch))
                        {
                            var tasks = new Task[chunk.Length];
                            for (var i = 0; i < chunk.Length; i++)
                                tasks[i] = PublishOneAsync(chunk[i]);
                            // WaitAsync: при остановке хоста прекращаем ожидание; доставка не
                            // подтверждена ⇒ advance не выполняется (после рестарта — повторная доставка)
                            await Task.WhenAll(tasks).WaitAsync(stoppingToken);
                        }

                        publishSw.Stop();
                        kafkaAttempts = 0;
                        delivered = true;
                        RecoveredIfWas(StateWaitingKafka);
                        break;
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        kafkaAttempts++;
                        var reason = ClassifyKafkaError(ex);
                        lock (_gate)
                        {
                            _state = StateWaitingKafka;
                            if (_degradedSince is null) _degradedSince = DateTimeOffset.UtcNow;
                            _kafkaErrors++;
                            _retries++;
                            _lastKafkaError = DescribeException(ex);
                        }
                        _metrics.ObserveState(CdcPollingMetrics.StateWaitingKafka);
                        _metrics.KafkaErrors.Add(1, new KeyValuePair<string, object?>("reason", reason));
                        _metrics.Retries.Add(1);
                        _logger.LogWarning(
                            "Kafka недоступна/ошибка публикации (попытка {Attempt}, reason={Reason}); LSN не подтверждается, ожидаю восстановления: {Error}",
                            kafkaAttempts, reason, DescribeException(ex));

                        // Ждём с backoff, периодически проверяя слот: при переполнении
                        // max_slot_wal_keep_size слот инвалидируется — ошибка должна
                        // проявиться даже пока доставка стоит (requirements.md 12)
                        if (!await WaitKafkaAsync(Backoff.Delay(kafkaAttempts, backoffBase, backoffCap), mode, stoppingToken))
                            break;
                        var status = await TryStatusAsync(mode, stoppingToken);
                        if (status?.IsInvalidated == true)
                        {
                            await EnterErrorSlotAsync(mode, "слот инвалидирован во время ожидания Kafka", status, stoppingToken);
                            return;
                        }
                    }
                }

                if (!delivered || stoppingToken.IsCancellationRequested)
                    break; // доставка не подтверждена — advance не выполняем

                lock (_gate)
                {
                    _publishBatches++;
                    _lastPublishMs = publishSw.Elapsed.TotalMilliseconds;
                }
                _metrics.PublishBatchDurationMs.Record(publishSw.Elapsed.TotalMilliseconds);
            }

            // ---------- 4. advance (только после доставки или для пустых commit'ов) ----------
            if (batch.LastCommitLsn is { } target)
            {
                try
                {
                    var endLsn = await _gateway.AdvanceAsync(mode, target, stoppingToken);
                    lock (_gate) _confirmed = endLsn;
                    _metrics.ObserveConfirmed(endLsn);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    var status = await TryStatusAsync(mode, stoppingToken);
                    if (status?.IsInvalidated == true)
                    {
                        await EnterErrorSlotAsync(mode, $"advance: {DescribeException(ex)}", status, stoppingToken);
                        return;
                    }
                    if (IsTransient(ex))
                    {
                        // Сообщения уже доставлены, позиция не подтверждена: следующий peek вернёт
                        // их повторно (дубликаты допустимы) и advance догонит — это норма at-least-once
                        lock (_gate)
                        {
                            _readErrors++;
                            _lastError = $"advance (будет повторён): {DescribeException(ex)}";
                        }
                        _metrics.ReadErrors.Add(1, new KeyValuePair<string, object?>("reason", "transient"));
                        _logger.LogWarning("Сбой advance (транзиентный), позиция будет подтверждена на следующем цикле: {Error}", DescribeException(ex));
                    }
                    else
                    {
                        lock (_gate)
                        {
                            _state = StateFailed;
                            _readErrors++;
                            _lastError = $"advance: {DescribeException(ex)}";
                        }
                        _metrics.ObserveState(CdcPollingMetrics.StateFailed);
                        _metrics.ReadErrors.Add(1, new KeyValuePair<string, object?>("reason", "fatal"));
                        _logger.LogCritical(ex, "Нетранзиентная ошибка advance — терминальная ошибка. Данные в Kafka доставлены, позиция слота не двигается.");
                        await MonitorTerminalAsync(mode, stoppingToken);
                        return;
                    }
                }
            }

            // ---------- 5. статус слота (метрики WAL lag; не чаще раза в секунду) ----------
            if (DateTimeOffset.UtcNow - _lastStatusRefresh >= TimeSpan.FromSeconds(1))
            {
                var status = await TryStatusAsync(mode, stoppingToken);
                if (status?.IsInvalidated == true)
                {
                    await EnterErrorSlotAsync(mode, "слот инвалидирован (обнаружено периодической проверкой)", status, stoppingToken);
                    return;
                }
            }

            // ---------- 6. пауза: полная пачка — сразу следующий poll ----------
            if (records.Count < pollBatch)
            {
                if (!await DelayCancellable(idle, stoppingToken))
                    break;
            }
        }

        SetState(StateStopped);
        _metrics.ObserveState(CdcPollingMetrics.StateStopped);
        _logger.LogInformation("CDC Polling остановлен (graceful shutdown)");
    }

    private async Task PublishOneAsync(OutboxEvent e)
    {
        await _publisher.ProduceAsync(e, CancellationToken.None);
        var latencyMs = (DateTimeOffset.UtcNow - e.CreatedAtUtc).TotalMilliseconds;
        _metrics.E2eLatencyMs.Record(latencyMs);
        lock (_gate)
        {
            _messagesDelivered++;
            _e2eSamples.Enqueue(latencyMs);
            while (_e2eSamples.Count > E2eSampleLimit)
                _e2eSamples.Dequeue();
        }
        _metrics.MessagesDelivered.Add(1);
    }

    /// <summary>
    /// Ожидание восстановления Kafka с периодическим обновлением статуса слота:
    /// пока доставка стоит, Writer продолжает писать и WAL копится — lag должен
    /// быть виден в метриках (requirements.md 10, 12.1, TEST-HA-003).
    /// </summary>
    private async Task<bool> WaitKafkaAsync(TimeSpan delay, string mode, CancellationToken ct)
    {
        var deadline = DateTimeOffset.UtcNow + delay;
        while (true)
        {
            var now = DateTimeOffset.UtcNow;
            if (now >= deadline) return true;
            _lastStatusRefresh = now;
            await TryStatusAsync(mode, ct); // ошибки игнорируются — БД может быть недоступна одновременно
            var chunk = TimeSpan.FromMilliseconds(Math.Min(2000, Math.Max(50, (deadline - now).TotalMilliseconds)));
            try { await Task.Delay(chunk, ct); }
            catch (OperationCanceledException) { return false; }
        }
    }

    private async Task<SlotStatus?> TryStatusAsync(string mode, CancellationToken ct)
    {
        try
        {
            var status = await _gateway.SlotStatusAsync(mode, ct);
            if (status is not null)
            {
                lock (_gate) _slot = status;
                if (status.CurrentWalLsn is { } cur && status.ConfirmedFlushLsn is { } conf)
                {
                    var lag = cur > conf ? (long)(cur.Value - conf.Value) : 0;
                    lock (_gate) _walLag = lag;
                    _metrics.ObserveWalLag(lag);
                }
                if (status.ConfirmedFlushLsn is { } confirmed)
                {
                    lock (_gate) _confirmed ??= confirmed;
                    _metrics.ObserveConfirmed(confirmed);
                }
            }
            return status;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            return null; // диагностический вызов — сбой не должен ронять цикл
        }
    }

    /// <summary>
    /// Терминальное состояние «слот инвалидирован»: без пересоздания слота и без
    /// заявлений об отсутствии потерь (requirements.md 10). Контейнер живёт,
    /// состояние наблюдаемо через /api/cdc/state и /ready (503); восстановление —
    /// только ручное (пересоздание слота/wipe стенда) + рестарт контейнера.
    /// </summary>
    private async Task EnterErrorSlotAsync(string mode, string reason, SlotStatus? status, CancellationToken ct)
    {
        lock (_gate)
        {
            _state = StateErrorSlot;
            _stateChangedAt = DateTimeOffset.UtcNow;
            _degradedSince = null;
            _readErrors++;
            _lastError = $"слот инвалидирован: {reason}";
            if (status is not null) _slot = status;
        }
        _metrics.ObserveState(CdcPollingMetrics.StateErrorSlot);
        _metrics.ReadErrors.Add(1, new KeyValuePair<string, object?>("reason", "fatal"));
        _logger.LogCritical(
            "СЛОТ ИНВАЛИДИРОВАН — терминальная ошибка CDC Polling. Причина: {Reason}. Статус: {@Status}. " +
            "Слот не пересоздаётся автоматически; подтверждения позиции не происходит. Сообщения, чей WAL утрачен, " +
            "НЕ доставлены и НЕ могут быть доставлены из этого слота (requirements.md 12.3). Требуется вмешательство.",
            reason, status);
        await MonitorTerminalAsync(mode, ct);
    }

    /// <summary>После терминальной ошибки остаёмся живыми и обновляем статус слота (наблюдаемость).</summary>
    private async Task MonitorTerminalAsync(string mode, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try { await Task.Delay(TimeSpan.FromSeconds(5), ct); }
            catch (OperationCanceledException) { break; }
            await TryStatusAsync(mode, CancellationToken.None);
        }
    }

    private void SetState(string state)
    {
        lock (_gate)
        {
            if (_state == state) return;
            _state = state;
            _stateChangedAt = DateTimeOffset.UtcNow;
        }
        _metrics.ObserveState(state switch
        {
            StateRunning => CdcPollingMetrics.StateRunning,
            StateStopped => CdcPollingMetrics.StateStopped,
            _ => CdcPollingMetrics.StateRunning,
        });
    }

    private void RecoveredIfWas(string degradedState)
    {
        double? recoveryMs = null;
        lock (_gate)
        {
            if (_state == degradedState && _degradedSince is { } since)
            {
                _degradedSince = null;
                recoveryMs = _lastRecoveryMs = (DateTimeOffset.UtcNow - since).TotalMilliseconds;
                _recoveries++;
            }
        }
        if (recoveryMs is { } ms)
        {
            _metrics.RecoveryDurationMs.Record(ms);
            _logger.LogInformation("Доставка возобновлена после сбоя (время восстановления ~{Ms:F0} мс)", ms);
        }
    }

    private static async Task<bool> DelayCancellable(TimeSpan delay, CancellationToken ct)
    {
        try { await Task.Delay(delay, ct); return true; }
        catch (OperationCanceledException) { return false; }
    }

    private static string DescribeException(Exception ex) =>
        ex switch
        {
            PostgresException pe => $"{pe.SqlState}: {pe.MessageText}",
            _ => ex.Message,
        };

    /// <summary>Транзиентные для стенда сбои (как у Writer) + 55006: слот занят конкурентным peek.</summary>
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
        "55006",                                               // object_in_use: конкурентный peek/advance на слоте
    };

    private static string ClassifyKafkaError(Exception ex) => ex switch
    {
        TimeoutException => "timeout",
        { } e when IsProduceException(e) => "produce",
        _ => "other",
    };

    private static bool IsProduceException(Exception ex) =>
        ex.GetType().FullName?.Contains("ProduceException") == true ||
        ex.GetType().FullName?.Contains("KafkaFlowException") == true;
}
