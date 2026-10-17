using System.Diagnostics;
using System.Net.Sockets;
using System.Threading.Channels;
using CdcCommon;
using CdcCommon.PgOutput;
using CdcStreaming.Configuration;
using CdcStreaming.Kafka;
using CdcStreaming.Metrics;
using CdcStreaming.Replication;
using Microsoft.Extensions.Options;
using Npgsql;
using Npgsql.Replication;

namespace CdcStreaming.Streaming;

/// <summary>
/// Цикл CDC Streaming (requirements.md 8, 10): StartReplication (pgoutput) →
/// декод → публикация в Kafka → подтверждение доставки → replication feedback
/// (SetReplicationStatus + SendStatusUpdate). Ключевой инвариант at-least-once:
/// LSN подтверждается ТОЛЬКО после delivery-репортов Kafka по всему flush-набору;
/// при недоступности Kafka подтверждения нет и WAL копится; при сбое между
/// доставкой и подтверждением сообщения будут доставлены повторно (дубликаты
/// допустимы, консюмер дедуплицирует по id).
///
/// Архитектура двух циклов (push-стрим нельзя «спросить, есть ли ещё» без
/// блокировки — поэтому данные и публикация разнесены через bounded-канал):
///   receiver  — тянет replication-стрим, декодирует, кладёт ЗАВЕРШЁННЫЕ
///               транзакции в канал; при заполнении канала стоит в WriteAsync
///               → TCP backpressure → walsender блокируется → WAL копится
///               на сервере (§10, TEST-HA-003). В Kafka ничего не пишет.
///   publisher — набирает flush-набор из канала (до Kafka.BatchSize событий
///               или до опустошения канала — «drain-or-batch»: под нагрузкой
///               полные батчи с конвейеризацией ack'ов, на trickling-нагрузке
///               мгновенная публикация одиночек), публикует, ждёт acks и
///               подтверждает LSN последнего закоммиченного набора.
/// Порядок событий сохранён (один канал, один publisher); подтверждение
/// выставляется только после публикации всего предшествующего — редоставка
/// после сбоя между доставкой и подтверждением допустима.
///
/// Состояния:
///   running           — стрим идёт;
///   waiting_database  — транзиентный сбой replication-соединения (обрыв,
///                       Patroni failover/switchover) — backoff и переподключение;
///   waiting_kafka     — Kafka недоступна — повтор публикации без подтверждения;
///   error_slot        — слот инвалидирован (wal_status=lost или удалён): ТЕРМИНАЛЬНОЕ
///                       состояние с диагностикой, слот не пересоздаётся и потери
///                       не скрываются (requirements.md 10, 12.3);
///   failed            — неустранимая ошибка данных/протокола (битый pgoutput);
///   stopped           — graceful shutdown.
/// </summary>
public sealed class CdcStreamingEngine : BackgroundService
{
    public const string StateStarting = "starting";
    public const string StateRunning = "running";
    public const string StateWaitingDatabase = "waiting_database";
    public const string StateWaitingKafka = "waiting_kafka";
    public const string StateErrorSlot = "error_slot";
    public const string StateFailed = "failed";
    public const string StateStopped = "stopped";

    private const int E2eSampleLimit = 4096;

    private readonly CdcStreamingOptions _options;
    private readonly ReplicationGateway _gateway;
    private readonly KafkaPublisher _publisher;
    private readonly CdcStreamingMetrics _metrics;
    private readonly ILogger<CdcStreamingEngine> _logger;

    private readonly object _gate = new();
    private readonly CancellationTokenSource _internalCts = new(); // фатал: останавливает оба цикла

    private string _state = StateStarting;
    private DateTimeOffset _startedAt;
    private DateTimeOffset? _stateChangedAt;
    private DateTimeOffset? _degradedSince;

    private long _messagesRead, _messagesDelivered, _recordsDecoded, _streamSessions, _publishBatches;
    private long _readErrors, _kafkaErrors, _retries, _reconnects;
    private int _recoveries;
    private int _incompleteEvents;
    private double? _lastPublishMs, _lastRecoveryMs;
    private Lsn? _lastReceived, _confirmed;
    private long? _walLag;
    private string? _lastError, _lastKafkaError;
    private SlotStatus? _slot;
    private readonly Queue<double> _e2eSamples = new();
    private DateTimeOffset _lastStatusRefresh;

    /// <summary>Активное replication-соединение (publisher шлёт по нему feedback).</summary>
    private LogicalReplicationConnection? _activeConnection;

    private Channel<FlushItem>? _channel;

    public CdcStreamingEngine(
        IOptions<CdcStreamingOptions> options,
        ReplicationGateway gateway,
        KafkaPublisher publisher,
        CdcStreamingMetrics metrics,
        ILogger<CdcStreamingEngine> logger)
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
                    Host = _options.Connection.Endpoint.Host,
                    Port = _options.Connection.Endpoint.Port,
                    Database = _options.Connection.Database.Name,
                    Username = _options.Connection.Database.Username,
                    SlotName = _options.Streaming.SlotName,
                    Publication = _options.Streaming.Publication,
                    ProtocolVersion = _options.Streaming.ProtocolVersion,
                    ChannelCapacity = _options.Streaming.ClampChannelCapacity(),
                    KafkaBatchSize = _options.Kafka.BatchSize,
                    KafkaTopic = _options.Kafka.Topic,
                    KafkaBrokers = _options.Kafka.Brokers,
                },
                MessagesRead = _messagesRead,
                MessagesDelivered = _messagesDelivered,
                RecordsDecoded = _recordsDecoded,
                StreamSessions = _streamSessions,
                PublishBatches = _publishBatches,
                ReadErrors = _readErrors,
                KafkaErrors = _kafkaErrors,
                Retries = _retries,
                Reconnects = _reconnects,
                IncompleteTransactionEvents = _incompleteEvents,
                ChannelDepth = _channel?.Reader.Count ?? 0,
                LastReceivedLsn = _lastReceived?.ToString(),
                ConfirmedLsn = _confirmed?.ToString(),
                WalLagBytes = _walLag,
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
        var kafkaBatch = Math.Max(1, _options.Kafka.BatchSize);
        var channelCapacity = _options.Streaming.ClampChannelCapacity();
        var backoffBase = Math.Max(50, _options.Retry.BaseDelayMs);
        var backoffCap = Math.Max(backoffBase, _options.Retry.MaxDelayMs);

        lock (_gate) _startedAt = DateTimeOffset.UtcNow;
        _logger.LogInformation(
            "CDC Streaming запускается: {Host}:{Port}/{Db} user={User} slot={Slot} publication={Publication} proto=v{Proto} " +
            "kafkaBatch={KafkaBatch} channelCapacity={ChannelCapacity} topic={Topic} brokers={Brokers} retry={Base}..{Cap}ms",
            _options.Connection.Endpoint.Host, _options.Connection.Endpoint.Port, _options.Connection.Database.Name,
            _options.Connection.Database.Username, _options.Streaming.SlotName, _options.Streaming.Publication,
            _options.Streaming.ProtocolVersion, kafkaBatch, channelCapacity, _options.Kafka.Topic, _options.Kafka.Brokers,
            backoffBase, backoffCap);

        _channel = Channel.CreateBounded<FlushItem>(new BoundedChannelOptions(channelCapacity)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait, // backpressure: receiver ждёт, стрим не читается
        });

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, _internalCts.Token);
        var receiver = ReceiveLoopAsync(_channel.Writer, linked.Token, backoffBase, backoffCap);
        var publisher = PublishLoopAsync(_channel.Reader, linked.Token, kafkaBatch, backoffBase, backoffCap);
        try
        {
            await Task.WhenAll(receiver, publisher);
        }
        catch (OperationCanceledException)
        {
        }

        lock (_gate) { _stateChangedAt ??= DateTimeOffset.UtcNow; }
        if (State is StateErrorSlot or StateFailed)
        {
            // Терминальная ошибка уже выставлена (error_slot/failed): остаёмся живыми
            // для наблюдаемости, статус слота обновляем (как у Polling)
            await MonitorTerminalAsync(stoppingToken);
            return;
        }

        SetState(StateStopped);
        _logger.LogInformation("CDC Streaming остановлен (graceful shutdown)");
    }

    // ---------- receiver: replication-стрим → канал ----------

    private async Task ReceiveLoopAsync(ChannelWriter<FlushItem> writer, CancellationToken ct, int backoffBase, int backoffCap)
    {
        var attempts = 0;
        while (!ct.IsCancellationRequested)
        {
            LogicalReplicationConnection? connection = null;
            var sessionStartedAt = DateTimeOffset.UtcNow;
            try
            {
                connection = await _gateway.OpenReplicationConnectionAsync(ct);
                var stream = _gateway.StartReplication(connection, ct);
                var decoder = new PgOutputSessionDecoder();
                lock (_gate)
                {
                    _activeConnection = connection;
                    _streamSessions++;
                }
                // порядок как у Polling: сначала фиксируем recovery деградации, затем running
                RecoveredIfWas(StateWaitingDatabase);
                SetState(StateRunning);
                _logger.LogInformation(
                    "Replication-стрим открыт (slot={Slot}, publication={Publication}); продолжение от confirmed_flush слота",
                    _options.Streaming.SlotName, _options.Streaming.Publication);

                await foreach (var message in stream.WithCancellation(ct))
                {
                    var decoded = await decoder.ProcessAsync(message, ct);

                    lock (_gate) _recordsDecoded++;
                    _metrics.AddRecord(decoded.Kind);

                    // WalEnd каждого сообщения — актуальный конец WAL на сервере:
                    // это «полученная» позиция (received), НЕ подтверждённая
                    lock (_gate)
                    {
                        _lastReceived = LsnConvert.FromNpgsql(message.WalEnd);
                        _incompleteEvents = decoder.IncompleteEventCount;
                    }
                    _metrics.ObserveLastReceived(LsnConvert.FromNpgsql(message.WalEnd));

                    if (decoded.Completed is { } item)
                    {
                        lock (_gate) _messagesRead += item.Events.Count;
                        _metrics.MessagesRead.Add(item.Events.Count);
                        _metrics.ObserveChannelDepth(_channel?.Reader.Count ?? 0);
                        // Заполненный канал → ожидание здесь → стрим не читается →
                        // TCP backpressure → WAL копится (подтверждения нет)
                        await writer.WriteAsync(item, ct);
                    }
                }
                // Сервер закрыл стрим без ошибки (неожидаемо, но не фатально) — переподключаемся;
                // сессия доработала до конца — считаем backoff с начала
                _logger.LogWarning("Replication-стрим завершился без ошибки — переподключаюсь");
                attempts = 0;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (PgOutputParseException ex)
            {
                await DisposeConnectionQuietly(connection);
                EnterFatal(StateFailed, $"pgoutput decode: {ex.Message}");
                break;
            }
            catch (InvalidCastException ex)
            {
                // Get<string> на значении неожидаемого вида — ошибка декодирования
                // (не транзиентная): повторный стрим вернёт те же сообщения
                await DisposeConnectionQuietly(connection);
                EnterFatal(StateFailed, $"pgoutput value: {ex.Message}");
                break;
            }
            catch (Exception ex)
            {
                var status = await TryStatusAsync(CancellationToken.None);
                if (status?.IsInvalidated == true)
                {
                    await DisposeConnectionQuietly(connection);
                    EnterFatal(StateErrorSlot, $"разрыв стрима: {DescribeException(ex)}", status);
                    break;
                }
                var transient = IsTransient(ex);
                // Сессия, прожившая достаточно, считается здоровой: следующий сбой
                // начинается с минимального backoff; короткоживущие сессии копят
                // попытки — никакого бесконечного быстрого цикла переподключений (§8.2)
                if (IsLongSession(sessionStartedAt)) attempts = 0;
                lock (_gate)
                {
                    _state = StateWaitingDatabase;
                    if (_degradedSince is null) _degradedSince = DateTimeOffset.UtcNow;
                    _stateChangedAt ??= DateTimeOffset.UtcNow;
                    _readErrors++;
                    _reconnects++;
                    _lastError = DescribeException(ex);
                    if (_activeConnection == connection) _activeConnection = null;
                }
                _metrics.ObserveState(CdcStreamingMetrics.StateWaitingDatabase);
                _metrics.ReadErrors.Add(1, new KeyValuePair<string, object?>("reason", transient ? "transient" : "fatal"));
                _metrics.Reconnects.Add(1);
                if (!transient)
                    _logger.LogError(ex, "Нетранзиентная ошибка replication-соединения — повтор с backoff");
                else
                    _logger.LogWarning("Сбой replication-соединения (транзиентный, попытка {Attempt}): {Error}",
                        attempts + 1, DescribeException(ex));
                await DisposeConnectionQuietly(connection);
                if (!await DelayCancellable(Backoff.Delay(++attempts, backoffBase, backoffCap), ct))
                    break;
            }
        }

        writer.TryComplete(); // publisher доберёт записанное и выйдет без подтверждения непубликованного
    }

    // ---------- publisher: канал → Kafka → replication feedback ----------

    private async Task PublishLoopAsync(ChannelReader<FlushItem> reader, CancellationToken ct, int kafkaBatch, int backoffBase, int backoffCap)
    {
        var kafkaAttempts = 0;
        var buffer = new List<OutboxEvent>();
        while (true)
        {
            try
            {
                // false = writer завершен (shutdown/фатал): записанное добираем ниже и выходим
                if (!await reader.WaitToReadAsync(ct))
                {
                    // drain без публикации не подтверждаем — редоставка после рестарта
                    break;
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }

            // drain-or-batch: набираем до kafkaBatch событий или до опустошения канала
            buffer.Clear();
            Lsn? confirmTarget = null;
            while (buffer.Count < kafkaBatch && reader.TryRead(out var item))
            {
                buffer.AddRange(item.Events);
                // items идут в порядке коммитов — последний Target и есть подтверждаемая позиция
                confirmTarget = item.TransactionEndLsn;
            }
            if (confirmTarget is null)
                continue;
            _metrics.ObserveChannelDepth(reader.Count);

            if (buffer.Count == 0)
            {
                // Только «пустые» транзакции (BEGIN..COMMIT без событий): публиковать нечего,
                // но их TransactionEndLsn проталкиваем в подтверждение — иначе слот стоит
                await ConfirmAsync(confirmTarget.Value);
                await RefreshStatusIfNeededAsync();
                continue;
            }

            var publishSw = Stopwatch.StartNew();
            var delivered = false;
            while (true)
            {
                try
                {
                    foreach (var chunk in buffer.Chunk(kafkaBatch))
                    {
                        // ProduceAsync синхронно ставит сообщение в буфер продюсера и возвращает
                        // Task на delivery-репорт: запускаем весь чанк до первого await, чтобы
                        // ack'и конвейеризировались (последовательное await платило бы RTT за
                        // каждое сообщение) и librdkafka собирал per-partition батчи. Порядок в
                        // партиции сохраняется: key=id + идемпотентный продюсер.
                        var tasks = new Task[chunk.Length];
                        for (var i = 0; i < chunk.Length; i++)
                            tasks[i] = PublishOneAsync(chunk[i]);
                        // WaitAsync: при остановке хоста прекращаем ожидание; доставка не
                        // подтверждена ⇒ LSN не подтверждается (после рестарта — повторная доставка)
                        await Task.WhenAll(tasks).WaitAsync(ct);
                    }
                    delivered = true;
                    break;
                }
                catch (OperationCanceledException)
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
                        _stateChangedAt ??= DateTimeOffset.UtcNow;
                        _kafkaErrors++;
                        _retries++;
                        _lastKafkaError = DescribeException(ex);
                    }
                    _metrics.ObserveState(CdcStreamingMetrics.StateWaitingKafka);
                    _metrics.KafkaErrors.Add(1, new KeyValuePair<string, object?>("reason", reason));
                    _metrics.Retries.Add(1);
                    _logger.LogWarning(
                        "Kafka недоступна/ошибка публикации (попытка {Attempt}, reason={Reason}); LSN не подтверждается, ожидаю восстановления: {Error}",
                        kafkaAttempts, reason, DescribeException(ex));

                    // Ждём с backoff, периодически проверяя слот: канал тем временем
                    // заполняется → receiver стоит → WAL копится — при переполнении
                    // max_slot_wal_keep_size слот инвалидируется (requirements.md 12)
                    if (!await WaitKafkaAsync(Backoff.Delay(kafkaAttempts, backoffBase, backoffCap), ct))
                        break;
                    var status = await TryStatusAsync(CancellationToken.None);
                    if (status?.IsInvalidated == true)
                    {
                        EnterFatal(StateErrorSlot, "слот инвалидирован во время ожидания Kafka", status);
                        return;
                    }
                }
            }

            if (!delivered)
                break; // отмена — подтверждение не выполняем

            kafkaAttempts = 0;
            RecoveredIfWas(StateWaitingKafka);
            lock (_gate)
            {
                _publishBatches++;
                _lastPublishMs = publishSw.Elapsed.TotalMilliseconds;
            }
            _metrics.PublishBatchDurationMs.Record(publishSw.Elapsed.TotalMilliseconds);
            _metrics.PublishBatchSize.Record(buffer.Count);

            // Подтверждение ТОЛЬКО после acks всего набора (requirements.md 8.1, 10)
            await ConfirmAsync(confirmTarget.Value);
            await RefreshStatusIfNeededAsync();
        }
    }

    /// <summary>
    /// Replication feedback: выставить подтверждаемую позицию и вытолкнуть статус
    /// на сервер немедленно (SendStatusUpdate — 39-байтное сообщение; иначе сервер
    /// узнает с ближайшим keepalive-запросом или таймером Npgsql ~10 с).
    /// Сетевая ошибка здесь не фатальна: позиция догонит следующим флашем.
    /// </summary>
    private async Task ConfirmAsync(Lsn confirmTarget)
    {
        LogicalReplicationConnection? connection;
        lock (_gate)
        {
            connection = _activeConnection;
            if (_confirmed is null || confirmTarget > _confirmed)
                _confirmed = confirmTarget;
        }
        _metrics.ObserveConfirmed(confirmTarget);
        if (connection is null)
            return; // между reconnect'ами: следующий флаш подтвердит большую позицию
        try
        {
            connection.SetReplicationStatus(LsnConvert.ToNpgsql(confirmTarget));
            await connection.SendStatusUpdate(CancellationToken.None);
        }
        catch (Exception ex)
        {
            _logger.LogDebug("SendStatusUpdate не прошёл (догонит следующим флашем/таймером): {Error}", ex.Message);
        }
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
    private async Task<bool> WaitKafkaAsync(TimeSpan delay, CancellationToken ct)
    {
        var deadline = DateTimeOffset.UtcNow + delay;
        while (true)
        {
            var now = DateTimeOffset.UtcNow;
            if (now >= deadline) return true;
            _lastStatusRefresh = now;
            await TryStatusAsync(ct); // ошибки игнорируются — БД может быть недоступна одновременно
            var chunk = TimeSpan.FromMilliseconds(Math.Min(2000, Math.Max(50, (deadline - now).TotalMilliseconds)));
            try { await Task.Delay(chunk, ct); }
            catch (OperationCanceledException) { return false; }
        }
    }

    private async Task RefreshStatusIfNeededAsync()
    {
        if (DateTimeOffset.UtcNow - _lastStatusRefresh < TimeSpan.FromSeconds(1))
            return;
        var status = await TryStatusAsync(CancellationToken.None);
        if (status?.IsInvalidated == true)
            EnterFatal(StateErrorSlot, "слот инвалидирован (обнаружено периодической проверкой)", status);
    }

    private async Task<SlotStatus?> TryStatusAsync(CancellationToken ct)
    {
        try
        {
            var status = await _gateway.SlotStatusAsync(ct);
            if (status is not null)
            {
                lock (_gate) _slot = status;
                if (status.WalLagBytes is { } lag)
                {
                    lock (_gate) _walLag = lag;
                    _metrics.ObserveWalLag(lag);
                }
                _lastStatusRefresh = DateTimeOffset.UtcNow;
                if (status.ConfirmedFlushLsn is { } confirmed)
                {
                    lock (_gate) _confirmed ??= confirmed; // после рестарта берём из слота; далее только растёт
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
    /// Терминальное состояние (failed/error_slot): останавливаем оба цикла,
    /// БЕЗ пересоздания слота и без заявлений об отсутствии потерь (requirements.md 10).
    /// Контейнер живёт, состояние наблюдаемо через /api/cdc/state и /ready (503);
    /// восстановление — только ручное + рестарт контейнера.
    /// </summary>
    private void EnterFatal(string state, string reason, SlotStatus? status = null)
    {
        lock (_gate)
        {
            _state = state;
            _stateChangedAt = DateTimeOffset.UtcNow;
            _degradedSince = null;
            _readErrors++;
            _lastError = state == StateErrorSlot ? $"слот инвалидирован: {reason}" : reason;
            if (status is not null) _slot = status;
        }
        _metrics.ObserveState(state == StateErrorSlot ? CdcStreamingMetrics.StateErrorSlot : CdcStreamingMetrics.StateFailed);
        _metrics.ReadErrors.Add(1, new KeyValuePair<string, object?>("reason", "fatal"));
        _logger.LogCritical(
            "ТЕРМИНАЛЬНАЯ ошибка CDC Streaming ({State}): {Reason}. Статус: {@Status}. " +
            "Слот не пересоздаётся автоматически; подтверждение позиции не происходит. Данные, чей WAL утрачен, " +
            "НЕ доставлены и НЕ могут быть доставлены из этого слота (requirements.md 12.3). Требуется вмешательство.",
            state, reason, status);
        _internalCts.Cancel();
    }

    /// <summary>После терминальной ошибки остаёмся живыми и обновляем статус слота (наблюдаемость).</summary>
    private async Task MonitorTerminalAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try { await Task.Delay(TimeSpan.FromSeconds(5), ct); }
            catch (OperationCanceledException) { break; }
            await TryStatusAsync(CancellationToken.None);
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
            StateRunning => CdcStreamingMetrics.StateRunning,
            _ => CdcStreamingMetrics.StateRunning,
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
                _state = StateRunning; // в отличие от Polling, у стриминга нет «следующего peek» — возвращаем running здесь
                recoveryMs = _lastRecoveryMs = (DateTimeOffset.UtcNow - since).TotalMilliseconds;
                _recoveries++;
            }
        }
        if (recoveryMs is { } ms)
        {
            _metrics.ObserveState(CdcStreamingMetrics.StateRunning);
            _metrics.RecoveryDurationMs.Record(ms);
            _logger.LogInformation("Доставка возобновлена после сбоя (время восстановления ~{Ms:F0} мс)", ms);
        }
    }

    private static async Task<bool> DelayCancellable(TimeSpan delay, CancellationToken ct)
    {
        try { await Task.Delay(delay, ct); return true; }
        catch (OperationCanceledException) { return false; }
    }

    /// <summary>Сессия, прожившая ≥30 с, сбрасывает счётчик backoff (анти-«быстрый цикл переподключений», §8.2).</summary>
    private static bool IsLongSession(DateTimeOffset sessionStartedAt) =>
        DateTimeOffset.UtcNow - sessionStartedAt >= TimeSpan.FromSeconds(30);

    private static async Task DisposeConnectionQuietly(LogicalReplicationConnection? connection)
    {
        if (connection is null) return;
        try { await connection.DisposeAsync(); }
        catch { /* соединение уже разорвано */ }
    }

    private static string DescribeException(Exception ex) =>
        ex switch
        {
            PostgresException pe => $"{pe.SqlState}: {pe.MessageText}",
            _ => ex.Message,
        };

    /// <summary>
    /// Транзиентные для стенда сбои (как у Polling) + специфика стрима:
    /// 55006 — слот ещё активен на старом walsender (failover-перекрытие),
    /// 25006 — read-only: HAProxy успел отдать соединение demoted-ноде.
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
        "25006",                                               // read_only_sql_transaction: соединение с demoted-нодой через HAProxy
        "40001", "40P01",                                      // serialization_failure, deadlock_detected
        "55P03",                                               // lock_not_available
        "54000",                                               // out_of_memory / program limit
        "55006",                                               // object_in_use: слот ещё активен на прежнем walsender
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
