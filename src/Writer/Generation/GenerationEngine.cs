using Writer.Metrics;

namespace Writer.Generation;

/// <summary>
/// Оркестратор прогона нагрузки. Стартует в Idle и не генерирует ничего, пока не
/// пришёл POST /api/generation/start (requirements.md 6.1); после рестарта процесса —
/// снова Idle (состояние только in-memory, авто-возобновления нет).
///
/// Модель параллельности: диспетчер создаёт батчи не чаще ratePerSecond-каданса
/// (batch/rate), число одновременно выполняемых батчей ограничено parallelism
/// (SemaphoreSlim). Мягкий stop прекращает диспетчеризацию и ожидает in-flight
/// батчи; начавшийся commit всегда доигрывается (interrupt ретрая → ambiguous).
/// </summary>
public sealed class GenerationEngine : IHostedService
{
    public const string StateIdle = "idle";
    public const string StateRunning = "running";
    public const string StateStopping = "stopping";
    public const string StateCompleted = "completed";
    public const string StateFailed = "failed";
    public const string StateStopped = "stopped";

    private readonly OutboxWriter _db;
    private readonly WriterMetrics _metrics;
    private readonly ILogger<GenerationEngine> _logger;

    private readonly object _gate = new();
    private string _state = StateIdle;
    private RunContext? _current;
    private RunResults? _lastResults;
    private string? _lastStopReason;

    public GenerationEngine(OutboxWriter db, WriterMetrics metrics, ILogger<GenerationEngine> logger)
    {
        _db = db;
        _metrics = metrics;
        _logger = logger;
        metrics.SetGenerationActiveProvider(() =>
            Volatile.Read(ref _state) is StateRunning or StateStopping ? 1 : 0);
    }

    public string State { get { lock (_gate) return _state; } }

    public bool IsRunning => State is StateRunning or StateStopping;

    public RunResults? LastResults { get { lock (_gate) return _lastResults; } }

    // ---------- IHostedService: старт в Idle, graceful shutdown ----------

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask; // Idle по требованиям 6.1

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await StopAsync(TimeSpan.FromSeconds(20));
        RunContext? ctx;
        lock (_gate) ctx = _current;
        if (ctx is not null && !ctx.RunTask.IsCompleted)
        {
            // прогон не уложился в мягкое окно — прерываем in-flight hard-отменой;
            // такие батчи попадут в AmbiguousUnresolved (исход commit неопределён)
            try { ctx.HardCts.Cancel(); } catch (ObjectDisposedException) { }
            try { await ctx.RunTask.WaitAsync(TimeSpan.FromSeconds(5)); } catch { }
        }
    }

    // ---------- Управление ----------

    public (bool Success, string Error) Start(LoadProfile profile)
    {
        lock (_gate)
        {
            if (_state is StateRunning or StateStopping)
                return (false, $"генерация уже выполняется (state={_state}, runId={_current?.Profile.RunId})");
            var ctx = new RunContext
            {
                Profile = profile,
                StartedAt = DateTimeOffset.UtcNow,
                HardCts = new CancellationTokenSource(),
                StopCts = new CancellationTokenSource(),
                Sem = new SemaphoreSlim(profile.Parallelism, profile.Parallelism),
            };
            _current = ctx;
            _state = StateRunning;
            ctx.RunTask = Task.Run(() => RunAsync(ctx));
            return (true, string.Empty);
        }
    }

    /// <summary>Мягкая остановка: прекратить диспетчеризацию, дождаться in-flight батчей.</summary>
    public async Task<RunResults?> StopAsync(TimeSpan waitTimeout)
    {
        RunContext? ctx;
        lock (_gate)
        {
            if (_state is not (StateRunning or StateStopping)) return _lastResults;
            _state = StateStopping;
            ctx = _current;
        }
        if (ctx is not null)
        {
            ctx.StopCts.Cancel();
            if (!ctx.RunTask.IsCompleted)
            {
                var done = await Task.WhenAny(ctx.RunTask, Task.Delay(waitTimeout));
                if (done != ctx.RunTask)
                    _logger.LogWarning("Прогон {RunId}: таймаут мягкой остановки ({Sec:F0}s), прогон ещё завершается",
                        ctx.Profile.RunId, waitTimeout.TotalSeconds);
            }
        }
        return LastResults;
    }

    public RunStateSnapshot GetState()
    {
        lock (_gate)
        {
            var snap = new RunStateSnapshot
            {
                State = _state,
                LastStopReason = _lastStopReason,
                RunId = _current?.Profile.RunId,
                Profile = _current?.Profile,
            };
            if (_current is { } ctx)
            {
                lock (ctx.Sync)
                {
                    var elapsed = (DateTimeOffset.UtcNow - ctx.StartedAt).TotalSeconds;
                    snap.StartedAt = ctx.StartedAt;
                    snap.ElapsedSeconds = elapsed;
                    snap.Dispatched = ctx.Dispatched;
                    snap.Written = ctx.Written;
                    snap.BytesWritten = ctx.Bytes;
                    snap.Ambiguous = ctx.Ambiguous;
                    snap.Failed = ctx.Failed;
                    snap.InFlight = ctx.Tasks.Count(t => !t.IsCompleted);
                    snap.LastError = ctx.FatalError ?? (ctx.ErrorMessages.Count > 0 ? ctx.ErrorMessages[^1] : null);
                    snap.ActualRatePerSecond = ctx.Written / Math.Max(elapsed, 0.001);
                }
            }
            else if (_lastResults is { } r)
            {
                snap.RunId = r.RunId;
                snap.StartedAt = r.StartedAt;
                snap.ElapsedSeconds = r.DurationSeconds;
                snap.Dispatched = r.DispatchedMessages;
                snap.Written = r.WrittenMessages;
                snap.BytesWritten = r.BytesWritten;
                snap.Ambiguous = r.AmbiguousUnresolved;
                snap.Failed = r.FailedMessages;
                snap.ActualRatePerSecond = r.ActualRatePerSecond;
            }
            return snap;
        }
    }

    public RunIdsDto? GetLastIds()
    {
        var r = LastResults;
        return r is null
            ? null
            : new RunIdsDto(r.RunId, r.WrittenIds.Count, r.AmbiguousIds.Count, r.FailedIds.Count,
                r.WrittenIds, r.AmbiguousIds, r.FailedIds);
    }

    // ---------- Прогон ----------

    private async Task RunAsync(RunContext ctx)
    {
        var p = ctx.Profile;
        _logger.LogInformation(
            "Прогон {RunId} стартовал: mode={Mode} rate={Rate} msg/s batch={Batch} par={Par} size={Size}B цели: count={Count} bytes={Bytes}B dur={Dur}s",
            p.RunId, p.ConnectionMode, p.RatePerSecond?.ToString() ?? "unlimited", p.BatchSize, p.Parallelism,
            p.MessageSizeBytes, p.TotalMessages?.ToString() ?? "-", p.TotalPayloadBytes?.ToString() ?? "-",
            p.DurationSeconds?.ToString() ?? "-");

        var avgBodyBytes = MessageFactory.EstimateBodyBytes(p.RunId, p.MessageSizeBytes);
        try
        {
            await DispatchLoopAsync(ctx, avgBodyBytes);
        }
        catch (OperationCanceledException) { /* hard-cancel во время диспетчеризации */ }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Прогон {RunId}: сбой диспетчера", p.RunId);
            lock (ctx.Sync)
            {
                ctx.Fatal = true;
                ctx.FatalError ??= $"engine: {ex.Message}";
            }
        }

        try { await Task.WhenAll(ctx.Tasks.ToArray()); } catch { /* ExecuteBatchAsync не бросает; на всякий случай */ }
        FinalizeRun(ctx);
    }

    private async Task DispatchLoopAsync(RunContext ctx, int avgBodyBytes)
    {
        var p = ctx.Profile;
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ctx.StopCts.Token, ctx.HardCts.Token);
        var token = linked.Token;

        // rate-каденс: один батч каждые batch/rate секунд; без rate — закрытый контур:
        // новый батч создаётся, как только освобождается слот parallelism
        PeriodicTimer? timer = null;
        var firstTick = true;
        if (p.RatePerSecond is { } rate && rate > 0)
            timer = new PeriodicTimer(TimeSpan.FromSeconds((double)p.BatchSize / rate));

        try
        {
            while (!token.IsCancellationRequested && !ctx.Fatal)
            {
                if (RunTargets.StopDispatch(p, Interlocked.Read(ref ctx.Dispatched), Interlocked.Read(ref ctx.DispatchedBytes),
                        DateTimeOffset.UtcNow, ctx.StartedAt))
                    break;

                var size = RunTargets.NextBatchSize(p, Interlocked.Read(ref ctx.Dispatched),
                    Interlocked.Read(ref ctx.DispatchedBytes), avgBodyBytes);
                if (size <= 0) break;

                try { await ctx.Sem.WaitAsync(token); }
                catch (OperationCanceledException) { break; }

                // пока ждали слот, условия могли измениться — перепроверяем
                if (token.IsCancellationRequested || ctx.Fatal ||
                    RunTargets.StopDispatch(p, Interlocked.Read(ref ctx.Dispatched), Interlocked.Read(ref ctx.DispatchedBytes),
                        DateTimeOffset.UtcNow, ctx.StartedAt))
                {
                    ctx.Sem.Release();
                    break;
                }

                var (messages, bytes) = MessageFactory.CreateBatch(p.RunId, size, p.MessageSizeBytes);
                Interlocked.Add(ref ctx.Dispatched, size);
                Interlocked.Add(ref ctx.DispatchedBytes, bytes);
                var task = Task.Run(() => ExecuteBatchAsync(ctx, messages, bytes));
                lock (ctx.Sync) ctx.Tasks.Add(task);

                if (timer is not null)
                {
                    if (firstTick) { firstTick = false; continue; } // первый батч без задержки
                    try { if (!await timer.WaitForNextTickAsync(token)) break; }
                    catch (OperationCanceledException) { break; }
                }
            }
        }
        finally
        {
            timer?.Dispose();
        }
    }

    private async Task ExecuteBatchAsync(RunContext ctx, OutboxMessage[] messages, long bodyBytes)
    {
        try
        {
            var res = await _db.WriteBatchAsync(ctx.Profile.ConnectionMode, messages, ctx.HardCts.Token, ctx.StopCts.Token);
            var ids = messages.Select(m => m.Id).ToArray();
            var reconnects = Math.Max(0, res.Attempts - 1);

            lock (ctx.Sync)
            {
                ctx.Batches++;
                ctx.Reconnects += reconnects;
                switch (res.Outcome)
                {
                    case BatchOutcome.Committed:
                    case BatchOutcome.AlreadyCommitted:
                        ctx.Written += messages.Length;
                        ctx.Bytes += bodyBytes;
                        ctx.Commits++;
                        // у AlreadyCommitted реальная длительность commit прошлой попытки неизвестна —
                        // в выборку commit-латентности не попадает, только в batch-длительность
                        if (res.Outcome == BatchOutcome.Committed) ctx.CommitMs.Add(res.CommitDurationMs);
                        ctx.BatchMs.Add(res.BatchDurationMs);
                        ctx.WrittenIds.AddRange(ids);
                        if (res.Outcome == BatchOutcome.AlreadyCommitted) ctx.AlreadyCommitted += messages.Length;
                        if (res.LastTransientError is not null)
                            ctx.ErrorMessages.Add($"{DateTimeOffset.UtcNow:O} transient→resolved: {Trunc(res.LastTransientError)}");
                        break;

                    case BatchOutcome.Cancelled:
                        ctx.Ambiguous += messages.Length;
                        ctx.AmbiguousIds.AddRange(ids);
                        ctx.ErrorMessages.Add($"{DateTimeOffset.UtcNow:O} AMBIGUOUS (остановка в retry): {Trunc(res.LastTransientError ?? "")}");
                        break;

                    case BatchOutcome.Fatal:
                        ctx.Failed += messages.Length;
                        ctx.FailedIds.AddRange(ids);
                        ctx.Fatal = true;
                        ctx.FatalError ??= res.FatalError ?? "unknown fatal";
                        ctx.ErrorMessages.Add($"{DateTimeOffset.UtcNow:O} FATAL: {Trunc(res.FatalError ?? "unknown")}");
                        break;
                }
                if (ctx.ErrorMessages.Count > 20) ctx.ErrorMessages.RemoveRange(0, ctx.ErrorMessages.Count - 20);
            }

            // метрики — вне lock
            _metrics.Reconnects.Add(reconnects);
            switch (res.Outcome)
            {
                case BatchOutcome.Committed:
                case BatchOutcome.AlreadyCommitted:
                    _metrics.MessagesWritten.Add(messages.Length);
                    _metrics.BytesWritten.Add(bodyBytes);
                    _metrics.BatchDurationMs.Record(res.BatchDurationMs);
                    if (res.Outcome == BatchOutcome.Committed)
                        _metrics.CommitDurationMs.Record(res.CommitDurationMs);
                    break;
                case BatchOutcome.Cancelled:
                    _metrics.AmbiguousCommits.Add(1);
                    _metrics.WriteErrors.Add(1, new KeyValuePair<string, object?>("reason", "ambiguous"));
                    break;
                case BatchOutcome.Fatal:
                    _metrics.WriteErrors.Add(1, new KeyValuePair<string, object?>("reason", "fatal"));
                    break;
            }

            if (res.Outcome == BatchOutcome.Fatal)
                _logger.LogError("Прогон {RunId}: фатальная ошибка записи батча ({Count} сообщений): {Error}",
                    ctx.Profile.RunId, messages.Length, res.FatalError);
        }
        catch (Exception ex)
        {
            // страховка: ExecuteBatchAsync не должен бросать, иначе зависнет слот семафора
            _logger.LogError(ex, "Прогон {RunId}: необработанное исключение в ExecuteBatchAsync", ctx.Profile.RunId);
            lock (ctx.Sync)
            {
                ctx.Fatal = true;
                ctx.FatalError ??= $"engine(batch): {ex.Message}";
                ctx.Failed += messages.Length;
                ctx.FailedIds.AddRange(messages.Select(m => m.Id));
                ctx.ErrorMessages.Add($"{DateTimeOffset.UtcNow:O} FATAL(engine): {Trunc(ex.Message)}");
            }
            _metrics.WriteErrors.Add(1, new KeyValuePair<string, object?>("reason", "fatal"));
        }
        finally
        {
            ctx.Sem.Release();
        }
    }

    private void FinalizeRun(RunContext ctx)
    {
        var p = ctx.Profile;
        DateTimeOffset finished;
        long written, bytes, dispatched, alreadyCommitted, ambiguous, failed;
        int batches, commits, reconnects;
        List<Guid> writtenIds, ambiguousIds, failedIds;
        List<double> commitMs, batchMs;
        List<string> errors;
        string? fatalError;

        lock (ctx.Sync)
        {
            finished = DateTimeOffset.UtcNow;
            written = ctx.Written; bytes = ctx.Bytes; dispatched = ctx.Dispatched;
            alreadyCommitted = ctx.AlreadyCommitted; ambiguous = ctx.Ambiguous; failed = ctx.Failed;
            batches = ctx.Batches; commits = ctx.Commits; reconnects = ctx.Reconnects;
            writtenIds = ctx.WrittenIds; ambiguousIds = ctx.AmbiguousIds; failedIds = ctx.FailedIds;
            commitMs = ctx.CommitMs; batchMs = ctx.BatchMs; errors = new List<string>(ctx.ErrorMessages);
            fatalError = ctx.FatalError;
        }

        var durationSec = Math.Max((finished - ctx.StartedAt).TotalSeconds, 0.001);
        var target = RunTargets.ReachedTarget(p, written, bytes, finished, ctx.StartedAt);

        string state, reason;
        if (fatalError is not null)
        {
            state = StateFailed;
            reason = "FatalError";
        }
        else if (ctx.HardCts.IsCancellationRequested)
        {
            state = StateStopped;
            reason = "HostShutdown";
        }
        else if (ctx.StopCts.IsCancellationRequested)
        {
            // stop мог прийтись на момент достижения цели — честно отражаем achieved-цель
            state = target is not null ? StateCompleted : StateStopped;
            reason = target ?? "UserStop";
        }
        else
        {
            state = StateCompleted;
            reason = target ?? (p.RunUntilStopped ? "UserStop" : "TargetMessages");
        }

        var (cp50, cp95, cp99) = Percentiles.P50P95P99(commitMs);
        var (bp50, bp95, bp99) = Percentiles.P50P95P99(batchMs);
        var results = new RunResults
        {
            RunId = p.RunId,
            ConnectionMode = p.ConnectionMode,
            Profile = p,
            State = state,
            StopReason = reason,
            StartedAt = ctx.StartedAt,
            FinishedAt = finished,
            DurationSeconds = durationSec,
            DispatchedMessages = dispatched,
            WrittenMessages = written,
            AlreadyCommittedDuplicates = alreadyCommitted,
            AmbiguousUnresolved = ambiguous,
            FailedMessages = failed,
            BytesWritten = bytes,
            Batches = batches,
            Commits = commits,
            Reconnects = reconnects,
            CommitMeanMs = commitMs.Count > 0 ? commitMs.Average() : 0,
            CommitP50Ms = cp50,
            CommitP95Ms = cp95,
            CommitP99Ms = cp99,
            BatchMeanMs = batchMs.Count > 0 ? batchMs.Average() : 0,
            BatchP50Ms = bp50,
            BatchP95Ms = bp95,
            BatchP99Ms = bp99,
            ActualRatePerSecond = written / durationSec,
            ActualBytesPerSecond = bytes / durationSec,
            Errors = errors,
        };
        writtenIds.ForEach(results.WrittenIds.Add);
        ambiguousIds.ForEach(results.AmbiguousIds.Add);
        failedIds.ForEach(results.FailedIds.Add);

        lock (_gate)
        {
            _lastResults = results;
            _lastStopReason = reason;
            _current = null;
            _state = state;
        }

        _logger.LogInformation(
            "Прогон {RunId} завершён: {State} ({Reason}): written={Written} ({Bytes} B) за {Dur:F1}s = {Rate:F1} msg/s; alreadyCommitted={AC} ambiguous={Amb} failed={Fail}; commit p50/p95/p99 = {P50:F1}/{P95:F1}/{P99:F1} ms; reconnects={Re}",
            p.RunId, state, reason, written, bytes, durationSec, results.ActualRatePerSecond,
            alreadyCommitted, ambiguous, failed, cp50, cp95, cp99, reconnects);
    }

    private static string Trunc(string s) => s.Length <= 300 ? s : s[..300];

    /// <summary>Мутабельное состояние одного прогона; после финализации больше не меняется.</summary>
    private sealed class RunContext
    {
        public required LoadProfile Profile { get; init; }
        public required CancellationTokenSource HardCts { get; init; }
        public required CancellationTokenSource StopCts { get; init; }
        public required SemaphoreSlim Sem { get; init; }
        public DateTimeOffset StartedAt { get; init; }
        public Task RunTask { get; set; } = Task.CompletedTask;

        public readonly object Sync = new();
        public volatile bool Fatal;
        public string? FatalError;

        public long Dispatched, DispatchedBytes, Written, Bytes, AlreadyCommitted, Ambiguous, Failed;
        public int Batches, Commits, Reconnects;

        public readonly List<Guid> WrittenIds = new();
        public readonly List<Guid> AmbiguousIds = new();
        public readonly List<Guid> FailedIds = new();
        public readonly List<double> CommitMs = new();
        public readonly List<double> BatchMs = new();
        public readonly List<string> ErrorMessages = new();
        public readonly List<Task> Tasks = new();
    }
}
