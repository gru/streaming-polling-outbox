using KafkaValidator.Configuration;
using KafkaValidator.Metrics;

namespace KafkaValidator.Validation;

/// <summary>
/// Состояние сервиса и снимок для GET /api/validation/state. Коды состояний —
/// числа CDC-сервисов: starting → running (после старта KafkaFlow-шины;
/// консюмер группы без сохранения оффсетов уже читает topic с начала) →
/// stopped/failed.
/// </summary>
public sealed class ValidatorEngine
{
    private readonly KafkaValidatorMetrics _metrics;
    private int _stateCode = KafkaValidatorMetrics.StateStarting;
    private string? _lastError;

    public ValidatorEngine(KafkaValidatorMetrics metrics) => _metrics = metrics;

    public DateTimeOffset StartedAtUtc { get; } = DateTimeOffset.UtcNow;

    public DateTimeOffset StateChangedUtc { get; private set; } = DateTimeOffset.UtcNow;

    public string State => _stateCode switch
    {
        KafkaValidatorMetrics.StateRunning => "running",
        KafkaValidatorMetrics.StateStopped => "stopped",
        KafkaValidatorMetrics.StateFailed => "failed",
        _ => "starting",
    };

    public void MarkRunning()
    {
        SetState(KafkaValidatorMetrics.StateRunning);
    }

    public void MarkStopped()
    {
        SetState(KafkaValidatorMetrics.StateStopped);
    }

    public void MarkFailed(string error)
    {
        _lastError = error;
        SetState(KafkaValidatorMetrics.StateFailed);
    }

    private void SetState(int code)
    {
        _stateCode = code;
        StateChangedUtc = DateTimeOffset.UtcNow;
        _metrics.ObserveState(code);
    }

    public ValidatorStateSnapshot GetSnapshot(RunRegistry registry, KafkaValidatorOptions options) => new(
        State,
        StartedAtUtc,
        StateChangedUtc,
        new ConfigEcho(options.Kafka.Brokers, options.Kafka.Topic, options.Kafka.GroupId, options.Writer.BaseUrl),
        registry.RecordsTotal,
        registry.RunsSeen,
        registry.SkippedNoRunId,
        registry.ParseErrors,
        registry.LastRecordUtc,
        _lastError);
}

public sealed record ValidatorStateSnapshot(
    string State,
    DateTimeOffset StartedAtUtc,
    DateTimeOffset StateChangedUtc,
    ConfigEcho Config,
    long RecordsConsumed,
    int RunsSeen,
    long SkippedNoRunId,
    long ParseErrors,
    DateTimeOffset? LastRecordUtc,
    string? LastError);

public sealed record ConfigEcho(string Brokers, string Topic, string GroupId, string WriterBaseUrl);

/// <summary>
/// running выставляется ПОСЛЕ старта hosted-сервиса KafkaFlow (hosted-сервисы
/// стартуют в порядке регистрации — этот регистрируется позже). Готовность не
/// зависит от наличия записей: на пустом topic (свежий стенд) валидатор тоже
/// готов — просто ещё нечего сверять.
/// </summary>
public sealed class ValidatorLifecycleHostedService(ValidatorEngine engine) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        engine.MarkRunning();
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        engine.MarkStopped();
        return Task.CompletedTask;
    }
}
