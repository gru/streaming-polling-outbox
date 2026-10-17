using System.Diagnostics.Metrics;

namespace KafkaValidator.Metrics;

/// <summary>
/// Метрики KafkaValidator (requirements.md 13.2/13.3). Кардинальность
/// фиксированная: runId и message id как label ЗАПРЕЩЕНЫ (requirements.md 13.3) —
/// счётчики глобальные, детализация по прогонам доступна через HTTP API
/// (/api/validation/runs, /api/validation/validate) из фактических данных,
/// а не через Prometheus (requirements.md 13.4: scrape не гарантирует
/// сохранение кратковременных событий).
/// </summary>
public sealed class KafkaValidatorMetrics : IDisposable
{
    public const string MeterName = "StreamingPollingOutbox.KafkaValidator";

    private readonly Meter _meter = new(MeterName);
    private int _stateCode = StateStarting;
    private int _runsSeen;

    // Коды состояний — те же числа, что у CDC-сервисов (starting/running/stopped/failed)
    public const int StateStarting = 0;
    public const int StateRunning = 1;
    public const int StateStopped = 5;
    public const int StateFailed = 6;

    public Counter<long> RecordsConsumed { get; }
    public Counter<long> ParseErrors { get; }
    public Counter<long> SkippedRecords { get; }
    public Counter<long> Validations { get; }

    /// <summary>Число прогонов в реестре на момент scrape (реестр — источник правды).</summary>
    public Func<int>? RunsSeenProvider { get; set; }

    public KafkaValidatorMetrics()
    {
        RecordsConsumed = _meter.CreateCounter<long>(
            "kafka_validator.records_consumed", description: "Записи topic, прочитанные валидатором (включая перемотку после рестарта)");
        ParseErrors = _meter.CreateCounter<long>(
            "kafka_validator.parse_errors", description: "Записи с битым конвертом: reason=malformed");
        SkippedRecords = _meter.CreateCounter<long>(
            "kafka_validator.skipped_records", description: "Валидные записи без body.runId (ручные INSERT и т.п.): reason=no_run_id");
        Validations = _meter.CreateCounter<long>(
            "kafka_validator.validations", description: "Выполненные валидации прогонов: verdict=pass|fail");

        _meter.CreateObservableGauge<int>(
            "kafka_validator.state",
            () => _stateCode,
            description: "Состояние сервиса: 0=starting 1=running 5=stopped 6=failed");
        _meter.CreateObservableGauge<int>(
            "kafka_validator.runs_seen",
            () => RunsSeenProvider?.Invoke() ?? _runsSeen,
            description: "Прогонов (runId), замеченных в topic с момента старта процесса");
    }

    public void ObserveState(int code) => _stateCode = code;
    public void ObserveRunsSeen(int count) => _runsSeen = count;

    public void AddParseError(string reason) =>
        ParseErrors.Add(1, new KeyValuePair<string, object?>("reason", reason));

    public void AddSkipped(string reason) =>
        SkippedRecords.Add(1, new KeyValuePair<string, object?>("reason", reason));

    public void AddValidation(bool pass) =>
        Validations.Add(1, new KeyValuePair<string, object?>("verdict", pass ? "pass" : "fail"));

    public void Dispose() => _meter.Dispose();
}
