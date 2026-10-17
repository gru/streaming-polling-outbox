using System.Diagnostics.Metrics;

namespace Writer.Metrics;

/// <summary>
/// Метрики Writer (requirements.md 13.1), System.Diagnostics.Metrics → OTel → Prometheus (pull, /metrics).
/// Кардинальность фиксированная: message id не используется как label.
/// </summary>
public sealed class WriterMetrics : IDisposable
{
    public const string MeterName = "StreamingPollingOutbox.Writer";

    private readonly Meter _meter = new(MeterName);
    private Func<int>? _activeProvider;

    public Counter<long> MessagesWritten { get; }
    public Counter<long> BytesWritten { get; }
    public Histogram<double> CommitDurationMs { get; }
    public Histogram<double> BatchDurationMs { get; }
    public Counter<long> WriteErrors { get; }
    public Counter<long> AmbiguousCommits { get; }
    public Counter<long> Reconnects { get; }

    public WriterMetrics()
    {
        MessagesWritten = _meter.CreateCounter<long>(
            "writer.messages_written", description: "Сообщения, зафиксированные commit'ом в outbox_messages");
        BytesWritten = _meter.CreateCounter<long>(
            "writer.bytes_written", unit: "By", description: "UTF-8 байты body записанных сообщений");
        CommitDurationMs = _meter.CreateHistogram<double>(
            "writer.commit_duration", unit: "ms", description: "Длительность Npgsql CommitAsync транзакционного батча");
        BatchDurationMs = _meter.CreateHistogram<double>(
            "writer.batch_duration", unit: "ms", description: "Полная длительность записи батча (open+insert+commit, включая ретраи)");
        WriteErrors = _meter.CreateCounter<long>(
            "writer.write_errors", description: "Ошибки записи: reason=fatal|ambiguous");
        AmbiguousCommits = _meter.CreateCounter<long>(
            "writer.ambiguous_commits", description: "Батчи, чей commit остался незавершённым (остановка во время retry)");
        Reconnects = _meter.CreateCounter<long>(
            "writer.reconnects", description: "Повторные попытки записи после транзиентных сбоев соединения");
        _meter.CreateObservableGauge<int>(
            "writer.generation_active",
            () => _activeProvider?.Invoke() ?? 0,
            description: "1 — генерация выполняется, 0 — idle");
    }

    /// <summary>engine подписывает сюда признак активности для gauge.</summary>
    public void SetGenerationActiveProvider(Func<int> provider) => _activeProvider = provider;

    public void Dispose() => _meter.Dispose();
}
