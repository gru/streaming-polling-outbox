using System.Diagnostics.Metrics;
using CdcCommon;

namespace CdcPolling.Metrics;

/// <summary>
/// Метрики CDC Polling (requirements.md 13.2), System.Diagnostics.Metrics →
/// OTel → Prometheus (pull, /metrics). Кардинальность фиксированная:
/// message id не используется как label; kind — фиксированный набор типов записей.
/// LSN-значения экспортируются как double (до 2^53 точны без потерь).
/// </summary>
public sealed class CdcPollingMetrics : IDisposable
{
    public const string MeterName = "StreamingPollingOutbox.CdcPolling";

    private readonly Meter _meter = new(MeterName);

    private Lsn _lastReceived;
    private Lsn _confirmed;
    private long _walLagBytes;
    private int _stateCode = StateStarting;

    // Состояния сервиса как число для gauge (числовые коды стабильны для дашбордов)
    public const int StateStarting = 0;
    public const int StateRunning = 1;
    public const int StateWaitingDatabase = 2;
    public const int StateWaitingKafka = 3;
    public const int StateErrorSlot = 4;
    public const int StateStopped = 5;
    public const int StateFailed = 6;

    public Counter<long> MessagesRead { get; }
    public Counter<long> MessagesDelivered { get; }
    public Counter<long> RecordsDecoded { get; }
    public Counter<long> ReadErrors { get; }
    public Counter<long> KafkaErrors { get; }
    public Counter<long> Retries { get; }
    public Counter<long> Reconnects { get; }
    public Histogram<double> ReadBatchDurationMs { get; }
    public Histogram<double> ReadBatchSize { get; }
    public Histogram<double> PublishBatchDurationMs { get; }
    public Histogram<double> E2eLatencyMs { get; }
    public Histogram<double> RecoveryDurationMs { get; }

    public CdcPollingMetrics()
    {
        MessagesRead = _meter.CreateCounter<long>(
            "cdc_polling.messages_read", description: "Пользовательские сообщения, извлечённые из Insert-записей");
        MessagesDelivered = _meter.CreateCounter<long>(
            "cdc_polling.messages_delivered", description: "Сообщения с подтверждённой Kafka-доставкой (delivery report)");
        RecordsDecoded = _meter.CreateCounter<long>(
            "cdc_polling.records_decoded", description: "Протокольные записи pgoutput по типам: kind=B|R|I|U|D|C|T|M|Y|O|?");
        ReadErrors = _meter.CreateCounter<long>(
            "cdc_polling.read_errors", description: "Ошибки чтения/advance: reason=transient|fatal");
        KafkaErrors = _meter.CreateCounter<long>(
            "cdc_polling.kafka_errors", description: "Ошибки публикации в Kafka: reason=timeout|produce|other");
        Retries = _meter.CreateCounter<long>(
            "cdc_polling.retries", description: "Повторные попытки публикации батча после ошибки Kafka");
        Reconnects = _meter.CreateCounter<long>(
            "cdc_polling.reconnects", description: "Повторные peek-циклы после транзиентных сбоев БД");
        ReadBatchDurationMs = _meter.CreateHistogram<double>(
            "cdc_polling.read_batch_duration", unit: "ms", description: "Длительность одного peek-вызова");
        ReadBatchSize = _meter.CreateHistogram<double>(
            "cdc_polling.read_batch_size", description: "Размер peek-выборки (протокольных записей)");
        PublishBatchDurationMs = _meter.CreateHistogram<double>(
            "cdc_polling.publish_batch_duration", unit: "ms", description: "Длительность публикации peek-батча в Kafka (до ack всего батча)");
        E2eLatencyMs = _meter.CreateHistogram<double>(
            "cdc_polling.e2e_latency", unit: "ms", description: "End-to-end latency: created_at → Kafka delivery report");
        RecoveryDurationMs = _meter.CreateHistogram<double>(
            "cdc_polling.recovery_duration", unit: "ms", description: "Время восстановления доставки после сбоя (БД/Kafka)");

        _meter.CreateObservableGauge<double>(
            "cdc_polling.last_received_lsn",
            () => (double)_lastReceived.Value,
            description: "Максимальный LSN, полученный из peek (актуальная прочитанная позиция)");
        _meter.CreateObservableGauge<double>(
            "cdc_polling.confirmed_lsn",
            () => (double)_confirmed.Value,
            description: "Подтверждённый LSN слота (после Kafka delivery)");
        _meter.CreateObservableGauge<long>(
            "cdc_polling.wal_lag_bytes",
            () => _walLagBytes,
            unit: "By",
            description: "WAL lag: текущий WAL LSN − confirmed_flush_lsn");
        _meter.CreateObservableGauge<int>(
            "cdc_polling.state",
            () => _stateCode,
            description: "Состояние сервиса: 0=starting 1=running 2=waiting_database 3=waiting_kafka 4=error_slot 5=stopped 6=failed");
    }

    public void ObserveLastReceived(Lsn lsn) => _lastReceived = lsn;
    public void ObserveConfirmed(Lsn lsn) => _confirmed = lsn;
    public void ObserveWalLag(long? bytes) => _walLagBytes = bytes ?? 0;
    public void ObserveState(int code) => _stateCode = code;

    public void AddRecords(IReadOnlyDictionary<char, long> counts)
    {
        foreach (var (kind, count) in counts)
            RecordsDecoded.Add(count, new KeyValuePair<string, object?>("kind", kind.ToString()));
    }

    public void Dispose() => _meter.Dispose();
}
